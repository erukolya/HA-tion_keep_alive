"""The Tion breezer component."""
from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import timedelta
from functools import cached_property

import bleak
import tion_btle
from bleak.backends.device import BLEDevice
from homeassistant.components import bluetooth
from homeassistant.components.bluetooth import BluetoothCallbackMatcher
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from tion_btle.tion import Tion, MaxTriesExceededError

from .const import DOMAIN, TION_SCHEMA, CONF_KEEP_ALIVE, CONF_AWAY_TEMP, CONF_MAC, PLATFORMS

import random

# Один коннект за раз на весь процесс (гасим гонку между устройствами)
GLOBAL_BLE_CONNECT_SEM = asyncio.Semaphore(1)

_LOGGER = logging.getLogger(__name__)


async def async_setup(hass, config):
    return True


async def async_setup_entry(hass, config_entry: ConfigEntry):
    _LOGGER.info("Setting up %s ", config_entry.unique_id)

    hass.data.setdefault(DOMAIN, {})

    instance = TionInstance(hass, config_entry)
    hass.data[DOMAIN][config_entry.unique_id] = instance
    config_entry.async_on_unload(
        bluetooth.async_register_callback(
            hass=hass,
            callback=instance.update_btle_device,
            match_dict=BluetoothCallbackMatcher(address=instance.config[CONF_MAC], connectable=True),
            mode=bluetooth.BluetoothScanningMode.ACTIVE,
        )
    )

    await hass.data[DOMAIN][config_entry.unique_id].async_config_entry_first_refresh()
    await hass.config_entries.async_forward_entry_setups(config_entry, PLATFORMS)
    return True


class TionInstance(DataUpdateCoordinator):
    """Экземпляр Tion c постоянным BLE-соединением, «праймингом» сервисов после коннекта и автопереподключением."""

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry):

        # Предохранитель (circuit-breaker)
        self._breaker_until_ts: float = 0.0   # мон.время до которого молчим
        self._breaker_level: int = 0          # ступень 0..3
        self._need_hard_reset: bool = False   # запросить жёсткий ресет перед следующим коннектом
        
        # Небольшая задержка после удачного connect
        self._initial_settle_s = 2.5          # было 1.5; можно 3.0 при желании
        
        # Защита от параллельных write/notify к одному устройству
        self._io_lock = asyncio.Lock()
        
        self._config_entry: ConfigEntry = config_entry

        assert self.config[CONF_MAC] is not None
        btle_device = bluetooth.async_ble_device_from_address(hass, self.config[CONF_MAC], connectable=True)
        if btle_device is None:
            raise ConfigEntryNotReady

        # keep_alive из настроек (секунды)
        keep_alive_seconds: int = TION_SCHEMA[CONF_KEEP_ALIVE]["default"]
        try:
            keep_alive_seconds = int(self.config[CONF_KEEP_ALIVE])
        except KeyError:
            pass
        self.__keep_alive = timedelta(seconds=keep_alive_seconds)

        # задержки
        self._reconnect_delay = timedelta(seconds=10)

        # Параметры хэндшейка/прайминга
        self._prime_timeout_s = 60.0      # общее окно, чтобы «раскачать» сервисы после connect()
        self._prime_sleep_s = 0.25       # пауза между попытками get() во время прайминга

        # объект протокола
        self.__tion: Tion = self.getTion(self.model, btle_device)

        # состояние соединения
        self._is_connected: bool = False
        self._connect_lock = asyncio.Lock()

        self.rssi: int = 0

        if self._config_entry.unique_id is None:
            _LOGGER.critical(
                f"Unique id is None for {self._config_entry.title}! Will fix it by using {self.unique_id}"
            )
            hass.config_entries.async_update_entry(
                entry=self._config_entry,
                unique_id=self.unique_id,
            )
            _LOGGER.critical("Done! Please restart Home Assistant.")

        super().__init__(
            name=self.config.get("name", TION_SCHEMA["name"]["default"]),
            hass=hass,
            logger=_LOGGER,
            update_interval=self.__keep_alive,
            update_method=self.async_update_state,
        )

    # ------------- конфиг и справочные свойства -------------

    @property
    def config(self) -> dict:
        try:
            data = dict(self._config_entry.data or {})
        except AttributeError:
            data = {}
        try:
            options = self._config_entry.options or {}
            data.update(options)
        except AttributeError:
            pass
        return data

    @cached_property
    def unique_id(self) -> str:
        return self.config[CONF_MAC]

    @cached_property
    def model(self) -> str:
        try:
            return self.config["model"]
        except KeyError:
            _LOGGER.warning(
                f"Model was not found in config. Please update integration settings! Config is {self.config}"
            )
            _LOGGER.warning("Assume that model is S3")
            return "S3"

    @cached_property
    def supported_air_sources(self) -> list[str]:
        if self.model == "S3":
            return ["outside", "mixed", "recirculation"]
        else:
            return ["outside", "recirculation"]

    @property
    def away_temp(self) -> int:
        """Temperature for away mode"""
        return self.config.get(CONF_AWAY_TEMP, TION_SCHEMA[CONF_AWAY_TEMP]["default"])

    # ------------- утилиты -------------
    
    
    def _mark_disconnected(self, reason: str) -> None:
        # экспоненциальный backoff 10→20→40→60 c потолком 60с — для "обычных" отвалов
        fail_count = getattr(self, "_fail_count", 0) + 1
        setattr(self, "_fail_count", fail_count)
        backoff_s = min(10 * (2 ** (fail_count - 1)), 60)
    
        # если это таймаут хендшейка/сервисы не поднялись — включаем предохранитель жестче
        reason_l = (reason or "").lower()
        is_handshake = (
            "handshake timeout" in reason_l
            or "services are not ready" in reason_l
            or "service discovery has not been performed" in reason_l
            or "maxtriesexceeded" in reason_l
        )
        if is_handshake:
            self._breaker_level = min(self._breaker_level + 1, 3)
            # ступени молчания: 15, 45, 120, 300
            silence = [15, 45, 120, 300][self._breaker_level]
            # джиттер ±20%, чтобы не столкнуться со вторым устройством
            silence = int(silence * random.uniform(0.8, 1.2))
            self._breaker_until_ts = time.monotonic() + silence
            # просим при следующем подключении сделать жёсткий ресет стека
            self._need_hard_reset = True
    
        # обычный backoff для планировщика координатора
        self._reconnect_delay = timedelta(seconds=backoff_s)
    
        if self._is_connected:
            _LOGGER.warning(
                "BLE: marked disconnected (%s). Retry in %ss; breaker=%ss.",
                reason,
                backoff_s,
                max(0, int(self._breaker_until_ts - time.monotonic())),
            )
        else:
            _LOGGER.debug(
                "BLE: still disconnected (%s). Next retry in %ss; breaker=%ss.",
                reason,
                backoff_s,
                max(0, int(self._breaker_until_ts - time.monotonic())),
            )
    
        self._is_connected = False
        self.update_interval = self._reconnect_delay

    def _bleak_service_not_ready(self, err: Exception) -> bool:
        msg = (str(err) or "").lower()
        return (
            "service discovery has not been performed yet" in msg
            or "services are not ready" in msg
        )
        
    async def _hard_reset_connection(self, reason: str, pause_s: float = 0.3) -> None:
        """Жёстко сбрасывает BLE-сессию и пересоздаёт подключение (без прайминга)."""
        _LOGGER.warning("BLE: hard reset connection (%s)", reason)
        self._is_connected = False
        try:
            await self.__tion.disconnect()
        except Exception:
            pass
        await asyncio.sleep(pause_s)
        async with GLOBAL_BLE_CONNECT_SEM:
            await self.__tion.connect()
        await asyncio.sleep(self._initial_settle_s)  # даём BlueZ «встать на ноги»

    async def _hard_reset_ble(self, reason: str) -> None:
        """Полный ресет: закрыть коннект, выкинуть клиент, пересоздать протокол (без перезапуска HA)."""
        _LOGGER.warning("BLE HARD RESET (%s): disconnecting and recreating client", reason)
        try:
            await self.__tion.disconnect()
        except Exception:
            pass
        # Пересоздаём объект протокола с MAC-адресом (BLEDevice прилетит позже в callback)
        self.__tion = self.getTion(self.model, self.unique_id)
        self._is_connected = False

    async def _prime_services(self) -> None:
        """Мягко «раскачиваем» GATT: пробуем get() с растущей паузой; при длинной серии NotReady сдаёмся."""
        started = time.monotonic()
        last_err: Exception | None = None
    
        # короткая стартовая пауза помогает BlueZ
        await asyncio.sleep(0.15)
    
        sleep_s = max(0.25, getattr(self, "_prime_sleep_s", 0.25))
        not_ready_streak = 0
    
        while time.monotonic() - started < self._prime_timeout_s:
            try:
                async with self._io_lock:
                    await self.__tion.get()
                return  # успех
            except MaxTriesExceededError as e:
                last_err = e
                await asyncio.sleep(sleep_s)
                sleep_s = min(sleep_s * 1.5, 2.0)
                continue
            except bleak.BleakError as e:
                last_err = e
                if self._bleak_service_not_ready(e):
                    not_ready_streak += 1
                    # если прям лавина NotReady в самом начале — уход в быстрый backoff, не долбим минуту
                    elapsed = time.monotonic() - started
                    if not_ready_streak >= 7 and elapsed < 10.0:
                        raise UpdateFailed("Handshake timeout: BLE services are not ready (fast)") from e
                    await asyncio.sleep(sleep_s)
                    sleep_s = min(sleep_s * 1.5, 2.0)
                    continue
                # другие BLE-ошибки — фатал для хэндшейка
                raise UpdateFailed(f"Handshake failed: {e}") from e
            except Exception as e:
                raise UpdateFailed(f"Handshake failed with unexpected error: {e}") from e
    
        raise UpdateFailed("Handshake timeout: BLE services are not ready") from last_err


    # ------------- управление соединением -------------

    async def _ensure_connected(self) -> None:
        """Активное постоянное соединение + готовность сервисов; защищено от гонок/коллизий."""
        async with self._connect_lock:
            if self._is_connected:
                return
    
            # предохранитель: если «молчим», даже не начинаем коннект
            now = time.monotonic()
            if now < self._breaker_until_ts:
                remaining = int(self._breaker_until_ts - now)
                raise UpdateFailed(f"Breaker open: waiting {remaining}s before reconnect")
    
            _LOGGER.debug("BLE: connecting to Tion (%s) in persistent mode…", self.unique_id)
            try:
                # по запросу — полный ресет клиента перед коннектом
                if self._need_hard_reset:
                    await self._hard_reset_ble("requested by breaker")
                    self._need_hard_reset = False
    
                # один коннект на весь процесс
                async with GLOBAL_BLE_CONNECT_SEM:
                    await self.__tion.connect()
    
                await asyncio.sleep(self._initial_settle_s)
                await self._prime_services()
    
            except Exception as e:
                # оборвать возможное полуподключение
                try:
                    await self.__tion.disconnect()
                except Exception:
                    pass
    
                self._is_connected = False
    
                # при таймауте сервисов — просим жёсткий ресет на следующий круг
                if isinstance(e, UpdateFailed) and "services are not ready" in (str(e).lower()):
                    self._need_hard_reset = True
    
                self._mark_disconnected(f"connect/prime failed: {e}")
                raise
            else:
                self._is_connected = True
                setattr(self, "_fail_count", 0)
                # закрываем предохранитель
                self._breaker_until_ts = 0.0
                self._breaker_level = 0
                self.update_interval = self.__keep_alive
                _LOGGER.info("BLE: connected to %s (persistent).", self.unique_id)

    async def connect(self):
        """Совместимость: просто гарантируем соединение (персистентность не нарушаем)."""
        await self._ensure_connected()
        return True

    async def disconnect(self):
        """Ничего не делаем — соединение должно быть постоянным."""
        _LOGGER.debug("BLE: disconnect() ignored in persistent mode.")
        return True

    # ------------- основной опрос и команды -------------

    @staticmethod
    def _decode_state(state: str) -> bool:
        return state == "on"

    async def async_update_state(self):
        """Периодический опрос с автопереподключением; при сбое — hard reset линка и единичный повтор."""
        self.logger.info("Tion instance update started")
        response: dict[str, str | bool | int] = {}
    
        try:
            await self._ensure_connected()
            async with self._io_lock:
                response = await self.__tion.get()
            setattr(self, "_fail_count", 0)
            self.update_interval = self.__keep_alive
    
        except MaxTriesExceededError as e:
            try:
                await self._prime_services()
                async with self._io_lock:
                    response = await self.__tion.get()
                setattr(self, "_fail_count", 0)
                self.update_interval = self.__keep_alive
            except Exception as inner:
                # просим жёсткий ресет на следующий круг
                self._need_hard_reset = True
                self._mark_disconnected(f"MaxTriesExceeded after connect: {inner}")
                raise UpdateFailed("MaxTriesExceeded after connect") from inner
        
        except bleak.BleakError as e:
            if self._bleak_service_not_ready(e):
                self._need_hard_reset = True
            self._mark_disconnected(f"BleakError: {e}")
            raise UpdateFailed(f"BleakError: {e}") from e
        
        except UpdateFailed as e:
            if "services are not ready" in (str(e).lower()):
                self._need_hard_reset = True
            self._mark_disconnected(str(e))
            raise
    
        except Exception as e:
            self._mark_disconnected(f"{type(e).__name__}: {e}")
            raise
    
        # нормализация полей
        response["is_on"] = self._decode_state(response["state"])
        response["heater"] = self._decode_state(response["heater"])
        response["is_heating"] = self._decode_state(response["heating"])
        response["filter_remain"] = math.ceil(response["filter_remain"])
        response["fan_speed"] = int(response["fan_speed"])
        response["rssi"] = self.rssi
    
        self.logger.debug(f"Result is {response}")
        return response


    async def set(self, **kwargs):
        if "fan_speed" in kwargs:
            kwargs["fan_speed"] = int(kwargs["fan_speed"])
    
        original_args = kwargs.copy()
        if "is_on" in kwargs:
            kwargs["state"] = "on" if kwargs["is_on"] else "off"
            del kwargs["is_on"]
        if "heater" in kwargs:
            kwargs["heater"] = "on" if kwargs["heater"] else "off"
    
        args = ", ".join("%s=%r" % x for x in kwargs.items())
        _LOGGER.info("Need to set: " + args)
    
        try:
            await self._ensure_connected()
    
            # Внутренний try только под MaxTriesExceededError — делаем единичный re-prime и повтор
            try:
                async with self._io_lock:
                    await self.__tion.set(kwargs)
            except MaxTriesExceededError:
                await self._prime_services()
                async with self._io_lock:
                    await self.__tion.set(kwargs)
    
        except bleak.BleakError as e:
            if self._bleak_service_not_ready(e):
                self._need_hard_reset = True
            self._mark_disconnected(f"BleakError on set: {e}")
            raise
    
        except Exception as e:
            self._mark_disconnected(f"{type(e).__name__} on set: {e}")
            # если это похоже на проблему GATT/сервисов — запросим жёсткий ресет
            if "service" in (str(e).lower()):
                self._need_hard_reset = True
            raise
    
        else:
            # успех — обновляем локальное состояние, чтобы UI не «плавал»
            self.data.update(original_args)
            self.async_update_listeners()
            setattr(self, "_fail_count", 0)
            self.update_interval = self.__keep_alive

    # ------------- фабрика устройств и BTLE событийка -------------

    @staticmethod
    def getTion(model: str, mac: str | BLEDevice) -> tion_btle.TionS3 | tion_btle.TionLite | tion_btle.TionS4:
        if model == "S3":
            from tion_btle.s3 import TionS3 as Breezer
        elif model == "S4":
            from tion_btle.s4 import TionS4 as Breezer
        elif model == "Lite":
            from tion_btle.lite import TionLite as Breezer
        else:
            raise NotImplementedError("Model '%s' is not supported!" % model)
        return Breezer(mac)

    @property
    def device_info(self):
        info = {
            "identifiers": {(DOMAIN, self.unique_id)},
            "name": self.name,
            "manufacturer": "Tion",
            "model": self.data.get("model"),
        }
        if self.data.get("fw_version") is not None:
            info["sw_version"] = self.data.get("fw_version")
        return info

    @callback
    def update_btle_device(
        self,
        service_info: bluetooth.BluetoothServiceInfoBleak,
        _change: bluetooth.BluetoothChange,
    ) -> None:
        """Подхватываем новый BLEDevice и сохраняем RSSI; библиотека сама переиспользует его внутри."""
        if service_info.device is not None:
            self.rssi = service_info.rssi
            self.__tion.update_btle_device(service_info.device)
