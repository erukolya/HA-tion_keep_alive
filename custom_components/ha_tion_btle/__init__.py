"""The Tion breezer component."""
from __future__ import annotations

import asyncio
import logging
import math
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
    """Экземпляр устройства Tion с постоянным BLE-соединением и автопереподключением."""

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry):
        self._config_entry: ConfigEntry = config_entry

        assert self.config[CONF_MAC] is not None
        # https://developers.home-assistant.io/docs/network_discovery/#fetching-the-bleak-bledevice-from-the-address
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

        # короткая задержка для повторного подключения при обрыве
        self._reconnect_delay = timedelta(seconds=10)

        # сам объект протокола
        self.__tion: Tion = self.getTion(self.model, btle_device)

        # состояние и синхронизация подключения
        self._is_connected: bool = False
        self._connect_lock = asyncio.Lock()

        self.rssi: int = 0

        # чиним unique_id при необходимости
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

    # ------------- управление соединением -------------

    async def _ensure_connected(self) -> None:
        """Гарантирует активное BLE-соединение; безопасно вызывается конкурентно."""
        async with self._connect_lock:
            if self._is_connected:
                return
            _LOGGER.debug("BLE: connecting to Tion (%s) in persistent mode…", self.unique_id)
            try:
                await self.__tion.connect()
                self._is_connected = True
                _LOGGER.info("BLE: connected to %s (persistent).", self.unique_id)
            except Exception as e:
                self._is_connected = False
                _LOGGER.error("BLE: connect failed: %s", e)
                raise

    def _mark_disconnected(self, reason: str) -> None:
        if self._is_connected:
            _LOGGER.warning("BLE: marked disconnected (%s). Will retry in %ss.", reason, int(self._reconnect_delay.total_seconds()))
        self._is_connected = False
        # ускоряем следующую попытку опроса/переподключения
        self.update_interval = self._reconnect_delay

    async def connect(self):
        """Совместимость с существующими вызовами: просто гарантируем соединение (не рвём постоянное)."""
        await self._ensure_connected()
        return True

    async def disconnect(self):
        """Ничего не делаем умышленно — соединение должно быть постоянным."""
        _LOGGER.debug("BLE: disconnect() ignored in persistent mode.")
        return True

    # ------------- основной опрос и команды -------------

    @staticmethod
    def _decode_state(state: str) -> bool:
        return state == "on"

    async def async_update_state(self):
        """Периодическое обновление состояния с автопереподключением."""
        self.logger.info("Tion instance update started")
        response: dict[str, str | bool | int] = {}

        try:
            await self._ensure_connected()
            response = await self.__tion.get()
            # раз успешно — возвращаем нормальный интервал keep_alive
            self.update_interval = self.__keep_alive

        except MaxTriesExceededError as e:
            self._mark_disconnected(f"MaxTriesExceededError: {e}")
            raise UpdateFailed("MaxTriesExceededError") from e
        except bleak.BleakError as e:
            self._mark_disconnected(f"BleakError: {e}")
            raise UpdateFailed(f"BleakError: {e}") from e
        except Exception as e:
            # Любая иная ошибка тоже переводит нас в retry-режим
            self._mark_disconnected(f"{type(e).__name__}: {e}")
            raise

        response["is_on"] = self._decode_state(response["state"])
        response["heater"] = self._decode_state(response["heater"])
        response["is_heating"] = self._decode_state(response["heating"])
        response["filter_remain"] = math.ceil(response["filter_remain"])
        response["fan_speed"] = int(response["fan_speed"])
        response["rssi"] = self.rssi

        self.logger.debug(f"Result is {response}")
        return response

    async def set(self, **kwargs):
        """Отправка команд в режиме постоянного соединения + автопереподключение."""
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
            await self.__tion.set(kwargs)
            # локально применяем изменения, чтобы UI не «плавал»
            self.data.update(original_args)
            self.async_update_listeners()
        except bleak.BleakError as e:
            self._mark_disconnected(f"BleakError on set: {e}")
            raise
        except Exception as e:
            self._mark_disconnected(f"{type(e).__name__} on set: {e}")
            raise

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
