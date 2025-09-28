"""
Tion breezer – Bluetooth (BTLE) integration for Home Assistant.

Фокус правок для устойчивого соединения:
- общий asyncio.Lock для сериализации BLE-операций (опрос и команды);
- ожидание фактического connection_status == 'connected' после connect();
- «мягкий» прайминг сервисов (ждём завершение discovery) перед любыми write/read;
- увеличенные таймауты для Tion 4S;
- корректная очистка флагов при дисконнекте.

Публичные API и остальная логика не менялись.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

# Библиотека протокола Tion (как в исходном проекте)
from tion_btle.s4 import TionS4  # при необходимости адаптируй импорты под свой layout
from tion_btle.tion import Tion
from bleak import BleakError

DOMAIN = "ha_tion_btle"
_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Tion breezer from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    cfg = entry.data or {}
    options = entry.options or {}

    mac = cfg.get("mac") or options.get("mac")
    model = (cfg.get("model") or options.get("model") or "S4").upper()
    name = cfg.get("name") or options.get("name") or f"Tion Breezer {mac}"

    keep_alive = int(options.get("keep_alive") or cfg.get("keep_alive") or 60)
    away_temp = options.get("away_temp") or cfg.get("away_temp") or 15

    if not mac:
        raise HomeAssistantError("Tion: MAC address is required in config entry")

    tion: Tion = TionS4(mac) if model == "S4" else TionS4(mac)

    instance = TionInstance(
        hass=hass,
        entry=entry,
        tion=tion,
        name=name,
        keep_alive=keep_alive,
        away_temp=away_temp,
    )

    coordinator = TionCoordinator(hass, instance, name, keep_alive)
    hass.data[DOMAIN][entry.entry_id] = {"instance": instance, "coordinator": coordinator}

    await coordinator.async_config_entry_first_refresh()
    await hass.config_entries.async_forward_entry_setups(entry, ["climate", "fan", "sensor", "select"])
    _LOGGER.info("Setting up %s", mac)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    stored = hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    if stored and "instance" in stored:
        await stored["instance"].async_shutdown()
    return await hass.config_entries.async_unload_platforms(entry, ["climate", "fan", "sensor", "select"])


class TionCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Координатор опроса устройства."""

    def __init__(self, hass: HomeAssistant, instance: "TionInstance", name: str, keep_alive: int) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=f"{name} coordinator",
            update_interval=timedelta(seconds=max(keep_alive, 30)),
        )
        self._instance = instance

    async def _async_update_data(self) -> dict[str, Any]:
        return await self._instance.async_update_state()


class TionInstance:
    """Обёртка над tion_btle с устойчивым соединением и сериализацией операций."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        tion: Tion,
        name: str,
        keep_alive: int = 60,
        away_temp: int | float = 15,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.__tion: Tion = tion
        self.name = name
        self.__keep_alive = int(keep_alive)
        self.away_temp = away_temp

        self.logger = _LOGGER
        self._loop = asyncio.get_event_loop()

        # ------- устойчивость -------
        self._io_lock = asyncio.Lock()          # сериализация BLE
        self._services_ready = asyncio.Event()  # флаг готовности сервисов
        self._services_ready.clear()

        # Подними это значение до 60.0, как ты и хотел
        self._prime_timeout_s: float = 60.0     # ожидание discovery/notifications
        self._prime_sleep_s: float = 0.25
        # ----------------------------

        self._last_result: dict[str, Any] = {}
        self._connected_logged_once = False

    # ----------------- внутренние утилиты -----------------

    async def _wait_lib_connected(self, max_wait: float = 5.0) -> None:
        """Ждём, пока библиотека выставит connection_status == 'connected'."""
        start = self._loop.time()
        while getattr(self.__tion, "connection_status", "disc") != "connected":
            if self._loop.time() - start > max_wait:
                break
            await asyncio.sleep(0.1)

    async def _prime_services(self) -> None:
        """Прайминг сервисов: не выполняем write/read, пока discovery не готов."""
        if self._services_ready.is_set():
            return

        deadline = self._loop.time() + self._prime_timeout_s
        attempt = 0
        while self._loop.time() < deadline:
            attempt += 1
            try:
                # Лёгкий get() часто триггерит/проверяет, что discovery завершён
                _ = await self.__tion.get()
                self._services_ready.set()
                self.logger.debug("BLE: services are ready after %d attempt(s).", attempt)
                return
            except BleakError as e:
                msg = str(e)
                if (
                    "Service Discovery has not been performed yet" in msg
                    or "Not connected" in msg
                    or "Disconnected" in msg
                    or "Failed to write" in msg
                ):
                    await asyncio.sleep(self._prime_sleep_s)
                    continue
                raise
            except Exception:
                await asyncio.sleep(self._prime_sleep_s)

        raise HomeAssistantError("Handshake timeout: BLE services are not ready")

    def _mark_disconnected(self, reason: str = "") -> None:
        if reason:
            self.logger.debug("BLE: marked disconnected (%s).", reason)
        else:
            self.logger.debug("BLE: marked disconnected.")
        self._services_ready.clear()
        self._connected_logged_once = False

    def _mark_connected(self) -> None:
        if not self._connected_logged_once:
            self.logger.info("BLE: connected to %s.", getattr(self.__tion, "mac", "device"))
            self._connected_logged_once = True

    async def _ensure_connected(self) -> None:
        """Единая точка входа: подключиться и дождаться готовности сервисов."""
        if getattr(self.__tion, "connection_status", "disc") == "connected" and self._services_ready.is_set():
            return

        # 1) подключение
        try:
            self.logger.debug("BLE: connecting to Tion %s …", getattr(self.__tion, "mac", "device"))
            # ВАЖНО: без аргумента persistent
            await self.__tion.connect()
            await self._wait_lib_connected(max_wait=5.0)
            self._mark_connected()
        except Exception as e:
            self._mark_disconnected(reason=f"connect error: {e}")
            raise

        # 2) прайминг сервисов (ожидаем discovery)
        try:
            await self._prime_services()
        except Exception as e:
            # если не вышло — аккуратно разорвём, чтобы не залипнуть в BlueZ
            try:
                await self.__tion.disconnect()
            except Exception:
                pass
            self._mark_disconnected(reason=str(e))
            raise

    # ----------------- публичные методы -----------------

    async def async_update_state(self) -> dict[str, Any]:
        """Опрос состояния. Сериализован локсом с командами."""
        self.logger.info("Tion instance update started")
        async with self._io_lock:
            try:
                await self._ensure_connected()
                result = await self.__tion.get()
                self._last_result = self._normalize_result(result)
                return self._last_result
            except Exception as e:
                self._mark_disconnected(reason=str(e))
                # короткий, как и раньше
                self.logger.error("BLE: connect failed: %s", "" if isinstance(e, HomeAssistantError) else str(e))
                raise

    async def set(self, **kwargs: Any) -> dict[str, Any]:
        """Отправка команды устройству. Сериализовано с опросом."""
        async with self._io_lock:
            try:
                await self._ensure_connected()
                result = await self.__tion.set(**kwargs)  # type: ignore[arg-type]
                if result:
                    self._last_result = self._normalize_result(result)
                return self._last_result or {}
            except Exception as e:
                self._mark_disconnected(reason=str(e))
                msg = str(e)
                if "Service Discovery has not been performed yet" in msg or "services are not ready" in msg:
                    raise HomeAssistantError("Handshake timeout: BLE services are not ready") from e
                raise

    async def async_shutdown(self) -> None:
        """Акуратное завершение работы интеграции."""
        try:
            async with self._io_lock:
                try:
                    await self.__tion.disconnect()
                except Exception:
                    pass
                self._mark_disconnected(reason="shutdown")
        except Exception:
            pass

    # ----------------- утилиты нормализации -----------------

    def _normalize_result(self, raw: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(raw, dict):
            return {}
        out = dict(raw)
        out["is_on"] = bool(out.get("state") == "on")
        out["is_heating"] = bool(out.get("heating") == "on")
        return out
