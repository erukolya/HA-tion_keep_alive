"""The Tion breezer component."""
from __future__ import annotations

import asyncio
import logging
import math
import random
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
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from tion_btle.tion import MaxTriesExceededError, Tion

from .const import CONF_AWAY_TEMP, CONF_KEEP_ALIVE, CONF_MAC, DOMAIN, PLATFORMS, TION_SCHEMA

GLOBAL_BLE_CONNECT_SEM = asyncio.Semaphore(1)

_LOGGER = logging.getLogger(__name__)


class _BleakDirectConnectWarningFilter(logging.Filter):
    """Hide the known warning emitted by tion_btle's internal BleakClient.connect()."""

    _MESSAGE = "BleakClient.connect() called without bleak-retry-connector"

    def filter(self, record: logging.LogRecord) -> bool:
        return self._MESSAGE not in record.getMessage()


logging.getLogger("habluetooth.wrappers").addFilter(_BleakDirectConnectWarningFilter())


async def async_setup(hass, config):
    return True


async def async_setup_entry(hass, config_entry: ConfigEntry):
    _LOGGER.info("Setting up %s", config_entry.unique_id)

    hass.data.setdefault(DOMAIN, {})

    instance = TionInstance(hass, config_entry)
    hass.data[DOMAIN][config_entry.unique_id] = instance
    config_entry.async_on_unload(
        bluetooth.async_register_callback(
            hass=hass,
            callback=instance.update_btle_device,
            match_dict=BluetoothCallbackMatcher(
                address=instance.config[CONF_MAC],
                connectable=True,
            ),
            mode=bluetooth.BluetoothScanningMode.ACTIVE,
        )
    )

    await hass.config_entries.async_forward_entry_setups(config_entry, PLATFORMS)
    hass.async_create_task(instance.async_request_refresh())
    return True


async def async_unload_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Unload config entry and really close the persistent BLE connection."""
    _LOGGER.info("Unloading %s", config_entry.unique_id)

    unload_ok = await hass.config_entries.async_unload_platforms(config_entry, PLATFORMS)

    instance = hass.data.get(DOMAIN, {}).pop(config_entry.unique_id, None)
    if instance is not None:
        await instance.async_shutdown()

    if not hass.data.get(DOMAIN):
        hass.data.pop(DOMAIN, None)

    return unload_ok


class TionInstance(DataUpdateCoordinator):
    """Tion instance with persistent BLE connection and auto-reconnect."""

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry):
        self._breaker_until_ts: float = 0.0
        self._breaker_level: int = 0
        self._need_hard_reset: bool = False
        self._initial_settle_s = 2.5
        self._io_lock = asyncio.Lock()
        self._config_entry: ConfigEntry = config_entry

        assert self.config[CONF_MAC] is not None
        btle_device = bluetooth.async_ble_device_from_address(
            hass,
            self.config[CONF_MAC],
            connectable=True,
        )
        if btle_device is None:
            _LOGGER.warning(
                "BLE device %s is not in discovery cache yet. Will start with MAC and update BLEDevice later.",
                self.config[CONF_MAC],
            )
            btle_device = self.config[CONF_MAC]

        keep_alive_seconds: int = TION_SCHEMA[CONF_KEEP_ALIVE]["default"]
        try:
            keep_alive_seconds = int(self.config[CONF_KEEP_ALIVE])
        except KeyError:
            pass
        self.__keep_alive = timedelta(seconds=keep_alive_seconds)

        self._reconnect_delay = timedelta(seconds=10)
        self._prime_timeout_s = 60.0
        self._prime_sleep_s = 0.25
        self.__tion: Tion = self.getTion(self.model, btle_device)
        self._is_connected: bool = False
        self._connect_lock = asyncio.Lock()
        self.rssi: int = 0

        if self._config_entry.unique_id is None:
            _LOGGER.critical(
                "Unique id is None for %s! Will fix it by using %s",
                self._config_entry.title,
                self.unique_id,
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
        self.data = {"model": self.model, "rssi": self.rssi}

    @property
    def config(self) -> dict:
        try:
            data = dict(self._config_entry.data or {})
        except AttributeError:
            data = {}
        try:
            data.update(self._config_entry.options or {})
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
                "Model was not found in config. Please update integration settings! Config is %s",
                self.config,
            )
            _LOGGER.warning("Assume that model is S3")
            return "S3"

    @cached_property
    def supported_air_sources(self) -> list[str]:
        if self.model == "S3":
            return ["outside", "mixed", "recirculation"]
        return ["outside", "recirculation"]

    @property
    def away_temp(self) -> int:
        return self.config.get(CONF_AWAY_TEMP, TION_SCHEMA[CONF_AWAY_TEMP]["default"])

    def _mark_disconnected(self, reason: str) -> None:
        fail_count = getattr(self, "_fail_count", 0) + 1
        setattr(self, "_fail_count", fail_count)
        backoff_s = min(10 * (2 ** (fail_count - 1)), 60)

        reason_l = (reason or "").lower()
        is_handshake = (
            "handshake timeout" in reason_l
            or "services are not ready" in reason_l
            or "service discovery has not been performed" in reason_l
            or "maxtriesexceeded" in reason_l
        )
        if is_handshake:
            self._breaker_level = min(self._breaker_level + 1, 3)
            silence = [15, 45, 120, 300][self._breaker_level]
            silence = int(silence * random.uniform(0.8, 1.2))
            self._breaker_until_ts = time.monotonic() + silence
            self._need_hard_reset = True

        self._reconnect_delay = timedelta(seconds=backoff_s)
        breaker_s = max(0, int(self._breaker_until_ts - time.monotonic()))
        if self._is_connected:
            _LOGGER.warning(
                "BLE: marked disconnected (%s). Retry in %ss; breaker=%ss.",
                reason,
                backoff_s,
                breaker_s,
            )
        else:
            _LOGGER.debug(
                "BLE: still disconnected (%s). Next retry in %ss; breaker=%ss.",
                reason,
                backoff_s,
                breaker_s,
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
        _LOGGER.warning("BLE: hard reset connection (%s)", reason)
        self._is_connected = False
        try:
            await self.__tion.disconnect()
        except Exception:
            pass
        await asyncio.sleep(pause_s)
        async with GLOBAL_BLE_CONNECT_SEM:
            await self.__tion.connect()
        await asyncio.sleep(self._initial_settle_s)

    async def _hard_reset_ble(self, reason: str) -> None:
        _LOGGER.warning("BLE HARD RESET (%s): disconnecting and recreating client", reason)
        try:
            await self.__tion.disconnect()
        except Exception:
            pass
        self.__tion = self.getTion(self.model, self.unique_id)
        self._is_connected = False

    async def _reset_after_protocol_desync(self, reason: str) -> None:
        _LOGGER.warning("BLE protocol desync detected (%s): resetting client", reason)
        self._need_hard_reset = True
        self._is_connected = False
        await self._hard_reset_ble(reason)

    async def async_shutdown(self) -> None:
        """Really close persistent BLE connection during disable/unload."""
        _LOGGER.info("BLE: shutting down persistent Tion connection for %s", self.unique_id)
        self._is_connected = False
        self._need_hard_reset = False
        self._breaker_until_ts = 0.0
        self._breaker_level = 0
        try:
            await self.__tion.disconnect()
        except Exception as e:
            _LOGGER.debug("BLE: disconnect during shutdown failed: %s", e)

    async def _prime_services(self) -> None:
        started = time.monotonic()
        last_err: Exception | None = None
        await asyncio.sleep(0.15)
        sleep_s = max(0.25, getattr(self, "_prime_sleep_s", 0.25))
        not_ready_streak = 0

        while time.monotonic() - started < self._prime_timeout_s:
            try:
                async with self._io_lock:
                    await self.__tion.get()
                return
            except MaxTriesExceededError as e:
                last_err = e
                await asyncio.sleep(sleep_s)
                sleep_s = min(sleep_s * 1.5, 2.0)
                continue
            except bleak.BleakError as e:
                last_err = e
                if self._bleak_service_not_ready(e):
                    not_ready_streak += 1
                    elapsed = time.monotonic() - started
                    if not_ready_streak >= 7 and elapsed < 10.0:
                        raise UpdateFailed(
                            "Handshake timeout: BLE services are not ready (fast)"
                        ) from e
                    await asyncio.sleep(sleep_s)
                    sleep_s = min(sleep_s * 1.5, 2.0)
                    continue
                raise UpdateFailed(f"Handshake failed: {e}") from e
            except Exception as e:
                raise UpdateFailed(f"Handshake failed with unexpected error: {e}") from e

        raise UpdateFailed("Handshake timeout: BLE services are not ready") from last_err

    async def _ensure_connected(self) -> None:
        async with self._connect_lock:
            if self._is_connected:
                return

            now = time.monotonic()
            if now < self._breaker_until_ts:
                remaining = int(self._breaker_until_ts - now)
                raise UpdateFailed(f"Breaker open: waiting {remaining}s before reconnect")

            _LOGGER.debug("BLE: connecting to Tion (%s) in persistent mode…", self.unique_id)
            try:
                if self._need_hard_reset:
                    await self._hard_reset_ble("requested by breaker")
                    self._need_hard_reset = False

                async with GLOBAL_BLE_CONNECT_SEM:
                    await self.__tion.connect()

                await asyncio.sleep(self._initial_settle_s)
                await self._prime_services()

            except Exception as e:
                try:
                    await self.__tion.disconnect()
                except Exception:
                    pass

                self._is_connected = False
                if isinstance(e, UpdateFailed) and "services are not ready" in str(e).lower():
                    self._need_hard_reset = True

                self._mark_disconnected(f"connect/prime failed: {e}")
                raise
            else:
                self._is_connected = True
                setattr(self, "_fail_count", 0)
                self._breaker_until_ts = 0.0
                self._breaker_level = 0
                self.update_interval = self.__keep_alive
                _LOGGER.info("BLE: connected to %s (persistent).", self.unique_id)

    async def connect(self):
        await self._ensure_connected()
        return True

    async def disconnect(self):
        _LOGGER.debug("BLE: disconnect() ignored in persistent mode.")
        return True

    @staticmethod
    def _decode_state(state: str) -> bool:
        return state == "on"

    async def async_update_state(self):
        self.logger.info("Tion instance update started")
        response: dict[str, str | bool | int] = {}

        try:
            await self._ensure_connected()
            async with self._io_lock:
                response = await self.__tion.get()
            setattr(self, "_fail_count", 0)
            self.update_interval = self.__keep_alive

        except MaxTriesExceededError as e:
            await self._reset_after_protocol_desync(f"MaxTriesExceeded on get: {e}")
            self._mark_disconnected(f"protocol desync / MaxTriesExceeded: {e}")
            raise UpdateFailed("BLE protocol desync, client reset") from e

        except bleak.BleakError as e:
            if self._bleak_service_not_ready(e):
                self._need_hard_reset = True
            self._mark_disconnected(f"BleakError: {e}")
            raise UpdateFailed(f"BleakError: {e}") from e

        except UpdateFailed as e:
            if "services are not ready" in str(e).lower():
                self._need_hard_reset = True
            self._mark_disconnected(str(e))
            raise

        except Exception as e:
            self._mark_disconnected(f"{type(e).__name__}: {e}")
            raise

        response["is_on"] = self._decode_state(response["state"])
        response["heater"] = self._decode_state(response["heater"])
        response["is_heating"] = self._decode_state(response["heating"])
        response["filter_remain"] = math.ceil(response["filter_remain"])
        response["fan_speed"] = int(response["fan_speed"])
        response["rssi"] = self.rssi

        self.logger.debug("Result is %s", response)
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
        _LOGGER.info("Need to set: %s", args)

        try:
            await self._ensure_connected()
            try:
                async with self._io_lock:
                    await self.__tion.set(kwargs)
            except MaxTriesExceededError as e:
                await self._reset_after_protocol_desync(f"MaxTriesExceeded on set: {e}")
                self._mark_disconnected(f"protocol desync on set: {e}")
                raise

        except bleak.BleakError as e:
            if self._bleak_service_not_ready(e):
                self._need_hard_reset = True
            self._mark_disconnected(f"BleakError on set: {e}")
            raise

        except Exception as e:
            self._mark_disconnected(f"{type(e).__name__} on set: {e}")
            if "service" in str(e).lower():
                self._need_hard_reset = True
            raise

        else:
            self.data.update(original_args)
            self.async_update_listeners()
            setattr(self, "_fail_count", 0)
            self.update_interval = self.__keep_alive

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
        data = self.data or {}
        info = {
            "identifiers": {(DOMAIN, self.unique_id)},
            "name": self.name,
            "manufacturer": "Tion",
            "model": data.get("model", self.model),
        }
        if data.get("fw_version") is not None:
            info["sw_version"] = data.get("fw_version")
        return info

    @callback
    def update_btle_device(
        self,
        service_info: bluetooth.BluetoothServiceInfoBleak,
        _change: bluetooth.BluetoothChange,
    ) -> None:
        if service_info.device is not None:
            self.rssi = service_info.rssi
            self.__tion.update_btle_device(service_info.device)
