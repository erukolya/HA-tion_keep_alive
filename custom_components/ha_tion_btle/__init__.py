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
from homeassistant.exceptions import HomeAssistantError
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


def _describe_btle_device(device: str | BLEDevice | None) -> str:
    if device is None:
        return "None"
    if isinstance(device, str):
        return f"MAC({device})"
    return (
        f"BLEDevice(address={device.address}, name={device.name}, "
        f"details={getattr(device, 'details', None)!r})"
    )


async def async_setup(hass, config):
    _LOGGER.info("TION_DIAG async_setup called")
    return True


async def async_setup_entry(hass, config_entry: ConfigEntry):
    _LOGGER.warning(
        "TION_DIAG setup_entry start: entry_id=%s unique_id=%s title=%s data=%s options=%s",
        config_entry.entry_id,
        config_entry.unique_id,
        config_entry.title,
        dict(config_entry.data or {}),
        dict(config_entry.options or {}),
    )

    hass.data.setdefault(DOMAIN, {})

    instance = TionInstance(hass, config_entry)
    hass.data[DOMAIN][config_entry.unique_id] = instance
    _LOGGER.warning(
        "TION_DIAG setup_entry instance created: unique_id=%s model=%s keep_alive=%s initial_data=%s",
        instance.unique_id,
        instance.model,
        instance.keep_alive_seconds,
        instance.data,
    )

    unregister_callback = bluetooth.async_register_callback(
        hass=hass,
        callback=instance.update_btle_device,
        match_dict=BluetoothCallbackMatcher(
            address=instance.config[CONF_MAC],
            connectable=True,
        ),
        mode=bluetooth.BluetoothScanningMode.ACTIVE,
    )
    config_entry.async_on_unload(unregister_callback)
    _LOGGER.warning("TION_DIAG bluetooth callback registered: mac=%s", instance.config[CONF_MAC])

    _LOGGER.warning("TION_DIAG forwarding platforms start: platforms=%s", PLATFORMS)
    await hass.config_entries.async_forward_entry_setups(config_entry, PLATFORMS)
    _LOGGER.warning("TION_DIAG forwarding platforms done")

    task = hass.async_create_task(instance.async_request_refresh())

    def _log_first_refresh_done(done_task: asyncio.Task) -> None:
        try:
            done_task.result()
        except Exception as e:  # pylint: disable=broad-except
            _LOGGER.exception("TION_DIAG background first refresh failed: %s", e)
        else:
            _LOGGER.warning("TION_DIAG background first refresh finished successfully")

    task.add_done_callback(_log_first_refresh_done)
    _LOGGER.warning("TION_DIAG background first refresh scheduled")
    return True


async def async_unload_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Unload config entry and really close the persistent BLE connection."""
    _LOGGER.warning(
        "TION_DIAG unload_entry start: entry_id=%s unique_id=%s",
        config_entry.entry_id,
        config_entry.unique_id,
    )

    unload_ok = await hass.config_entries.async_unload_platforms(config_entry, PLATFORMS)
    _LOGGER.warning("TION_DIAG unload platforms result: %s", unload_ok)

    instance = hass.data.get(DOMAIN, {}).pop(config_entry.unique_id, None)
    if instance is not None:
        await instance.async_shutdown()
    else:
        _LOGGER.warning("TION_DIAG unload_entry: no instance found in hass.data")

    if not hass.data.get(DOMAIN):
        hass.data.pop(DOMAIN, None)

    _LOGGER.warning("TION_DIAG unload_entry done: unload_ok=%s", unload_ok)
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
        _LOGGER.warning(
            "TION_DIAG init BLE lookup: mac=%s found=%s device=%s",
            self.config[CONF_MAC],
            btle_device is not None,
            _describe_btle_device(btle_device),
        )
        if btle_device is None:
            _LOGGER.warning(
                "TION_DIAG init fallback to MAC: device %s is not in discovery cache yet",
                self.config[CONF_MAC],
            )
            btle_device = self.config[CONF_MAC]

        keep_alive_seconds: int = TION_SCHEMA[CONF_KEEP_ALIVE]["default"]
        try:
            keep_alive_seconds = int(self.config[CONF_KEEP_ALIVE])
        except KeyError:
            pass
        self._keep_alive_seconds = keep_alive_seconds
        self.__keep_alive = timedelta(seconds=keep_alive_seconds)

        self._reconnect_delay = timedelta(seconds=10)
        self._prime_timeout_s = 60.0
        self._prime_sleep_s = 0.25
        self.__tion: Tion = self.getTion(self.model, btle_device)
        _LOGGER.warning(
            "TION_DIAG init Tion object created: model=%s tion_class=%s source=%s",
            self.model,
            type(self.__tion).__name__,
            _describe_btle_device(btle_device),
        )
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
        _LOGGER.warning(
            "TION_DIAG coordinator initialized: name=%s update_interval=%s data=%s",
            self.name,
            self.update_interval,
            self.data,
        )

    @property
    def keep_alive_seconds(self) -> int:
        return self._keep_alive_seconds

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
            or "timeout" in reason_l
        )
        if is_handshake:
            self._breaker_level = min(self._breaker_level + 1, 3)
            silence = [15, 45, 120, 300][self._breaker_level]
            silence = int(silence * random.uniform(0.8, 1.2))
            self._breaker_until_ts = time.monotonic() + silence
            self._need_hard_reset = True

        self._reconnect_delay = timedelta(seconds=backoff_s)
        breaker_s = max(0, int(self._breaker_until_ts - time.monotonic()))
        _LOGGER.warning(
            "TION_DIAG mark_disconnected: reason=%s fail_count=%s backoff=%ss breaker=%ss need_hard_reset=%s was_connected=%s",
            reason,
            fail_count,
            backoff_s,
            breaker_s,
            self._need_hard_reset,
            self._is_connected,
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
        _LOGGER.warning("TION_DIAG hard_reset_connection start: reason=%s", reason)
        self._is_connected = False
        try:
            await self.__tion.disconnect()
            _LOGGER.warning("TION_DIAG hard_reset_connection disconnect done")
        except Exception as e:
            _LOGGER.warning("TION_DIAG hard_reset_connection disconnect failed: %s", e)
        await asyncio.sleep(pause_s)
        async with GLOBAL_BLE_CONNECT_SEM:
            _LOGGER.warning("TION_DIAG hard_reset_connection connect start")
            await self.__tion.connect()
        await asyncio.sleep(self._initial_settle_s)
        _LOGGER.warning("TION_DIAG hard_reset_connection done")

    async def _hard_reset_ble(self, reason: str) -> None:
        _LOGGER.warning("TION_DIAG hard_reset_ble start: reason=%s", reason)
        try:
            await self.__tion.disconnect()
            _LOGGER.warning("TION_DIAG hard_reset_ble disconnect done")
        except Exception as e:
            _LOGGER.warning("TION_DIAG hard_reset_ble disconnect failed: %s", e)
        self.__tion = self.getTion(self.model, self.unique_id)
        self._is_connected = False
        _LOGGER.warning(
            "TION_DIAG hard_reset_ble recreated Tion object: model=%s tion_class=%s source=MAC(%s)",
            self.model,
            type(self.__tion).__name__,
            self.unique_id,
        )

    async def _reset_after_protocol_desync(self, reason: str) -> None:
        _LOGGER.warning("TION_DIAG protocol_desync: reason=%s", reason)
        self._need_hard_reset = True
        self._is_connected = False
        await self._hard_reset_ble(reason)

    async def async_shutdown(self) -> None:
        """Really close persistent BLE connection during disable/unload."""
        _LOGGER.warning("TION_DIAG shutdown start: unique_id=%s", self.unique_id)
        self._is_connected = False
        self._need_hard_reset = False
        self._breaker_until_ts = 0.0
        self._breaker_level = 0
        try:
            await self.__tion.disconnect()
            _LOGGER.warning("TION_DIAG shutdown disconnect done")
        except Exception as e:
            _LOGGER.warning("TION_DIAG shutdown disconnect failed: %s", e)

    async def _prime_services(self) -> None:
        _LOGGER.warning(
            "TION_DIAG prime start: timeout=%ss sleep=%ss",
            self._prime_timeout_s,
            self._prime_sleep_s,
        )
        started = time.monotonic()
        last_err: Exception | None = None
        await asyncio.sleep(0.15)
        sleep_s = max(0.25, getattr(self, "_prime_sleep_s", 0.25))
        not_ready_streak = 0
        attempt = 0

        while time.monotonic() - started < self._prime_timeout_s:
            attempt += 1
            try:
                _LOGGER.warning("TION_DIAG prime attempt %s get start", attempt)
                async with self._io_lock:
                    await self.__tion.get()
                _LOGGER.warning("TION_DIAG prime success: attempt=%s elapsed=%.2fs", attempt, time.monotonic() - started)
                return
            except MaxTriesExceededError as e:
                last_err = e
                _LOGGER.warning("TION_DIAG prime MaxTriesExceeded: attempt=%s err=%s", attempt, e)
                await asyncio.sleep(sleep_s)
                sleep_s = min(sleep_s * 1.5, 2.0)
                continue
            except TimeoutError as e:
                _LOGGER.warning("TION_DIAG prime TimeoutError: attempt=%s err=%s", attempt, e)
                raise UpdateFailed("Handshake failed: BLE operation timed out") from e
            except bleak.BleakError as e:
                last_err = e
                _LOGGER.warning("TION_DIAG prime BleakError: attempt=%s err=%s", attempt, e)
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
                _LOGGER.exception("TION_DIAG prime unexpected error: attempt=%s", attempt)
                raise UpdateFailed(f"Handshake failed with unexpected error: {e}") from e

        _LOGGER.warning(
            "TION_DIAG prime timeout: attempts=%s elapsed=%.2fs last_err=%s",
            attempt,
            time.monotonic() - started,
            last_err,
        )
        raise UpdateFailed("Handshake timeout: BLE services are not ready") from last_err

    async def _ensure_connected(self) -> None:
        async with self._connect_lock:
            _LOGGER.warning(
                "TION_DIAG ensure_connected enter: is_connected=%s breaker_left=%ss need_hard_reset=%s fail_count=%s",
                self._is_connected,
                max(0, int(self._breaker_until_ts - time.monotonic())),
                self._need_hard_reset,
                getattr(self, "_fail_count", 0),
            )
            if self._is_connected:
                _LOGGER.warning("TION_DIAG ensure_connected already connected")
                return

            now = time.monotonic()
            if now < self._breaker_until_ts:
                remaining = int(self._breaker_until_ts - now)
                _LOGGER.warning("TION_DIAG ensure_connected breaker open: remaining=%ss", remaining)
                raise UpdateFailed(f"Breaker open: waiting {remaining}s before reconnect")

            _LOGGER.warning("TION_DIAG connect start: unique_id=%s", self.unique_id)
            started = time.monotonic()
            try:
                if self._need_hard_reset:
                    _LOGGER.warning("TION_DIAG connect requires hard reset before connect")
                    await self._hard_reset_ble("requested by breaker")
                    self._need_hard_reset = False

                async with GLOBAL_BLE_CONNECT_SEM:
                    _LOGGER.warning("TION_DIAG connect calling tion.connect")
                    await self.__tion.connect()
                    _LOGGER.warning("TION_DIAG connect tion.connect returned: elapsed=%.2fs", time.monotonic() - started)

                _LOGGER.warning("TION_DIAG connect settle sleep: %ss", self._initial_settle_s)
                await asyncio.sleep(self._initial_settle_s)
                await self._prime_services()

            except TimeoutError as e:
                _LOGGER.warning("TION_DIAG connect TimeoutError after %.2fs: %s", time.monotonic() - started, e)
                try:
                    await self.__tion.disconnect()
                    _LOGGER.warning("TION_DIAG connect timeout cleanup disconnect done")
                except Exception as disconnect_err:
                    _LOGGER.warning("TION_DIAG connect timeout cleanup disconnect failed: %s", disconnect_err)
                self._is_connected = False
                self._mark_disconnected("connect timeout")
                raise UpdateFailed("BLE connect timed out") from e

            except Exception as e:
                _LOGGER.exception("TION_DIAG connect/prime failed after %.2fs", time.monotonic() - started)
                try:
                    await self.__tion.disconnect()
                    _LOGGER.warning("TION_DIAG connect failure cleanup disconnect done")
                except Exception as disconnect_err:
                    _LOGGER.warning("TION_DIAG connect failure cleanup disconnect failed: %s", disconnect_err)

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
                _LOGGER.warning("TION_DIAG connected successfully: elapsed=%.2fs", time.monotonic() - started)

    async def connect(self):
        await self._ensure_connected()
        return True

    async def disconnect(self):
        _LOGGER.warning("TION_DIAG disconnect ignored in persistent mode")
        return True

    @staticmethod
    def _decode_state(state: str) -> bool:
        return state == "on"

    async def async_update_state(self):
        _LOGGER.warning(
            "TION_DIAG update_state start: is_connected=%s data_keys=%s",
            self._is_connected,
            sorted((self.data or {}).keys()),
        )
        response: dict[str, str | bool | int] = {}

        try:
            await self._ensure_connected()
            _LOGGER.warning("TION_DIAG update_state get start")
            async with self._io_lock:
                response = await self.__tion.get()
            _LOGGER.warning("TION_DIAG update_state get success: keys=%s raw=%s", sorted(response.keys()), response)
            setattr(self, "_fail_count", 0)
            self.update_interval = self.__keep_alive

        except MaxTriesExceededError as e:
            _LOGGER.warning("TION_DIAG update_state MaxTriesExceeded: %s", e)
            await self._reset_after_protocol_desync(f"MaxTriesExceeded on get: {e}")
            self._mark_disconnected(f"protocol desync / MaxTriesExceeded: {e}")
            raise UpdateFailed("BLE protocol desync, client reset") from e

        except TimeoutError as e:
            _LOGGER.warning("TION_DIAG update_state TimeoutError: %s", e)
            self._mark_disconnected(f"TimeoutError: {e}")
            raise UpdateFailed("BLE operation timed out") from e

        except bleak.BleakError as e:
            _LOGGER.warning("TION_DIAG update_state BleakError: %s", e)
            if self._bleak_service_not_ready(e):
                self._need_hard_reset = True
            self._mark_disconnected(f"BleakError: {e}")
            raise UpdateFailed(f"BleakError: {e}") from e

        except UpdateFailed as e:
            _LOGGER.warning("TION_DIAG update_state UpdateFailed: %s", e)
            if "services are not ready" in str(e).lower() or "timeout" in str(e).lower():
                self._need_hard_reset = True
            self._mark_disconnected(str(e))
            raise

        except Exception as e:
            _LOGGER.exception("TION_DIAG update_state unexpected error")
            self._mark_disconnected(f"{type(e).__name__}: {e}")
            raise

        response["is_on"] = self._decode_state(response["state"])
        response["heater"] = self._decode_state(response["heater"])
        response["is_heating"] = self._decode_state(response["heating"])
        response["filter_remain"] = math.ceil(response["filter_remain"])
        response["fan_speed"] = int(response["fan_speed"])
        response["rssi"] = self.rssi

        _LOGGER.warning("TION_DIAG update_state normalized success: %s", response)
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
        _LOGGER.warning("TION_DIAG set start: original=%s translated=%s is_connected=%s", original_args, kwargs, self._is_connected)

        try:
            await self._ensure_connected()
            try:
                _LOGGER.warning("TION_DIAG set calling tion.set: %s", kwargs)
                async with self._io_lock:
                    await self.__tion.set(kwargs)
                _LOGGER.warning("TION_DIAG set tion.set success")
            except MaxTriesExceededError as e:
                _LOGGER.warning("TION_DIAG set MaxTriesExceeded: %s", e)
                await self._reset_after_protocol_desync(f"MaxTriesExceeded on set: {e}")
                self._mark_disconnected(f"protocol desync on set: {e}")
                raise HomeAssistantError("Tion BLE protocol desync, client reset") from e

        except UpdateFailed as e:
            _LOGGER.warning("TION_DIAG set UpdateFailed: %s", e)
            raise HomeAssistantError(f"Tion BLE command failed: {e}") from e

        except TimeoutError as e:
            _LOGGER.warning("TION_DIAG set TimeoutError: %s", e)
            self._mark_disconnected(f"TimeoutError on set: {e}")
            raise HomeAssistantError("Tion BLE command timed out") from e

        except bleak.BleakError as e:
            _LOGGER.warning("TION_DIAG set BleakError: %s", e)
            if self._bleak_service_not_ready(e):
                self._need_hard_reset = True
            self._mark_disconnected(f"BleakError on set: {e}")
            raise HomeAssistantError(f"Tion BLE command failed: {e}") from e

        except Exception as e:
            _LOGGER.exception("TION_DIAG set unexpected error")
            self._mark_disconnected(f"{type(e).__name__} on set: {e}")
            if "service" in str(e).lower() or "timeout" in str(e).lower():
                self._need_hard_reset = True
            raise HomeAssistantError(f"Tion command failed: {type(e).__name__}: {e}") from e

        else:
            self.data.update(original_args)
            self.async_update_listeners()
            setattr(self, "_fail_count", 0)
            self.update_interval = self.__keep_alive
            _LOGGER.warning("TION_DIAG set success: data now=%s", self.data)

    @staticmethod
    def getTion(model: str, mac: str | BLEDevice) -> tion_btle.TionS3 | tion_btle.TionLite | tion_btle.TionS4:
        _LOGGER.warning("TION_DIAG getTion: model=%s source=%s", model, _describe_btle_device(mac))
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
        _LOGGER.warning("TION_DIAG device_info requested: %s", info)
        return info

    @callback
    def update_btle_device(
        self,
        service_info: bluetooth.BluetoothServiceInfoBleak,
        _change: bluetooth.BluetoothChange,
    ) -> None:
        _LOGGER.warning(
            "TION_DIAG bluetooth callback: address=%s rssi=%s name=%s device=%s",
            service_info.address,
            service_info.rssi,
            service_info.name,
            _describe_btle_device(service_info.device),
        )
        if service_info.device is not None:
            self.rssi = service_info.rssi
            self.__tion.update_btle_device(service_info.device)
            _LOGGER.warning("TION_DIAG bluetooth callback updated Tion BLEDevice and RSSI=%s", self.rssi)
