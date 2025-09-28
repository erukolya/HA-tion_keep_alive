# ... заголовок файла и импорты оставьте как есть ...
from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from homeassistant.components import bluetooth
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import (
    DOMAIN,
    PLATFORMS,
    CONF_MAC,
    CONF_MODEL,
    CONF_KEEP_ALIVE_S,
)

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, config_entry: ConfigEntry):
    """Создание инстанса и запуск платформ."""
    hass.data.setdefault(DOMAIN, {})

    instance = TionInstance(hass, config_entry)

    # Кладем инстанс по двум ключам: unique_id и entry_id
    hass.data[DOMAIN][config_entry.unique_id] = instance
    hass.data[DOMAIN][config_entry.entry_id] = instance

    # Регистрируем авто-обновление BLE-устройства (если у вас была эта логика — оставьте)
    @callback
    def _ble_seen(device):
        # можно обновлять self._ble_device, если нужно
        return True

    cancel = bluetooth.async_register_callback(
        hass,
        _ble_seen,
        {"address": config_entry.data[CONF_MAC], "connectable": True},
        bluetooth.BluetoothScanningMode.ACTIVE,
    )
    config_entry.async_on_unload(cancel)

    # Первая загрузка: не валим интеграцию, если рукопожатие не успело.
    try:
        await instance.async_config_entry_first_refresh()
    except UpdateFailed as err:
        _LOGGER.warning("Initial refresh failed for %s: %s", config_entry.unique_id, err)

    # Всё равно поднимаем платформы — они будут 'unavailable' пока не законнектимся.
    await hass.config_entries.async_forward_entry_setups(config_entry, PLATFORMS)
    return True



async def async_unload_entry(hass: HomeAssistant, config_entry: ConfigEntry):
    """Отключение конфиг-энтри."""
    unload_ok = await hass.config_entries.async_unload_platforms(config_entry, PLATFORMS)

    instance: TionInstance | None = (
        hass.data.get(DOMAIN, {}).get(config_entry.unique_id)
        or hass.data.get(DOMAIN, {}).get(config_entry.entry_id)
    )
    if instance:
        await instance.async_disconnect()

    # Убираем из обоих ключей
    if config_entry.unique_id in hass.data.get(DOMAIN, {}):
        hass.data[DOMAIN].pop(config_entry.unique_id, None)
    if config_entry.entry_id in hass.data.get(DOMAIN, {}):
        hass.data[DOMAIN].pop(config_entry.entry_id, None)

    if not hass.data[DOMAIN]:
        hass.data.pop(DOMAIN, None)

    return unload_ok


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


class TionInstance(DataUpdateCoordinator):
    """Координатор и владелец соединения с устройством."""

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry) -> None:
        self.hass = hass
        self._config = config_entry

        # keep-alive, интервал опроса
        keep_alive_s = config_entry.options.get(CONF_KEEP_ALIVE_S, 30)
        self.__keep_alive = timedelta(seconds=keep_alive_s)

        # адрес и BLE-устройство
        self._mac: str = config_entry.data[CONF_MAC]
        self._ble_device = bluetooth.async_ble_device_from_address(
            hass, self._mac, connectable=True
        )

        # Экземпляр драйвера (создание как у вас было — не меняем сигнатуры)
        self.__tion = self._build_driver(self._ble_device)

        # состояния соединения
        self._connect_lock = asyncio.Lock()
        self._is_connected = False

        # важно: дольше ждем сервис-дискавери (как просили)
        self._prime_timeout_s = 60.0  # было 8.0 — увеличили до 60

        super().__init__(
            hass,
            _LOGGER,
            name=f"Tion Breezer {self._mac}",
            update_interval=self.__keep_alive,
        )

    def _build_driver(self, ble_device):
        """Создание инстанса драйвера, как у вас в текущем коде (оставьте вашу логику)."""
        # Пример: return TionS4(ble_device or self._mac)
        # Оставьте существующую реализацию.
        return getTion(self._config.data.get(CONF_MODEL), ble_device or self._mac)

    async def _async_update_data(self):
        """Плановый опрос устройства."""
        await self._ensure_connected()
        try:
            # Чтение состояния — оставьте как у вас (например, await self.__tion.get())
            state = await self.__tion.get()
            return state
        except Exception as err:
            # Не рвем платформы, просто помечаем как временную ошибку
            raise UpdateFailed(f"Read failed: {err}") from err

    async def _ensure_connected(self) -> None:
        """Поднять соединение, если оно не готово, и дождаться сервисов."""
        if self._is_connected:
            return

        async with self._connect_lock:
            if self._is_connected:
                return

            # Свежий BLEDevice (на случай переподключений)
            self._ble_device = bluetooth.async_ble_device_from_address(
                self.hass, self._mac, connectable=True
            )

            # Если движок умеет принимать BLEDevice — подкинем.
            if hasattr(self.__tion, "set_ble_device"):
                try:
                    self.__tion.set_ble_device(self._ble_device or self._mac)
                except Exception:  # не критично
                    pass

            # Важно: НИКАКИХ нестандартных аргументов типа persistent=
            try:
                await self.__tion.connect()
            except Exception as err:
                _LOGGER.error("BLE: connect failed: %s", err)
                raise UpdateFailed(err) from err

            # Прайминг сервисов: ждем, пока драйвер перестанет бросать
            # Service Discovery / Not Ready — до 60 секунд.
            if not await self._prime_services():
                try:
                    await self.__tion.disconnect()
                except Exception:
                    pass
                raise UpdateFailed("Handshake timeout: BLE services are not ready")

            self._is_connected = True
            _LOGGER.debug("BLE: connected and primed %s", self._mac)

    async def _prime_services(self) -> bool:
        """Нежно дергаем устройство до успешного ответа или таймаута."""
        deadline = self.hass.loop.time() + self._prime_timeout_s
        last_err: Exception | None = None

        while self.hass.loop.time() < deadline:
            try:
                # Легкая операция чтения, которая требует готовых сервисов
                await self.__tion.get()
                return True
            except Exception as err:
                last_err = err
                # типичные ошибки: Service Discovery not performed / MaxTriesExceeded
                await asyncio.sleep(1.0)

        _LOGGER.warning("BLE prime still not ready for %s: %s", self._mac, last_err)
        return False

    async def async_disconnect(self):
        """Явное отключение."""
        if not self._is_connected:
            return
        try:
            await self.__tion.disconnect()
        except Exception:
            pass
        finally:
            self._is_connected = False


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
