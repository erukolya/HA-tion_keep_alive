# custom_components/ha_tion_btle/tion_btle/persistent.py
"""
Persistent BLE connection manager for Tion devices (S3/S4/Lite).

- Keeps a single long-lived connection per MAC.
- Auto-reconnects on failures with exponential backoff.
- Re-applies notification subscriptions after reconnect.
- Optional keepalive ping (read/write) to prevent idle disconnects.
- Thread-safe; BluePy is blocking, so we serialize operations internally.

Usage (later in device code):
    mgr = BleManager.get(mac, iface=0)
    mgr.register_notify_handler(handler)           # handler(cHandle: int, data: bytes)
    mgr.subscribe(cccd_handle)                     # enable notifications
    data = mgr.read(handle)
    mgr.write(handle, payload, with_response=True)
    mgr.wait_for_notifications(timeout)

You normally DO NOT call disconnect(); Home Assistant will keep it alive.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Dict, Optional, Set

try:
    # BluePy is the current backend used by HA-tion
    from bluepy.btle import DefaultDelegate, Peripheral, BTLEDisconnectError, BTLEException
except Exception as e:  # pragma: no cover
    raise RuntimeError("bluepy is required for persistent BLE manager") from e


_LOGGER = logging.getLogger("tion_btle.persistent")


class _NotifyDelegate(DefaultDelegate):
    def __init__(self, callback_getter: Callable[[], Optional[Callable[[int, bytes], None]]]):
        super().__init__()
        self._cb_getter = callback_getter

    def handleNotification(self, cHandle: int, data: bytes) -> None:  # noqa: N802 (bluepy naming)
        cb = self._cb_getter()
        if cb:
            try:
                cb(cHandle, data)
            except Exception:  # pragma: no cover
                _LOGGER.exception("Notify handler raised")


class PersistentPeripheral:
    """
    A thin, thread-safe wrapper around bluepy.Peripheral that maintains a persistent connection.
    Do not instantiate directly; use BleManager.get(mac, iface).
    """

    def __init__(
        self,
        mac: str,
        iface: int = 0,
        connect_timeout: float = 10.0,
        keepalive_interval: float = 30.0,
        keepalive_read_handle: Optional[int] = None,
        keepalive_write: Optional[tuple[int, bytes]] = None,  # (handle, payload)
    ) -> None:
        self._mac = mac
        self._iface = iface
        self._connect_timeout = connect_timeout
        self._keepalive_interval = keepalive_interval
        self._keepalive_read_handle = keepalive_read_handle
        self._keepalive_write = keepalive_write

        self._per: Optional[Peripheral] = None
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._notify_cb: Optional[Callable[[int, bytes], None]] = None
        self._delegate = _NotifyDelegate(lambda: self._notify_cb)

        # Subscriptions (CCCD handles) to restore after reconnect
        self._subscriptions: Set[int] = set()

        # Worker thread
        self._thr = threading.Thread(target=self._worker, name=f"tion-ble-{mac.replace(':','')}", daemon=True)
        self._thr.start()

    # ----------------------- public API -----------------------

    def is_connected(self) -> bool:
        return self._connected.is_set()

    def register_notify_handler(self, cb: Optional[Callable[[int, bytes], None]]) -> None:
        with self._lock:
            self._notify_cb = cb
            if self._per:
                self._per.setDelegate(self._delegate)

    def subscribe(self, cccd_handle: int) -> None:
        """
        Enable notifications by writing b'\x01\x00' to CCCD handle.
        IMPORTANT: pass the CCCD handle (NOT the value handle). In HA-tion it was often value+1.
        """
        with self._lock:
            self._ensure_connected_locked()
            try:
                # Write to CCCD with response to ensure it is set
                self._per.writeCharacteristic(cccd_handle, b"\x01\x00", withResponse=True)
                self._subscriptions.add(cccd_handle)
                _LOGGER.debug("Subscribed on CCCD handle=%d for %s", cccd_handle, self._mac)
            except (BTLEDisconnectError, BTLEException) as e:
                _LOGGER.warning("Subscribe failed on %s (handle=%d): %s", self._mac, cccd_handle, e)
                self._handle_disconnect_locked()

    def read(self, handle: int) -> bytes:
        with self._lock:
            self._ensure_connected_locked()
            try:
                return self._per.readCharacteristic(handle)
            except (BTLEDisconnectError, BTLEException) as e:
                _LOGGER.warning("Read failed on %s (handle=%d): %s", self._mac, handle, e)
                self._handle_disconnect_locked()
                raise

    def write(self, handle: int, data: bytes, with_response: bool = True) -> None:
        with self._lock:
            self._ensure_connected_locked()
            try:
                self._per.writeCharacteristic(handle, data, withResponse=with_response)
            except (BTLEDisconnectError, BTLEException) as e:
                _LOGGER.warning(
                    "Write failed on %s (handle=%d, len=%d): %s",
                    self._mac,
                    handle,
                    len(data),
                    e,
                )
                self._handle_disconnect_locked()
                raise

    def wait_for_notifications(self, timeout: float) -> bool:
        """
        Waits for notifications. Returns True if a notification was handled.
        Safe to call repeatedly; auto-reconnects if needed.
        """
        with self._lock:
            self._ensure_connected_locked()
            try:
                return self._per.waitForNotifications(timeout)
            except (BTLEDisconnectError, BTLEException) as e:
                _LOGGER.debug("waitForNotifications disconnect on %s: %s", self._mac, e)
                self._handle_disconnect_locked()
                return False

    def set_keepalive(
        self,
        interval_sec: float,
        read_handle: Optional[int] = None,
        write_tuple: Optional[tuple[int, bytes]] = None,
    ) -> None:
        """
        Configure keepalive strategy. Either a read handle or a (write_handle, payload) tuple.
        """
        with self._lock:
            self._keepalive_interval = max(5.0, float(interval_sec))
            self._keepalive_read_handle = read_handle
            self._keepalive_write = write_tuple

    def disconnect(self) -> None:
        """Gracefully stop worker and disconnect (used on HA shutdown)."""
        self._stop.set()
        self._connected.clear()
        # do not hold lock while join (avoid deadlock with worker trying to acquire)
        try:
            if self._thr.is_alive():
                self._thr.join(timeout=5.0)
        finally:
            with self._lock:
                if self._per:
                    try:
                        self._per.disconnect()
                    except Exception:
                        pass
                    self._per = None

    # ----------------------- internals -----------------------

    def _ensure_connected_locked(self) -> None:
        if self._connected.is_set() and self._per is not None:
            return
        # actively wake worker to connect now (busy-wait with backoff)
        deadline = time.time() + self._connect_timeout
        while not self._connected.is_set() and time.time() < deadline:
            # worker is trying; just sleep very briefly
            time.sleep(0.05)
        if not self._connected.is_set() or self._per is None:
            raise BTLEDisconnectError("Peripheral is not connected")

    def _handle_disconnect_locked(self) -> None:
        self._connected.clear()
        # actual closing done by worker loop

    def _worker(self) -> None:
        backoff = 1.0  # seconds
        while not self._stop.is_set():
            if not self._connected.is_set():
                try:
                    self._connect_once()
                    _LOGGER.info("Connected to %s on hci%d", self._mac, self._iface)
                    backoff = 1.0  # reset backoff after success
                except Exception as e:
                    _LOGGER.warning("Connect failed to %s: %s", self._mac, e)
                    # exponential backoff (cap 30s)
                    sleep_for = min(backoff, 30.0)
                    backoff = min(backoff * 2.0, 30.0)
                    self._sleep_cancellable(sleep_for)
                    continue

            # Connected: do keepalive/wait notifications loop
            ka_next = time.time() + self._keepalive_interval
            while self._connected.is_set() and not self._stop.is_set():
                # Wait notifications in short slices to be responsive for stop/reconnect
                try:
                    notified = False
                    if self._per is not None:
                        notified = self._per.waitForNotifications(1.0)
                    # Keepalive
                    ts = time.time()
                    if ts >= ka_next:
                        with self._lock:
                            # re-check connection and send KA
                            if self._per is None:
                                break
                            try:
                                if self._keepalive_read_handle is not None:
                                    _ = self._per.readCharacteristic(self._keepalive_read_handle)
                                elif self._keepalive_write is not None:
                                    wh, payload = self._keepalive_write
                                    self._per.writeCharacteristic(wh, payload, withResponse=False)
                            except (BTLEDisconnectError, BTLEException) as e:
                                _LOGGER.debug("Keepalive failed on %s: %s", self._mac, e)
                                self._handle_disconnect_locked()
                                break
                        ka_next = ts + self._keepalive_interval
                except (BTLEDisconnectError, BTLEException):
                    with self._lock:
                        self._handle_disconnect_locked()
                    break
                except Exception:
                    _LOGGER.exception("Unexpected error in wait loop for %s", self._mac)
                    # Don't disconnect on arbitrary handler exceptions

            # If we are here, connection considered down; ensure physical disconnect
            with self._lock:
                if self._per is not None:
                    try:
                        self._per.disconnect()
                    except Exception:
                        pass
                    self._per = None
                self._connected.clear()

            # Small pause before reconnect attempts
            self._sleep_cancellable(0.25)

        # stopping: ensure cleanup
        with self._lock:
            if self._per is not None:
                try:
                    self._per.disconnect()
                except Exception:
                    pass
            self._per = None
            self._connected.clear()

    def _connect_once(self) -> None:
        # Don't hold the lock while performing bluetooth operations that can block for seconds
        per = Peripheral()  # late-binding so we can re-create after disconnects
        per.withDelegate(self._delegate)
        per._helper.set_timeout(self._connect_timeout)  # type: ignore[attr-defined]
        per.connect(self._mac, iface=self._iface)
        # Success; apply into object under lock
        with self._lock:
            self._per = per
            self._connected.set()
            self._reapply_subscriptions_locked()

    def _reapply_subscriptions_locked(self) -> None:
        if not self._subscriptions or self._per is None:
            return
        for cccd in list(self._subscriptions):
            try:
                self._per.writeCharacteristic(cccd, b"\x01\x00", withResponse=True)
            except Exception as e:
                _LOGGER.debug("Failed to re-subscribe CCCD=%d on %s: %s", cccd, self._mac, e)

    def _sleep_cancellable(self, seconds: float) -> None:
        # Sleep in small slices so we can react to stop quickly
        end = time.time() + seconds
        while not self._stop.is_set() and time.time() < end:
            time.sleep(0.05)


class BleManager:
    """
    Global connection manager (singleton-like) to share persistent connections per MAC.
    """

    _instances: Dict[tuple[str, int], PersistentPeripheral] = {}
    _glock = threading.RLock()

    @classmethod
    def get(
        cls,
        mac: str,
        iface: int = 0,
        *,
        connect_timeout: float = 10.0,
        keepalive_interval: float = 30.0,
        keepalive_read_handle: Optional[int] = None,
        keepalive_write: Optional[tuple[int, bytes]] = None,
    ) -> PersistentPeripheral:
        key = (mac.upper(), int(iface))
        with cls._glock:
            pp = cls._instances.get(key)
            if pp is None:
                pp = PersistentPeripheral(
                    mac=key[0],
                    iface=key[1],
                    connect_timeout=connect_timeout,
                    keepalive_interval=keepalive_interval,
                    keepalive_read_handle=keepalive_read_handle,
                    keepalive_write=keepalive_write,
                )
                cls._instances[key] = pp
            return pp

    @classmethod
    def stop_all(cls) -> None:
        with cls._glock:
            for pp in list(cls._instances.values()):
                try:
                    pp.disconnect()
                except Exception:
                    pass
            cls._instances.clear()
