"""Adds support for generic thermostat units."""
from __future__ import annotations

import logging
import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.const import UnitOfTemperature

from homeassistant.components.climate import (
    ClimateEntity,
    ClimateEntityFeature,
    HVACMode,
    HVACAction,
)

from homeassistant.helpers import config_validation as cv, entity_platform
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import TionInstance
from .const import *

_LOGGER = logging.getLogger(__name__)

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Optional(CONF_NAME, default=DEFAULT_NAME): cv.string,
        vol.Required(CONF_MAC): cv.string,
        vol.Optional(CONF_TARGET_TEMP): vol.Coerce(float),
        vol.Optional(CONF_KEEP_ALIVE, default=30): vol.All(cv.time_period, cv.positive_timedelta),
        vol.Optional(CONF_INITIAL_HVAC_MODE): vol.In(
            [HVACMode.FAN_ONLY, HVACMode.HEAT, HVACMode.OFF]
        ),
        vol.Optional(CONF_AWAY_TEMP): vol.Coerce(float),
    }
)

devices: list[str] = []


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
):
    """Setup entry"""
    tion_instance: TionInstance = hass.data[DOMAIN][config_entry.unique_id]
    unique_id = tion_instance.unique_id

    if unique_id not in devices:
        devices.append(unique_id)
        async_add_entities([TionClimateEntity(hass, tion_instance)])
    else:
        _LOGGER.warning("Device %s is already configured! ", unique_id)

    platform = entity_platform.async_get_current_platform()
    platform.async_register_entity_service(
        "set_air_source",
        {
            vol.Required("source"): vol.In(tion_instance.supported_air_sources),
        },
        "set_air_source",
    )

    return True


class TionClimateEntity(ClimateEntity, CoordinatorEntity):
    """Representation of a Tion device."""

    # режимы HVAC — только то, что реально есть
    _attr_hvac_modes = [HVACMode.HEAT, HVACMode.FAN_ONLY, HVACMode.OFF]

    # температура
    _attr_min_temp = 0
    _attr_max_temp = 30
    _attr_precision = PRECISION_WHOLE
    _attr_target_temperature_step = 1
    _attr_temperature_unit = UnitOfTemperature.CELSIUS

    # скорости вентилятора — строго 1..6 (в Алисе отобразятся «как есть»)
    _attr_fan_modes = [1, 2, 3, 4, 5, 6]

    # пресеты
    _attr_preset_modes = [PRESET_NONE, PRESET_BOOST, PRESET_SLEEP]
    _attr_preset_mode = PRESET_NONE

    # ВАЖНО ДЛЯ АЛИСЫ: добавляем TURN_ON / TURN_OFF
    _attr_supported_features = (
        ClimateEntityFeature.TARGET_TEMPERATURE
        | ClimateEntityFeature.FAN_MODE
        | ClimateEntityFeature.PRESET_MODE
        | ClimateEntityFeature.TURN_ON
        | ClimateEntityFeature.TURN_OFF
    )

    _attr_icon = "mdi:air-purifier"
    _attr_fan_mode: int
    coordinator: TionInstance

    def __init__(self, hass: HomeAssistant, instance: TionInstance):
        CoordinatorEntity.__init__(self=self, coordinator=instance)
        self.hass = hass

        self._away_temp = self.coordinator.away_temp

        # saved states
        self._last_mode: HVACMode | None = None
        self._saved_target_temp: float | None = None
        self._saved_fan_mode: int | None = None

        # current state caches
        self._target_temp: float | None = None
        self._is_boost: bool = False
        self._fan_speed: int = 1

        if self._away_temp:
            self._attr_preset_modes.append(PRESET_AWAY)

        self._attr_device_info = self.coordinator.device_info
        self._attr_name = self.coordinator.name
        self._attr_unique_id = self.coordinator.unique_id

        # первичная загрузка состояния
        self._get_current_state()

        # если при инициализации устройство было «включено», зафиксируем последний не-OFF режим
        if self.hvac_mode and self.hvac_mode != HVACMode.OFF:
            self._last_mode = self.hvac_mode
        else:
            # запасной вариант для turn_on()
            self._last_mode = HVACMode.FAN_ONLY

        ClimateEntity.__init__(self)

    # -------------------- управление режимами --------------------

    async def async_set_hvac_mode(self, hvac_mode: HVACMode):
        """Set hvac mode."""
        _LOGGER.info("Need to set mode to %s, current mode is %s", hvac_mode, self.hvac_mode)

        if self.hvac_mode == hvac_mode:
            _LOGGER.debug(
                "%s asked for mode %s, but it is already active. Skipping.",
                self.name,
                hvac_mode,
            )
            return

        if hvac_mode == HVACMode.OFF:
            # помним последний режим — пригодится для turn_on()
            self._last_mode = self.hvac_mode if self.hvac_mode != HVACMode.OFF else self._last_mode
            await self._async_set_state(is_on=False)

        elif hvac_mode == HVACMode.HEAT:
            saved_target_temp = self.target_temperature
            await self._async_set_state(heater=True, is_on=True)
            self._last_mode = HVACMode.HEAT
            if self.hvac_mode == HVACMode.FAN_ONLY and saved_target_temp is not None:
                await self.async_set_temperature(**{ATTR_TEMPERATURE: saved_target_temp})

        elif hvac_mode == HVACMode.FAN_ONLY:
            await self._async_set_state(heater=False, is_on=True)
            self._last_mode = HVACMode.FAN_ONLY

        else:
            _LOGGER.error("Unrecognized hvac mode: %s", hvac_mode)
            return

        # Обновляем состояние после команды
        self._handle_coordinator_update()

    async def async_turn_on(self):
        """Включить бризер. Восстанавливаем последний не-OFF режим (по умолчанию FAN_ONLY)."""
        _LOGGER.debug("Turning on from %s to %s", self.hvac_mode, self._last_mode)
        if self.hvac_mode != HVACMode.OFF:
            return
        await self.async_set_hvac_mode(self._last_mode or HVACMode.FAN_ONLY)

    async def async_turn_off(self):
        """Выключить бризер."""
        _LOGGER.debug("Turning off from %s", self.hvac_mode)
        await self.async_set_hvac_mode(HVACMode.OFF)

    # -------------------- пресеты --------------------

    async def async_set_preset_mode(self, preset_mode: str):
        """Set new preset mode."""
        actions = []
        _LOGGER.debug("Going to change preset mode from %s to %s", self.preset_mode, preset_mode)

        if preset_mode == PRESET_AWAY and self.preset_mode != PRESET_AWAY:
            _LOGGER.info("Going to AWAY mode. Will save target temperature %s", self.target_temperature)
            self._saved_target_temp = self.target_temperature
            actions.append([self._async_set_state, {"heater_temp": self._away_temp}])

        if preset_mode != PRESET_AWAY and self.preset_mode == PRESET_AWAY and self._saved_target_temp:
            _LOGGER.info("Returning from AWAY mode: will set saved temperature %s", self._saved_target_temp)
            actions.append([self._async_set_state, {"heater_temp": self._saved_target_temp}])
            self._saved_target_temp = None

        if preset_mode == PRESET_SLEEP and self.preset_mode != PRESET_SLEEP:
            _LOGGER.info("Going to night mode: will save fan_speed: %s", self.fan_mode)
            if self._saved_fan_mode is None:
                self._saved_fan_mode = int(self.fan_mode)
            actions.append(
                [self.async_set_fan_mode, {"fan_mode": min(int(self.fan_mode), self.sleep_max_fan_mode)}]
            )

        if preset_mode == PRESET_BOOST and not self._is_boost:
            self._is_boost = True
            if self._saved_fan_mode is None:
                self._saved_fan_mode = int(self.fan_mode)
            actions.append([self.async_set_fan_mode, {"fan_mode": self.boost_fan_mode}])

        if self.preset_mode in [PRESET_BOOST, PRESET_SLEEP] and preset_mode not in [PRESET_BOOST, PRESET_SLEEP]:
            _LOGGER.info(
                "Returning from %s mode. Going to set fan speed %d",
                self.preset_mode,
                self._saved_fan_mode,
            )
            if self.preset_mode == PRESET_BOOST:
                self._is_boost = False

            if self._saved_fan_mode is not None:
                actions.append([self.async_set_fan_mode, {"fan_mode": self._saved_fan_mode}])
                self._saved_fan_mode = None

        self._attr_preset_mode = preset_mode

        for a in actions:
            await a[0](**a[1])

        self._attr_preset_mode = preset_mode
        self._handle_coordinator_update()

    @property
    def boost_fan_mode(self) -> int:
        """Fan speed for boost mode (max of supported)."""
        return max(int(x) for x in self.fan_modes)

    @property
    def sleep_max_fan_mode(self) -> int:
        """Maximum fan speed for sleep mode."""
        return 2

    # -------------------- вентилятор/температура --------------------

    async def async_set_fan_mode(self, fan_mode):
        if self.preset_mode == PRESET_SLEEP and int(fan_mode) > self.sleep_max_fan_mode:
            _LOGGER.info(
                "Fan speed %s requested, but SLEEP mode limits it to %d",
                fan_mode,
                self.sleep_max_fan_mode,
            )
            fan_mode = self.sleep_max_fan_mode

        if (self.preset_mode == PRESET_BOOST and self._is_boost) and fan_mode != self.boost_fan_mode:
            _LOGGER.debug("In BOOST mode. Ignoring requested fan speed %s", fan_mode)
            fan_mode = self.boost_fan_mode

        if fan_mode != self.fan_mode or not self.coordinator.data.get("is_on"):
            self._fan_speed = int(fan_mode)
            await self._async_set_state(fan_speed=int(fan_mode), is_on=True)

    async def async_set_temperature(self, **kwargs):
        """Set new target temperature."""
        temperature = kwargs.get(ATTR_TEMPERATURE)
        if temperature is None:
            return
        self._target_temp = float(temperature)
        await self._async_set_state(heater_temp=float(temperature))

    # -------------------- базовые утилиты --------------------

    async def _async_set_state(self, **kwargs):
        await self.coordinator.set(**kwargs)
        self._handle_coordinator_update()

    def _handle_coordinator_update(self) -> None:
        self._get_current_state()

        # если вышли из BOOST, но скорость осталась не boost — сбросим BOOST
        if int(self.fan_mode) != self.boost_fan_mode and (self._is_boost or self.preset_mode == PRESET_BOOST):
            _LOGGER.warning(
                "BOOST mode flagged, but current speed %s != boost speed %s. Dropping BOOST.",
                self.fan_mode,
                self.boost_fan_mode,
            )
            self._is_boost = False
            self._attr_preset_mode = PRESET_NONE

        self.async_write_ha_state()

    def _get_current_state(self):
        self._attr_target_temperature = self.coordinator.data.get("heater_temp")
        self._attr_current_temperature = self.coordinator.data.get("out_temp")
        self._attr_fan_mode = self.coordinator.data.get("fan_speed")
        self._attr_assumed_state = False if self.coordinator.last_update_success else True
        self._attr_extra_state_attributes = {
            "air_mode": self.coordinator.data.get("in_temp")  # оставлено как было
        }
        self._attr_hvac_mode = (
            HVACMode.OFF
            if not self.coordinator.data.get("is_on")
            else HVACMode.HEAT
            if self.coordinator.data.get("heater")
            else HVACMode.FAN_ONLY
        )
        self._attr_hvac_action = (
            HVACAction.OFF
            if not self.coordinator.data.get("is_on")
            else HVACAction.HEATING
            if self.coordinator.data.get("is_heating")
            else HVACAction.FAN
        )

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        return True

    async def set_air_source(self, source: str):
        _LOGGER.debug("set_air_source: %s", source)
        await self.coordinator.set(mode=source)

    @property
    def fan_mode(self) -> str | None:
        return str(self._attr_fan_mode)

    @property
    def fan_modes(self) -> list[str] | None:
        return [str(i) for i in self._attr_fan_modes]

    @classmethod
    def attr_fan_modes(cls) -> list[int] | None:
        return cls._attr_fan_modes
