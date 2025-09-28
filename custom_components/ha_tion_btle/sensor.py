"""
Sensors for Tion breezers
"""
import logging
from datetime import timedelta

from homeassistant.components.sensor import SensorEntityDescription, SensorDeviceClass, SensorStateClass, SensorEntity
from homeassistant.const import UnitOfTemperature
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import TionInstance
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

SCAN_INTERVAL = timedelta(seconds=30)

SENSOR_TYPES: tuple[SensorEntityDescription, ...] = (
    SensorEntityDescription(
        key="in_temp",
        name="input temperature",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        entity_registry_enabled_default=True,
        icon="mdi:import",
    ),
    SensorEntityDescription(
        key="out_temp",
        name="output temperature",
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        entity_registry_enabled_default=True,
        icon="mdi:export",
    ),
    SensorEntityDescription(
        key="filter_remain",
        name="filters remain",
        entity_registry_enabled_default=True,
        entity_category=EntityCategory.DIAGNOSTIC,
    ),

    SensorEntityDescription(
        key="fan_speed",
        name="current fan speed",
        entity_registry_enabled_default=True,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:fan",
    ),
    SensorEntityDescription(
        key="rssi",
        name="rssi",
        entity_registry_enabled_default=False,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        icon="mdi:access-point",
    ),
)


async def async_setup_platform(_hass: HomeAssistant, _config, _async_add_entities, _discovery_info=None):
    _LOGGER.critical("Sensors configuration via configuration.yaml is not supported!")
    return False


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
):
    """Set up the sensor platform for a config entry."""
    bucket = hass.data.get(DOMAIN, {})

    candidates: list[str] = []
    uid = getattr(config_entry, "unique_id", None)
    if uid:
        candidates += [uid, uid.upper(), uid.lower()]

    mac = getattr(config_entry, "data", {}).get("mac")
    if mac:
        candidates += [mac, mac.upper(), mac.lower()]

    eid = getattr(config_entry, "entry_id", None)
    if eid:
        candidates.append(eid)

    tion_instance = None
    for key in [k for k in candidates if k]:
        if key in bucket:
            tion_instance = bucket[key]
            break

    if not tion_instance:
        _LOGGER.error(
            "Tion (sensor): instance for %s not found in hass.data[%s]. Available keys: %s",
            uid or mac or eid,
            DOMAIN,
            ", ".join(bucket.keys()),
        )
        return False

    entities: list[TionSensor] = [
        TionSensor(description, tion_instance) for description in SENSOR_TYPES
    ]
    async_add_entities(entities)
    return True


class TionSensor(SensorEntity, CoordinatorEntity):
    """Representation of a sensor."""

    def __init__(self, description: SensorEntityDescription, instance: TionInstance):
        """Initialize the sensor."""

        CoordinatorEntity.__init__(
            self=self,
            coordinator=instance,
        )
        self.entity_description = description
        self._attr_name = f"{instance.name} {description.name}"
        self._attr_device_info = instance.device_info
        self._attr_unique_id = f"{instance.unique_id}-{description.key}"

        _LOGGER.debug(f"Init of sensor {self.name} ({instance.unique_id})")

    @property
    def native_value(self):
        """Return the state of the sensor."""
        value = self.coordinator.data.get(self.entity_description.key)

        if self.entity_description.key == "fan_speed":
            if not self.coordinator.data.get("is_on"):
                # return zero fan speed if breezer turned off
                value = 0

        return value

    def _handle_coordinator_update(self) -> None:
        self._attr_assumed_state = False if self.coordinator.last_update_success else True
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        return True
