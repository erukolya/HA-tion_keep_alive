from homeassistant.components.select import SelectEntityDescription, SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import TionInstance
from .const import DOMAIN

INPUT_SELECTS: tuple[SelectEntityDescription, ...] = (
    SelectEntityDescription(
            key="mode",
            name="Air mode",
            icon="mdi:air-filter",
            entity_registry_enabled_default=True,
            entity_category=EntityCategory.CONFIG,
        ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
):
    """Set up Tion select platform for a config entry."""
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
        # В этом файле раньше не логировали — просто выходим «мягко».
        return False

    async_add_entities(
        [TionSelect(TionSelectConfig(**dict(config_entry.data)), tion_instance, hass)]
    )
    return True


class TionInputSelect(SelectEntity, CoordinatorEntity):
    coordinator: TionInstance

    def select_option(self, option: str) -> None:
        pass

    async def async_select_option(self, option: str) -> None:
        await self.coordinator.set(mode=option)
        self._handle_coordinator_update()

    def __init__(self, description: SelectEntityDescription, instance: TionInstance, hass: HomeAssistant):
        CoordinatorEntity.__init__(self=self, coordinator=instance, )
        self.hass = hass

        self.entity_description = description
        self._attr_name = f"{instance.name} {description.name}"
        self._attr_device_info = instance.device_info
        self._attr_unique_id = f"{instance.unique_id}-{description.key}"
        self._attr_icon = self.entity_description.icon
        self._attr_entity_registry_enabled_default = self.entity_description.entity_registry_enabled_default
        self._attr_entity_category = self.entity_description.entity_category

        self._attr_options = self.coordinator.supported_air_sources
        self._attr_current_option = self.coordinator.data.get(self.entity_description.key)

    def _handle_coordinator_update(self) -> None:
        self._attr_current_option = self.coordinator.data.get(self.entity_description.key)
        self._attr_assumed_state = False if self.coordinator.last_update_success else True
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        return True
