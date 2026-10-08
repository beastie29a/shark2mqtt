"""MQTT client with Home Assistant autodiscovery and command handling."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import TYPE_CHECKING, Any, Self

import aiomqtt

from .const import VERSION

if TYPE_CHECKING:
    from .config import Settings
    from .shark_device import SharkVacuum


logger = logging.getLogger(__name__)

# HA discovery prefix (standard)
HA_DISCOVERY_PREFIX = "homeassistant"

# Use HA device discovery (one retained payload per device at
# homeassistant/device/{uid}/config) instead of the legacy per-component
# config topics. Flip to False to roll back to the legacy path; the legacy
# implementation is kept as _publish_discovery_legacy for that purpose.
USE_DEVICE_DISCOVERY = True


class MqttClient:
    """Async MQTT client for shark2mqtt."""

    def __init__(self, config: Settings) -> None:
        self._config = config
        self._prefix = config.mqtt_prefix
        self._client: aiomqtt.Client | None = None
        # device_id -> clean mode: "Normal"/"Matrix" for dry-only models,
        # "Wet"/"Dry" for wet/dry-capable models (CleaningParameters present)
        self._clean_modes: dict[str, str] = {}
        self._fan_speed_overrides: dict[str, str] = {}  # device_id -> user-set speed
        self._water_flow_overrides: dict[str, str] = {}  # device_id -> user-set flow level
        self._published_rooms: dict[str, set[str]] = {}  # device_id -> room slugs
        self._discovery_sigs: dict[str, str] = {}  # device_id -> last published signature
        self._migrated: set[str] = set()  # device_ids that got the one-time migrate_discovery signal

    async def __aenter__(self) -> Self:
        will = aiomqtt.Will(
            topic=f"{self._prefix}/status",
            payload=json.dumps({"state": "offline"}),
            qos=1,
            retain=True,
        )
        self._client = aiomqtt.Client(
            hostname=self._config.mqtt_host,
            port=self._config.mqtt_port,
            username=self._config.mqtt_username,
            password=self._config.mqtt_password,
            will=will,
        )
        await self._client.__aenter__()
        # Announce online
        await self._publish(f"{self._prefix}/status", {"state": "online"}, retain=True)
        logger.info("MQTT connected to %s:%d", self._config.mqtt_host, self._config.mqtt_port)
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._client:
            await self._publish(f"{self._prefix}/status", {"state": "offline"}, retain=True)
            await self._client.__aexit__(*args)
            self._client = None

    async def _publish(self, topic: str, payload: Any, retain: bool = False) -> None:
        assert self._client is not None
        msg = json.dumps(payload) if isinstance(payload, dict) else str(payload)
        await self._client.publish(topic, msg, qos=1, retain=retain)

    # --- HA Autodiscovery ---

    async def publish_discovery(self, device: SharkVacuum) -> None:
        """Publish HA discovery for a vacuum and its sensors.

        Device discovery (HA 2025.5+): one retained payload per device at
        homeassistant/device/{uid}/config carrying dev/origin/availability
        plus every component under "cmps". Before the first device payload
        is published, a one-time {"migrate_discovery": true} (unretained)
        is sent to each legacy config topic so HA moves existing entities
        over instead of creating duplicates.
        """
        if not USE_DEVICE_DISCOVERY:
            await self._publish_discovery_legacy(device)
            return

        dsn = device.dsn
        uid = f"shark2mqtt_{dsn}"
        slug = re.sub(r"[^a-z0-9]+", "_", device.product_name.lower()).strip("_")
        topic = f"{self._prefix}/{dsn}"

        # Same signature-based dedup as the legacy path: discovery payloads
        # are retained, so republish every poll cycle is pure noise (#29).
        sig = json.dumps(
            {
                "device": device.device_info,
                "rooms": device.rooms,
                "has_flow_mode": device.has_flow_mode,
                "has_wet_dry": device.has_wet_dry,
            },
            sort_keys=True,
        )
        if self._discovery_sigs.get(dsn) == sig:
            logger.debug("Discovery unchanged for %s (%s), skipping", device.product_name, dsn)
            return

        # One-time migration signal: tell HA to move the existing
        # single-component entities to device discovery. Must precede the
        # device payload and must not be retained — HA treats it as an
        # event, not a config.
        if dsn not in self._migrated:
            for legacy_topic in self._legacy_config_topics(device, uid):
                await self._publish(legacy_topic, {"migrate_discovery": True})
            self._migrated.add(dsn)
            logger.info("Signaled HA to migrate %s to device discovery", dsn)

        payload = self._build_device_discovery_payload(device, uid, slug, topic)
        await self._publish(f"{HA_DISCOVERY_PREFIX}/device/{uid}/config", payload, retain=True)

        # Clean mode state publish (unchanged from the legacy path)
        if device.rooms:
            if device.has_wet_dry:
                options = ["Wet", "Dry"]
                default_mode = "Dry"
            else:
                options = ["Normal", "Matrix"]
                default_mode = "Normal"
            mode = self._clean_modes.get(dsn, default_mode)
            if mode not in options:
                mode = default_mode
            await self._publish(f"{topic}/clean_mode/state", mode, retain=True)

        # Stale room buttons no longer need empty-payload retracts — they
        # are simply absent from the republished payload. Keep the diff for
        # log visibility.
        current_room_slugs = {
            re.sub(r"[^a-z0-9]+", "_", room.lower()).strip("_")
            for room in (device.rooms or [])
        }
        prev_rooms = self._published_rooms.get(dsn, set())
        for room_slug in prev_rooms - current_room_slugs:
            logger.info("Removed stale room button %s for %s", room_slug, dsn)
        self._published_rooms[dsn] = current_room_slugs
        self._discovery_sigs[dsn] = sig

        logger.info("Published HA device discovery for %s (%s)", device.product_name, dsn)

    def _build_device_discovery_payload(
        self, device: SharkVacuum, uid: str, slug: str, topic: str,
    ) -> dict[str, Any]:
        """Build the single retained device-discovery payload for a device."""
        attrs_topic = f"{topic}/attributes"
        availability_topic = f"{topic}/available"

        def sensor(
            name: str, key: str, template: str,
            unit: str | None = None,
            device_class: str | None = None,
            state_class: str | None = None,
            icon: str | None = None,
            entity_category: str | None = None,
        ) -> dict[str, Any]:
            cfg: dict[str, Any] = {
                "p": "sensor",
                "unique_id": f"{uid}_{key}",
                "name": name,
                "object_id": f"{slug}_{key}",
                "state_topic": attrs_topic,
                "value_template": template,
            }
            if unit is not None:
                cfg["unit_of_measurement"] = unit
            if device_class is not None:
                cfg["device_class"] = device_class
            if state_class is not None:
                cfg["state_class"] = state_class
            if icon is not None:
                cfg["icon"] = icon
            if entity_category is not None:
                cfg["entity_category"] = entity_category
            return cfg

        def binary_sensor(
            name: str, key: str, template: str,
            device_class: str | None = None,
            entity_category: str | None = None,
        ) -> dict[str, Any]:
            cfg: dict[str, Any] = {
                "p": "binary_sensor",
                "unique_id": f"{uid}_{key}",
                "name": name,
                "object_id": f"{slug}_{key}",
                "state_topic": attrs_topic,
                "value_template": template,
                "payload_on": True,
                "payload_off": False,
            }
            if device_class is not None:
                cfg["device_class"] = device_class
            if entity_category is not None:
                cfg["entity_category"] = entity_category
            return cfg

        def button(
            name: str, key: str, command_topic: str,
            payload_press: Any, icon: str,
        ) -> dict[str, Any]:
            return {
                "p": "button",
                "unique_id": f"{uid}_{key}",
                "name": name,
                "object_id": f"{slug}_{key}",
                "command_topic": command_topic,
                "payload_press": payload_press,
                "icon": icon,
            }

        cmps: dict[str, dict[str, Any]] = {
            "vacuum": {
                "p": "vacuum",
                "unique_id": uid,
                "name": None,
                "object_id": slug,
                "state_topic": f"{topic}/state",
                "json_attributes_topic": attrs_topic,
                "command_topic": f"{topic}/command",
                "send_command_topic": f"{topic}/send_command",
                "set_fan_speed_topic": f"{topic}/set_fan_speed",
                "fan_speed_list": ["eco", "normal", "max"],
                "supported_features": [
                    "start", "stop", "pause", "return_home",
                    "locate", "fan_speed", "status", "send_command",
                ],
                "value_template": "{{ value_json.state }}",
            },
        }

        # Water flow select — only for models with a mop tank; omitting it
        # from cmps is the retraction under device discovery.
        if device.has_flow_mode:
            cmps["water_flow"] = {
                "p": "select",
                "unique_id": f"{uid}_water_flow",
                "name": "Water Flow Level",
                "object_id": f"{slug}_water_flow",
                "command_topic": f"{topic}/set_water_flow",
                "state_topic": attrs_topic,
                "value_template": "{{ value_json.water_flow }}",
                "options": ["eco", "normal", "max"],
                "icon": "mdi:water-percent",
            }

        cmps["battery"] = sensor(
            "Battery", "battery", "{{ value_json.battery_level }}",
            unit="%", device_class="battery", state_class="measurement",
        )
        cmps["rssi"] = sensor(
            "WiFi Signal", "rssi", "{{ value_json.rssi }}",
            unit="dBm", device_class="signal_strength", state_class="measurement",
            entity_category="diagnostic",
        )
        cmps["charging"] = binary_sensor(
            "Charging", "charging", "{{ value_json.is_charging }}",
            device_class="battery_charging",
        )
        cmps["error"] = binary_sensor(
            "Error", "error", "{{ value_json.error_code != 0 }}",
            device_class="problem",
        )
        cmps["error_text"] = sensor(
            "Error Status", "error_text", "{{ value_json.error_text }}",
            icon="mdi:alert-circle-outline", entity_category="diagnostic",
        )
        cmps["evacuating"] = binary_sensor(
            "Emptying Bin", "evacuating", "{{ value_json.is_evacuating }}",
            device_class="running",
        )
        cmps["warning"] = binary_sensor(
            "Warning", "warning", "{{ value_json.warning_code != 0 }}",
            device_class="problem", entity_category="diagnostic",
        )
        cmps["dock_error"] = sensor(
            "Dock Error Code", "dock_error", "{{ value_json.dock_error_code }}",
            icon="mdi:home-alert-outline", entity_category="diagnostic",
        )
        cmps["runtime"] = sensor(
            "Total Runtime", "runtime", "{{ value_json.run_time_cumulative }}",
            icon="mdi:history", entity_category="diagnostic",
        )
        cmps["replace_battery"] = binary_sensor(
            "Replace Battery", "replace_battery", "{{ value_json.replace_battery }}",
            device_class="problem", entity_category="diagnostic",
        )
        cmps["recommend_randr"] = binary_sensor(
            "Recommend Rest And Recharge", "recommend_randr",
            "{{ value_json.recommend_rest_and_recharge }}",
            device_class="problem", entity_category="diagnostic",
        )

        # unique_id is required here: it was missing from the legacy
        # single-component config and HA's migration refuses entities
        # without one.
        cmps["error_trigger"] = {
            "p": "device_automation",
            "unique_id": f"{uid}_error_trigger",
            "automation_type": "trigger",
            "type": "action",
            "subtype": "error",
            "topic": f"{topic}/error_event",
        }

        cmps["map"] = {
            "p": "image",
            "unique_id": f"{uid}_map",
            "name": "Map",
            "object_id": f"{slug}_map",
            "image_topic": f"{topic}/map_image",
            "content_type": "image/png",
        }

        for room in device.rooms or []:
            room_slug = re.sub(r"[^a-z0-9]+", "_", room.lower()).strip("_")
            cmps[f"clean_{room_slug}"] = button(
                f"Clean {room}", f"clean_{room_slug}",
                f"{topic}/clean_room",
                json.dumps({"room": room}),
                "mdi:robot-vacuum",
            )

        if device.rooms:
            if device.has_wet_dry:
                options = ["Wet", "Dry"]
            else:
                options = ["Normal", "Matrix"]
            cmps["clean_mode"] = {
                "p": "select",
                "unique_id": f"{uid}_clean_mode",
                "name": "Clean Mode",
                "object_id": f"{slug}_clean_mode",
                "command_topic": f"{topic}/clean_mode",
                "state_topic": f"{topic}/clean_mode/state",
                "options": options,
                "icon": "mdi:broom",
            }
            # Deep button (wet/dry models only) — omitted otherwise.
            if device.has_wet_dry:
                cmps["deep"] = button(
                    "Deep", "deep", f"{topic}/send_command",
                    "vacuum_and_mop", "mdi:water-pump",
                )

        return {
            "dev": device.device_info,
            "o": {
                "name": "shark2mqtt",
                "sw": VERSION,
                "su": "https://github.com/CamSoper/shark2mqtt",
            },
            "avty": {
                "t": availability_topic,
                "pl": "online",
                "npl": "offline",
            },
            "cmps": cmps,
        }

    def _legacy_config_topics(self, device: SharkVacuum, uid: str) -> list[str]:
        """Legacy single-component config topics for the migrate_discovery signal.

        Mirrors the topics _publish_discovery_legacy uses, so the signal
        reaches every entity HA currently knows about for this device.
        """
        prefix = f"{HA_DISCOVERY_PREFIX}/"
        topics = [
            f"{prefix}vacuum/{uid}/config",
            f"{prefix}sensor/{uid}_battery/config",
            f"{prefix}sensor/{uid}_rssi/config",
            f"{prefix}binary_sensor/{uid}_charging/config",
            f"{prefix}binary_sensor/{uid}_error/config",
            f"{prefix}sensor/{uid}_error_text/config",
            f"{prefix}binary_sensor/{uid}_evacuating/config",
            f"{prefix}binary_sensor/{uid}_warning/config",
            f"{prefix}sensor/{uid}_dock_error/config",
            f"{prefix}sensor/{uid}_runtime/config",
            f"{prefix}binary_sensor/{uid}_replace_battery/config",
            f"{prefix}binary_sensor/{uid}_recommend_randr/config",
            f"{prefix}device_automation/{uid}_error_trigger/config",
            f"{prefix}image/{uid}_map/config",
        ]
        if device.has_flow_mode:
            topics.append(f"{prefix}select/{uid}_water_flow/config")
        if device.rooms:
            for room in device.rooms:
                room_slug = re.sub(r"[^a-z0-9]+", "_", room.lower()).strip("_")
                topics.append(f"{prefix}button/{uid}_clean_{room_slug}/config")
            topics.append(f"{prefix}select/{uid}_clean_mode/config")
            if device.has_wet_dry:
                topics.append(f"{prefix}button/{uid}_deep/config")
        return topics

    async def remove_discovery(self, device: SharkVacuum) -> None:
        """Remove a device and all its entities (empty retained payload).

        Device discovery has no per-entity retract — clearing the single
        device config topic removes everything. Wire into the device
        removal path in main.py when one is added.
        """
        uid = f"shark2mqtt_{device.dsn}"
        await self._publish(f"{HA_DISCOVERY_PREFIX}/device/{uid}/config", "", retain=True)
        self._discovery_sigs.pop(device.dsn, None)
        self._published_rooms.pop(device.dsn, None)
        self._migrated.discard(device.dsn)

    async def _publish_discovery_legacy(self, device: SharkVacuum) -> None:
        """Legacy per-component autodiscovery (kept for one-flag rollback)."""
        dsn = device.dsn
        uid = f"shark2mqtt_{dsn}"
        slug = re.sub(r"[^a-z0-9]+", "_", device.product_name.lower()).strip("_")

        # Discovery configs are retained, so republishing every poll cycle
        # is pure noise (issue #29). Skip unless something that feeds the
        # configs (device info or room list) actually changed.
        sig = json.dumps(
            {
                "device": device.device_info,
                "rooms": device.rooms,
                "has_flow_mode": device.has_flow_mode,
                "has_wet_dry": device.has_wet_dry,
            },
            sort_keys=True,
        )
        if self._discovery_sigs.get(dsn) == sig:
            logger.debug("Discovery unchanged for %s (%s), skipping", device.product_name, dsn)
            return

        # Vacuum entity
        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/vacuum/{uid}/config",
            {
                "name": None,
                "unique_id": uid,
                "object_id": slug,
                "state_topic": f"{self._prefix}/{dsn}/state",
                "json_attributes_topic": f"{self._prefix}/{dsn}/attributes",
                "command_topic": f"{self._prefix}/{dsn}/command",
                "send_command_topic": f"{self._prefix}/{dsn}/send_command",
                "set_fan_speed_topic": f"{self._prefix}/{dsn}/set_fan_speed",
                "fan_speed_list": ["eco", "normal", "max"],
                "supported_features": [
                    "start", "stop", "pause", "return_home",
                    "locate", "fan_speed", "status", "send_command",
                ],
                "availability_topic": f"{self._prefix}/{dsn}/available",
                "payload_available": "online",
                "payload_not_available": "offline",
                "value_template": "{{ value_json.state }}",
                "device": device.device_info,
            },
            retain=True,
        )

        # Water flow level select — only for models that actually have a mop
        # tank. Publishing it unconditionally gave vac-only owners a control
        # that looked real but wrote a property their hardware ignores.
        if device.has_flow_mode:
            await self._publish(
                f"{HA_DISCOVERY_PREFIX}/select/{uid}_water_flow/config",
                {
                    "name": "Water Flow Level",
                    "unique_id": f"{uid}_water_flow",
                    "object_id": f"{slug}_water_flow",
                    "command_topic": f"{self._prefix}/{dsn}/set_water_flow",
                    "state_topic": f"{self._prefix}/{dsn}/attributes",
                    "value_template": "{{ value_json.water_flow }}",
                    "options": ["eco", "normal", "max"],
                    "icon": "mdi:water-percent",
                    "availability_topic": f"{self._prefix}/{dsn}/available",
                    "payload_available": "online",
                    "payload_not_available": "offline",
                    "device": device.device_info,
                },
                retain=True,
            )
        else:
            # Discovery configs are retained, so simply not publishing one
            # leaves any previously-published entity sitting in HA forever.
            # An empty retained payload is autodiscovery's delete.
            await self._publish(
                f"{HA_DISCOVERY_PREFIX}/select/{uid}_water_flow/config",
                "",
                retain=True,
            )

        # Battery sensor
        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/sensor/{uid}_battery/config",
            {
                "name": "Battery",
                "unique_id": f"{uid}_battery",
                "object_id": f"{slug}_battery",
                "state_topic": f"{self._prefix}/{dsn}/attributes",
                "value_template": "{{ value_json.battery_level }}",
                "unit_of_measurement": "%",
                "device_class": "battery",
                "state_class": "measurement",
                "availability_topic": f"{self._prefix}/{dsn}/available",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device.device_info,
            },
            retain=True,
        )

        # RSSI sensor
        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/sensor/{uid}_rssi/config",
            {
                "name": "WiFi Signal",
                "unique_id": f"{uid}_rssi",
                "object_id": f"{slug}_rssi",
                "state_topic": f"{self._prefix}/{dsn}/attributes",
                "value_template": "{{ value_json.rssi }}",
                "unit_of_measurement": "dBm",
                "device_class": "signal_strength",
                "state_class": "measurement",
                "entity_category": "diagnostic",
                "availability_topic": f"{self._prefix}/{dsn}/available",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device.device_info,
            },
            retain=True,
        )

        # Charging binary sensor
        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/binary_sensor/{uid}_charging/config",
            {
                "name": "Charging",
                "unique_id": f"{uid}_charging",
                "object_id": f"{slug}_charging",
                "state_topic": f"{self._prefix}/{dsn}/attributes",
                "value_template": "{{ value_json.is_charging }}",
                "payload_on": True,
                "payload_off": False,
                "device_class": "battery_charging",
                "availability_topic": f"{self._prefix}/{dsn}/available",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device.device_info,
            },
            retain=True,
        )

        # Error binary sensor (ON when error_code != 0)
        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/binary_sensor/{uid}_error/config",
            {
                "name": "Error",
                "unique_id": f"{uid}_error",
                "object_id": f"{slug}_error",
                "state_topic": f"{self._prefix}/{dsn}/attributes",
                "value_template": "{{ value_json.error_code != 0 }}",
                "payload_on": True,
                "payload_off": False,
                "device_class": "problem",
                "availability_topic": f"{self._prefix}/{dsn}/available",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device.device_info,
            },
            retain=True,
        )

        # Error text sensor (shows error description)
        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/sensor/{uid}_error_text/config",
            {
                "name": "Error Status",
                "unique_id": f"{uid}_error_text",
                "object_id": f"{slug}_error_text",
                "state_topic": f"{self._prefix}/{dsn}/attributes",
                "value_template": "{{ value_json.error_text }}",
                "entity_category": "diagnostic",
                "icon": "mdi:alert-circle-outline",
                "availability_topic": f"{self._prefix}/{dsn}/available",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device.device_info,
            },
            retain=True,
        )

        # Emptying bin binary sensor (self-empty dock actively evacuating)
        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/binary_sensor/{uid}_evacuating/config",
            {
                "name": "Emptying Bin",
                "unique_id": f"{uid}_evacuating",
                "object_id": f"{slug}_evacuating",
                "state_topic": f"{self._prefix}/{dsn}/attributes",
                "value_template": "{{ value_json.is_evacuating }}",
                "payload_on": True,
                "payload_off": False,
                "device_class": "running",
                "availability_topic": f"{self._prefix}/{dsn}/available",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device.device_info,
            },
            retain=True,
        )

        # Warning binary sensor (separate channel from Error_Code)
        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/binary_sensor/{uid}_warning/config",
            {
                "name": "Warning",
                "unique_id": f"{uid}_warning",
                "object_id": f"{slug}_warning",
                "state_topic": f"{self._prefix}/{dsn}/attributes",
                "value_template": "{{ value_json.warning_code != 0 }}",
                "payload_on": True,
                "payload_off": False,
                "device_class": "problem",
                "entity_category": "diagnostic",
                "availability_topic": f"{self._prefix}/{dsn}/available",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device.device_info,
            },
            retain=True,
        )

        # Dock error code sensor
        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/sensor/{uid}_dock_error/config",
            {
                "name": "Dock Error Code",
                "unique_id": f"{uid}_dock_error",
                "object_id": f"{slug}_dock_error",
                "state_topic": f"{self._prefix}/{dsn}/attributes",
                "value_template": "{{ value_json.dock_error_code }}",
                "entity_category": "diagnostic",
                "icon": "mdi:home-alert-outline",
                "availability_topic": f"{self._prefix}/{dsn}/available",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device.device_info,
            },
            retain=True,
        )

        # Lifetime runtime sensor (unit unconfirmed upstream, exposed raw)
        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/sensor/{uid}_runtime/config",
            {
                "name": "Total Runtime",
                "unique_id": f"{uid}_runtime",
                "object_id": f"{slug}_runtime",
                "state_topic": f"{self._prefix}/{dsn}/attributes",
                "value_template": "{{ value_json.run_time_cumulative }}",
                "entity_category": "diagnostic",
                "icon": "mdi:history",
                "availability_topic": f"{self._prefix}/{dsn}/available",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device.device_info,
            },
            retain=True,
        )

        # Maintenance binary sensors
        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/binary_sensor/{uid}_replace_battery/config",
            {
                "name": "Replace Battery",
                "unique_id": f"{uid}_replace_battery",
                "object_id": f"{slug}_replace_battery",
                "state_topic": f"{self._prefix}/{dsn}/attributes",
                "value_template": "{{ value_json.replace_battery }}",
                "payload_on": True,
                "payload_off": False,
                "device_class": "problem",
                "entity_category": "diagnostic",
                "availability_topic": f"{self._prefix}/{dsn}/available",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device.device_info,
            },
            retain=True,
        )

        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/binary_sensor/{uid}_recommend_randr/config",
            {
                "name": "Recommend Rest And Recharge",
                "unique_id": f"{uid}_recommend_randr",
                "object_id": f"{slug}_recommend_randr",
                "state_topic": f"{self._prefix}/{dsn}/attributes",
                "value_template": "{{ value_json.recommend_rest_and_recharge }}",
                "payload_on": True,
                "payload_off": False,
                "device_class": "problem",
                "entity_category": "diagnostic",
                "availability_topic": f"{self._prefix}/{dsn}/available",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device.device_info,
            },
            retain=True,
        )

        # Device trigger for error events (fires in HA automation UI)
        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/device_automation/{uid}_error_trigger/config",
            {
                "automation_type": "trigger",
                "type": "action",
                "subtype": "error",
                "topic": f"{self._prefix}/{dsn}/error_event",
                "device": device.device_info,
            },
            retain=True,
        )

        # Image entity for map
        await self._publish(
            f"{HA_DISCOVERY_PREFIX}/image/{uid}_map/config",
            {
                "name": "Map",
                "unique_id": f"{uid}_map",
                "object_id": f"{slug}_map",
                "image_topic": f"{self._prefix}/{dsn}/map_image",
                "content_type": "image/png",
                "availability_topic": f"{self._prefix}/{dsn}/available",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device.device_info,
            },
            retain=True,
        )

        # Per-room clean buttons (only when room data is available)
        current_room_slugs: set[str] = set()
        if device.rooms:
            for room in device.rooms:
                room_slug = re.sub(r"[^a-z0-9]+", "_", room.lower()).strip("_")
                current_room_slugs.add(room_slug)
                await self._publish(
                    f"{HA_DISCOVERY_PREFIX}/button/{uid}_clean_{room_slug}/config",
                    {
                        "name": f"Clean {room}",
                        "unique_id": f"{uid}_clean_{room_slug}",
                        "object_id": f"{slug}_clean_{room_slug}",
                        "command_topic": f"{self._prefix}/{dsn}/clean_room",
                        "payload_press": json.dumps({"room": room}),
                        "icon": "mdi:robot-vacuum",
                        "availability_topic": f"{self._prefix}/{dsn}/available",
                        "payload_available": "online",
                        "payload_not_available": "offline",
                        "device": device.device_info,
                    },
                    retain=True,
                )

            # Clean mode select. Wet/dry-capable models (CleaningParameters
            # present) clean the same rooms in wet (hard floors) or dry
            # (whole room incl. carpet) mode, with a separate Deep button
            # for the two-stage dry-then-mop run. Other models use the
            # legacy Normal vs Matrix single-stage modes.
            if device.has_wet_dry:
                options = ["Wet", "Dry"]
                default_mode = "Dry"
            else:
                options = ["Normal", "Matrix"]
                default_mode = "Normal"
            await self._publish(
                f"{HA_DISCOVERY_PREFIX}/select/{uid}_clean_mode/config",
                {
                    "name": "Clean Mode",
                    "unique_id": f"{uid}_clean_mode",
                    "object_id": f"{slug}_clean_mode",
                    "command_topic": f"{self._prefix}/{dsn}/clean_mode",
                    "state_topic": f"{self._prefix}/{dsn}/clean_mode/state",
                    "options": options,
                    "icon": "mdi:broom",
                    "availability_topic": f"{self._prefix}/{dsn}/available",
                    "payload_available": "online",
                    "payload_not_available": "offline",
                    "device": device.device_info,
                },
                retain=True,
            )

            # Deep clean button (wet/dry models only): dry pass on carpet
            # areas, then wet pass on hard floors.
            if device.has_wet_dry:
                await self._publish(
                    f"{HA_DISCOVERY_PREFIX}/button/{uid}_deep/config",
                    {
                        "name": "Deep",
                        "unique_id": f"{uid}_deep",
                        "object_id": f"{slug}_deep",
                        "command_topic": f"{self._prefix}/{dsn}/send_command",
                        "payload_press": "vacuum_and_mop",
                        "icon": "mdi:water-pump",
                        "availability_topic": f"{self._prefix}/{dsn}/available",
                        "payload_available": "online",
                        "payload_not_available": "offline",
                        "device": device.device_info,
                    },
                    retain=True,
                )
            else:
                await self._publish(
                    f"{HA_DISCOVERY_PREFIX}/button/{uid}_deep/config",
                    "", retain=True,
                )

            # Publish current clean mode state
            mode = self._clean_modes.get(dsn, default_mode)
            if mode not in options:
                mode = default_mode
            await self._publish(
                f"{self._prefix}/{dsn}/clean_mode/state", mode, retain=True,
            )

        # Remove stale room buttons that no longer exist
        prev_rooms = self._published_rooms.get(dsn, set())
        stale_rooms = prev_rooms - current_room_slugs
        for room_slug in stale_rooms:
            await self._publish(
                f"{HA_DISCOVERY_PREFIX}/button/{uid}_clean_{room_slug}/config",
                "", retain=True,
            )
            logger.info("Removed stale room button %s for %s", room_slug, dsn)
        self._published_rooms[dsn] = current_room_slugs
        self._discovery_sigs[dsn] = sig

        logger.info("Published HA discovery for %s (%s)", device.product_name, dsn)

    # --- State publishing ---

    async def publish_state(
        self, device: SharkVacuum, prev_error: dict[str, int] | None = None,
    ) -> None:
        """Publish device state, attributes, and availability.

        If prev_error is provided, fire a device trigger event when a NEW
        error is detected (error_code transitions from 0 to non-zero).
        """
        dsn = device.dsn
        available = "online"

        state_payload = device.to_state_payload()
        # When docked, the device reports eco — use the user's last-set speed instead
        if device.is_docked and dsn in self._fan_speed_overrides:
            state_payload["fan_speed"] = self._fan_speed_overrides[dsn]

        attributes_payload = device.to_attributes_payload()
        # Same dock-reports-eco quirk applies to water_flow (mop flow level),
        # but only reinstate the override on models that report the property
        # at all — otherwise it reintroduces water_flow on vac-only models.
        if device.has_flow_mode and device.is_docked and dsn in self._water_flow_overrides:
            attributes_payload["water_flow"] = self._water_flow_overrides[dsn]

        await self._publish(f"{self._prefix}/{dsn}/state", state_payload, retain=True)
        await self._publish(f"{self._prefix}/{dsn}/attributes", attributes_payload, retain=True)
        await self._publish(f"{self._prefix}/{dsn}/available", available, retain=True)

        # Fire error event if error_code changed to non-zero
        if prev_error is not None and device.error_code != 0:
            old_code = prev_error.get(dsn, 0)
            if old_code != device.error_code:
                await self._publish(
                    f"{self._prefix}/{dsn}/error_event",
                    {
                        "error_code": device.error_code,
                        "error_text": device.error_text,
                        "device_name": device.product_name,
                    },
                )
                logger.warning(
                    "Error on %s: %s (code %d)",
                    device.product_name, device.error_text, device.error_code,
                )

    async def publish_unavailable(self, devices: list[SharkVacuum]) -> None:
        """Mark all devices as unavailable."""
        for device in devices:
            await self._publish(f"{self._prefix}/{device.dsn}/available", "offline", retain=True)

    async def publish_status(self, status: dict[str, Any]) -> None:
        """Publish auth/system status."""
        await self._publish(f"{self._prefix}/status", status, retain=True)

    async def publish_map_image(
        self,
        device: SharkVacuum,
        png: bytes,
    ) -> None:
        """Publish floor map as a PNG image to Home Assistant.

        Args:
            device: The SharkVacuum device
            png: Raw PNG bytes
        """
        dsn = device.dsn

        # Publish raw PNG bytes to image topic
        image_topic = f"{self._prefix}/{dsn}/map_image"
        await self._client.publish(image_topic, png, qos=1, retain=True)
        logger.info("Published map image for %s (%d bytes)", dsn, len(png))

    # --- Command handling ---

    async def command_listener(
        self,
        command_handler: Any,
        devices: dict[str, SharkVacuum],
        command_event: asyncio.Event | None = None,
    ) -> None:
        """Subscribe to command topics and dispatch via handler.

        command_handler must implement:
          send_command(device_id, command) -> None
          set_fan_speed(device_id, speed) -> None
        """
        assert self._client is not None

        await self._client.subscribe(f"{self._prefix}/+/command")
        await self._client.subscribe(f"{self._prefix}/+/set_fan_speed")
        await self._client.subscribe(f"{self._prefix}/+/set_water_flow")
        await self._client.subscribe(f"{self._prefix}/+/send_command")
        await self._client.subscribe(f"{self._prefix}/+/clean_room")
        await self._client.subscribe(f"{self._prefix}/+/clean_mode")

        async for message in self._client.messages:
            topic = message.topic.value
            payload = message.payload.decode() if isinstance(message.payload, bytes) else str(message.payload)
            device_id = self._extract_dsn(topic)

            if not device_id:
                continue

            if device_id not in devices:
                logger.warning("Command for unknown device: %s", device_id)
                continue

            try:
                if topic.endswith("/command"):
                    command = payload.strip().lower()
                    logger.info("Command received: %s for %s", command, device_id)
                    await command_handler.send_command(device_id, command)
                elif topic.endswith("/set_fan_speed"):
                    speed = payload.strip().lower()
                    logger.info("Fan speed received: %s for %s", speed, device_id)
                    self._fan_speed_overrides[device_id] = speed
                    await command_handler.set_fan_speed(device_id, speed)
                elif topic.endswith("/set_water_flow"):
                    level = payload.strip().lower()
                    logger.info("Water flow level received: %s for %s", level, device_id)
                    self._water_flow_overrides[device_id] = level
                    await command_handler.set_water_flow(device_id, level)
                elif topic.endswith("/send_command"):
                    logger.info("send_command received for %s", device_id)
                    await self._handle_send_command(
                        command_handler, device_id, payload, devices,
                    )
                elif topic.endswith("/clean_room"):
                    logger.info("clean_room button pressed for %s", device_id)
                    await self._handle_clean_room(
                        command_handler, device_id, payload, devices,
                    )
                elif topic.endswith("/clean_mode"):
                    mode = payload.strip()
                    valid_modes = ("Normal", "Matrix", "Wet", "Dry")
                    if mode in valid_modes:
                        self._clean_modes[device_id] = mode
                        await self._publish(
                            f"{self._prefix}/{device_id}/clean_mode/state",
                            mode, retain=True,
                        )
                        logger.info("Clean mode set to %s for %s", mode, device_id)
                    else:
                        logger.warning("Unknown clean mode: %s", mode)
                if command_event is not None:
                    command_event.set()
            except Exception:
                logger.exception("Failed to handle command on %s", topic)

    async def _handle_clean_room(
        self, handler: Any, device_id: str, payload: str,
        devices: dict[str, Any],
    ) -> None:
        """Handle room button press — dispatches clean_rooms with current mode."""
        data = json.loads(payload)
        room = data.get("room", "")
        if not room:
            logger.warning("clean_room button: no room in payload: %r", payload)
            return

        device = devices.get(device_id)
        floor_id = ""
        if device and hasattr(device, "floor_id"):
            floor_id = device.floor_id
        if not floor_id:
            logger.warning("clean_room button: no floor_id for %s", device_id)
            return

        wet_dry = bool(device and getattr(device, "has_wet_dry", False))
        if wet_dry:
            # Wet/dry models: the select drives clean_type, and room runs
            # are always single-stage UserRoom (Deep is its own button).
            mode = self._clean_modes.get(device_id)
            if mode not in ("Wet", "Dry"):
                mode = "Dry"
            clean_type = "wet" if mode == "Wet" else "dry"
            api_mode, clean_count = "UserRoom", 1
        else:
            mode = self._clean_modes.get(device_id, "Normal")
            if mode not in ("Normal", "Matrix"):
                mode = "Normal"
            clean_type = "dry"
            if mode == "Matrix":
                api_mode, clean_count = "UltraClean", 2
            else:
                api_mode, clean_count = "UserRoom", 1

        use_v3 = getattr(device, "has_areas_v3", False)
        api_rooms = (
            device.to_robot_room_names([room])
            if device and hasattr(device, "to_robot_room_names")
            else [room]
        )
        await handler.clean_rooms(
            device_id, rooms=api_rooms, floor_id=floor_id,
            clean_type=clean_type, clean_count=clean_count, mode=api_mode,
            use_v3=use_v3,
        )
        logger.info(
            "Room clean started: %s (api=%s) on %s (mode=%s)",
            room, api_rooms, device_id, mode,
        )

    @staticmethod
    async def _handle_send_command(
        handler: Any, device_id: str, payload: str,
        devices: dict[str, Any],
    ) -> None:
        """Handle vacuum.send_command from HA.

        HA publishes JSON: {"command": "...", "params": {...}}

        Supported commands:
          clean_room:    {room: "Kitchen"}
          matrix_clean:  {room: "Kitchen"}
          clean_rooms:   {rooms: ["Kitchen", "Den"], mode: "UserRoom",
                          clean_count: 1, clean_type: "dry"}
        """
        import json as _json
        # HA may publish the command as raw JSON ({"command": "..."}) or as a
        # plain command string (e.g. "vacuum_and_mop"). Accept both.
        try:
            data = _json.loads(payload)
        except (_json.JSONDecodeError, TypeError):
            data = {"command": payload.strip(), "params": {}}
        if not isinstance(data, dict):
            data = {"command": str(data).strip(), "params": {}}
        logger.debug("send_command raw data: %r", data)
        command = data.get("command", "")
        params = data.get("params", data.get("param", {}))
        # HA may send params as a JSON string — unwrap it
        if isinstance(params, str):
            try:
                params = _json.loads(params)
            except (ValueError, TypeError):
                logger.warning("send_command params not valid JSON: %r", params)
                params = {}
        if not isinstance(params, dict):
            params = {}
        # HA puts service data keys at top level (not nested under "params")
        # so merge any top-level keys (except "command") as fallback
        for key, val in data.items():
            if key not in ("command", "params", "param") and key not in params:
                params[key] = val

        # Get device attributes
        device = devices.get(device_id)
        use_v3 = getattr(device, "has_areas_v3", False)

        def get_floor_id() -> str:
            fid = params.get("floor_id", "")
            if not fid and device and hasattr(device, "floor_id"):
                fid = device.floor_id
            return fid

        def to_api_rooms(display_rooms: list[str]) -> list[str]:
            if device and hasattr(device, "to_robot_room_names"):
                return device.to_robot_room_names(display_rooms)
            return list(display_rooms)

        if command == "clean_room":
            room = params.get("room", "")
            if not room:
                logger.warning("clean_room requires 'room' in params")
                return
            floor_id = get_floor_id()
            if not floor_id:
                logger.warning("clean_room: no floor_id available")
                return
            await handler.clean_rooms(
                device_id, rooms=to_api_rooms([room]), floor_id=floor_id,
                clean_type=params.get("clean_type", "dry"),
                clean_count=1, mode="UserRoom", use_v3=use_v3,
            )

        elif command == "matrix_clean":
            room = params.get("room", "")
            if not room:
                logger.warning("matrix_clean requires 'room' in params")
                return
            floor_id = get_floor_id()
            if not floor_id:
                logger.warning("matrix_clean: no floor_id available")
                return
            await handler.clean_rooms(
                device_id, rooms=to_api_rooms([room]), floor_id=floor_id,
                clean_type=params.get("clean_type", "dry"),
                clean_count=2, mode="UltraClean", use_v3=use_v3,
            )

        elif command == "clean_rooms":
            rooms = params.get("rooms", [])
            if not rooms:
                logger.warning("clean_rooms requires 'rooms' in params")
                return
            floor_id = get_floor_id()
            if not floor_id:
                logger.warning("clean_rooms: no floor_id available")
                return
            await handler.clean_rooms(
                device_id, rooms=to_api_rooms(rooms), floor_id=floor_id,
                clean_type=params.get("clean_type", "dry"),
                clean_count=params.get("clean_count", 1),
                mode=params.get("mode", "UserRoom"), use_v3=use_v3,
            )

        else:
            logger.info("Forwarding send_command '%s' as generic command", command)
            await handler.send_command(device_id, command)

    def _extract_dsn(self, topic: str) -> str | None:
        """Extract DSN from topic like 'shark2mqtt/{dsn}/command'."""
        parts = topic.split("/")
        if len(parts) >= 3 and parts[0] == self._prefix:
            return parts[1]
        return None
