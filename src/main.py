"""shark2mqtt — Shark vacuum to MQTT bridge for Home Assistant."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from typing import Any

from .ayla_api import AylaApi
from .config import Settings
from .exc import SharkAuthError
from .mqtt_client import MqttClient
from .shark_auth import SharkAuth
from .shark_device import SharkVacuum
from .skegox_api import SkegoxApi

logger = logging.getLogger("shark2mqtt")


class CommandRouter:
    """Routes commands to the correct API based on per-device api_backend."""

    def __init__(
        self,
        skegox: SkegoxApi,
        ayla: AylaApi,
        devices: dict[str, SharkVacuum],
    ) -> None:
        self._skegox = skegox
        self._ayla = ayla
        self._devices = devices

    def _api_for(self, device_id: str) -> Any:
        device = self._devices.get(device_id)
        if device and device.api_backend == "ayla":
            return self._ayla
        return self._skegox

    async def send_command(self, device_id: str, command: str) -> None:
        await self._api_for(device_id).send_command(device_id, command)

    async def set_fan_speed(self, device_id: str, speed: str) -> None:
        await self._api_for(device_id).set_fan_speed(device_id, speed)

    async def clean_rooms(
        self,
        device_id: str,
        rooms: list[str],
        floor_id: str,
        clean_type: str = "dry",
        clean_count: int = 1,
        mode: str = "UserRoom",
        use_v3: bool = False,
    ) -> None:
        await self._api_for(device_id).clean_rooms(
            device_id, rooms, floor_id, clean_type, clean_count, mode, use_v3,
        )


async def poll_loop(
    api: SkegoxApi,
    ayla_api: AylaApi,
    mqtt: MqttClient,
    auth: SharkAuth,
    config: Settings,
    devices_map: dict[str, SharkVacuum],
    ayla_room_data: dict[str, tuple[str, list[str]]],
    ayla_name_maps: dict[str, dict[str, str]],
    command_event: asyncio.Event,
) -> None:
    """Periodically poll device state and publish to MQTT."""
    from .ayla_api import _apply_room_name_map

    prev_errors: dict[str, int] = {}
    first_poll = True

    while True:
        any_active = False
        try:
            await auth.ensure_authenticated()

            # --- Skegox devices (primary) ---
            skegox_snds: set[str] = set()
            raw_devices = await api.get_all_devices()
            for raw in raw_devices:
                device = SharkVacuum.from_skegox(raw)
                if not device.rooms and device.dsn in ayla_room_data:
                    device.floor_id, device.rooms = ayla_room_data[device.dsn]
                name_map = ayla_name_maps.get(device.dsn, {})
                if name_map and device.rooms:
                    device.room_name_map = name_map
                    renamed = _apply_room_name_map(device.rooms, name_map)
                    if renamed != device.rooms and first_poll:
                        logger.info(
                            "Renamed skegox rooms for %s (%s): %s -> %s",
                            device.product_name, device.dsn,
                            device.rooms, renamed,
                        )
                    device.rooms = renamed
                skegox_snds.add(device.dsn)
                devices_map[device.dsn] = device
                await mqtt.publish_discovery(device)
                await mqtt.publish_state(device, prev_error=prev_errors)
                prev_errors[device.dsn] = device.error_code
                if device.ha_state != "docked":
                    any_active = True

            # --- Ayla fallback (only when skegox has no devices) ---
            # If a user reports missing devices, check whether skegox
            # returned some devices but not all — that would indicate
            # a mixed migration we don't currently handle.
            if first_poll:
                skegox_names = [d.product_name for d in devices_map.values()]
                logger.info(
                    "Skegox returned %d device(s): %s",
                    len(skegox_snds), skegox_names or "(none)",
                )
            if skegox_snds:
                if first_poll:
                    logger.info("Using skegox API, skipping Ayla")
            else:
                try:
                    ayla_devices = await ayla_api.get_devices()
                    if first_poll:
                        ayla_names = [d.product_name for d in ayla_devices]
                        logger.info(
                            "Falling back to Ayla, found %d device(s): %s",
                            len(ayla_devices), ayla_names or "(none)",
                        )
                    for device in ayla_devices:
                        devices_map[device.dsn] = device
                        await mqtt.publish_discovery(device)
                        await mqtt.publish_state(device, prev_error=prev_errors)
                        prev_errors[device.dsn] = device.error_code
                        if device.ha_state != "docked":
                            any_active = True
                except Exception:
                    logger.warning("Ayla device fetch failed", exc_info=True)

            first_poll = False

        except SharkAuthError as e:
            logger.error("Auth error during poll: %s", e)
            await mqtt.publish_status({"state": "auth_error", "message": str(e)})
            await mqtt.publish_unavailable(list(devices_map.values()))
        except Exception:
            logger.exception("Poll cycle failed")
            await mqtt.publish_unavailable(list(devices_map.values()))

        interval = config.poll_interval_active if any_active else config.poll_interval
        try:
            await asyncio.wait_for(command_event.wait(), timeout=interval)
            command_event.clear()
            logger.debug("Poll triggered early by command, waiting for device to update")
            await asyncio.sleep(5)
        except TimeoutError:
            pass


async def run(config: Settings) -> None:
    """Main run loop."""
    auth = SharkAuth(config)
    mqtt = MqttClient(config)

    # --auth-once: authenticate, save tokens, exit
    if config.auth_once:
        logger.info("Running in --auth-once mode")
        await auth.ensure_authenticated()
        if auth.id_token:
            api = SkegoxApi(config, auth)
            ayla_api = AylaApi(config, auth)
            if config.shark_household_id:
                api.set_household(config.shark_household_id)
            skegox_devices = await api.get_all_devices()

            all_devices: list[SharkVacuum] = []
            for d in skegox_devices:
                all_devices.append(SharkVacuum.from_skegox(d))

            # Fall back to Ayla only when skegox has no devices
            if not all_devices:
                all_devices = await ayla_api.get_devices()

            logger.info(
                "Auth successful. Found %d device(s). Tokens saved.",
                len(all_devices),
            )
            for v in all_devices:
                logger.info(
                    "  %s (%s) [%s]: battery=%d%%",
                    v.product_name, v.dsn, v.api_backend, v.battery_level,
                )
            await api.close()
            await ayla_api.close()
        else:
            logger.error("Authentication failed — no id_token obtained")
        return

    api = SkegoxApi(config, auth)
    ayla_api = AylaApi(config, auth)
    if config.shark_household_id:
        api.set_household(config.shark_household_id)

    # Shared mutable device map for command handler
    devices_map: dict[str, SharkVacuum] = {}

    # Set up graceful shutdown
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    router = CommandRouter(api, ayla_api, devices_map)

    try:
        await auth.ensure_authenticated()

        # Prefetch Ayla room data once at startup as a fallback for
        # skegox devices whose Robot_Room_List shadow is empty (rooms
        # configured before the skegox migration), and collect room-name
        # maps from Mobile_App_Room_Definition for accounts where the
        # display names are stored as user_room_name (issue #4).
        ayla_room_data: dict[str, tuple[str, list[str]]] = {}
        ayla_name_maps: dict[str, dict[str, str]] = {}
        try:
            ayla_vacuums = await ayla_api.get_devices()
            for v in ayla_vacuums:
                bsn = v._properties.get("GET_Battery_Serial_Num", "")
                snd = bsn.split("-")[-1] if "-" in bsn else ""
                if snd and v.rooms:
                    ayla_room_data[snd] = (v.floor_id, v.rooms)
                    logger.info("Ayla room fallback for %s: %s", snd, v.rooms)
                if snd and v.room_name_map:
                    ayla_name_maps[snd] = v.room_name_map
        except Exception:
            logger.warning("Failed to prefetch Ayla room data", exc_info=True)

        async with mqtt:
            await mqtt.publish_status({"state": "online"})

            command_event = asyncio.Event()

            async with asyncio.TaskGroup() as tg:
                tg.create_task(poll_loop(api, ayla_api, mqtt, auth, config, devices_map, ayla_room_data, ayla_name_maps, command_event))
                tg.create_task(mqtt.command_listener(router, devices_map, command_event))

                async def _shutdown_watcher() -> None:
                    await stop_event.wait()
                    logger.info("Shutdown signal received")
                    raise SystemExit(0)

                tg.create_task(_shutdown_watcher())

    except (SystemExit, KeyboardInterrupt):
        logger.info("Shutting down gracefully")
    finally:
        await api.close()
        await ayla_api.close()


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="shark2mqtt — Shark vacuum to MQTT bridge"
    )
    parser.add_argument(
        "--auth-once",
        action="store_true",
        help="Authenticate once, save tokens, and exit",
    )
    args = parser.parse_args()

    config = Settings()  # type: ignore[call-arg]

    if args.auth_once:
        config.auth_once = True

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("shark2mqtt starting")
    asyncio.run(run(config))


if __name__ == "__main__":
    main()
