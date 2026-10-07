"""Configuration via environment variables."""

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Shark account
    shark_username: str
    shark_password: str
    shark_region: str = Field(default="us", pattern=r"^(us|eu)$")

    # MQTT broker
    mqtt_host: str
    mqtt_port: int = 1883
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    mqtt_prefix: str = "shark2mqtt"

    # SharkNinja cloud (skegox)
    shark_household_id: str | None = None

    # Polling
    poll_interval: int = 300
    poll_interval_active: int = 20

    # Token persistence
    token_dir: str = "/data"

    # Logging
    log_level: str = "INFO"

    # Map image layer visibility (all default on = current rendering).
    # Mirrors the roborock integration's map drawable options, limited to
    # the layers Shark's Visual_Floor_1 .bin actually contains (there is no
    # charger/dock geometry in the file).
    map_show_background: bool = True
    map_show_rooms: bool = True
    map_show_obstacles: bool = True
    map_show_robot: bool = True

    # Operation modes
    auth_once: bool = False
    offline: bool = False

    model_config = {"env_file": ".env", "env_prefix": "", "case_sensitive": False}
