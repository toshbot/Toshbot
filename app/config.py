from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    environment: str = "development"
    api_key: str = ""
    cors_origins: str = "http://localhost:3000"
    log_level: str = "INFO"

    storage_backend: str = "firestore"  # "firestore" | "memory"
    firebase_project_id: str = ""
    google_application_credentials: str = ""
    firestore_collection_prefix: str = ""

    pumpportal_ws_url: str = "wss://pumpportal.fun/api/data"
    listener_enabled: bool = True
    reconnect_base_seconds: float = 1.0
    reconnect_max_seconds: float = 60.0
    stable_connection_seconds: float = 30.0
    feed_stale_after_seconds: float = 120.0
    queue_maxsize: int = 1000
    processor_max_attempts: int = 3

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]
