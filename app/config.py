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

    # Solana RPC (use a dedicated provider URL, e.g. Helius, for real use; keep the key only in env vars)
    solana_rpc_url: str = "https://api.mainnet-beta.solana.com"
    rpc_timeout_seconds: float = 10.0
    rpc_max_concurrency: int = 4

    # Analysis workers
    analysis_enabled: bool = True
    analysis_workers: int = 2
    analysis_queue_maxsize: int = 2000
    analysis_retry_delays: str = "5,20,60"  # seconds between re-tries while evidence is unavailable

    # Security thresholds. EXPERIMENTAL starting values, not calibrated. Authority/extension rules are NOT configurable.
    evidence_max_age_seconds: int = 900
    single_holder_reject_pct: float = 30.0
    top10_hold_pct: float = 40.0
    creator_hold_pct: float = 15.0
    max_transfer_fee_bps: int = 500
    creator_max_launches_24h: int = 5
    min_curve_reserve_sol: float = 0.1

    @property
    def retry_delay_list(self) -> list[float]:
        return [float(x) for x in self.analysis_retry_delays.split(",") if x.strip()]

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]
