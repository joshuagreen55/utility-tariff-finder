from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/utility_tariff_finder"
    sync_database_url: str = "postgresql://postgres:postgres@localhost:5432/utility_tariff_finder"
    redis_url: str = "redis://localhost:6379/0"
    openei_api_key: str = ""
    geocoder_provider: str = "census"
    google_maps_api_key: str = ""
    # Comma-separated origins for browser apps (local dev + production domain)
    cors_origins: str = "http://localhost:5173,http://localhost:3000"
    # If set, /api/admin/* requires X-Admin-Key or Authorization: Bearer <key>
    admin_api_key: str = ""

    # --- Pipeline API keys ---
    brave_api_key: str = ""
    anthropic_api_key: str = ""
    google_ai_api_key: str = ""
    google_cse_api_key: str = ""
    google_cse_cx: str = ""

    # --- Google sign-in gate (optional; when AUTH_ENABLED=true, most /api routes need a session) ---
    auth_enabled: bool = False
    google_oauth_client_id: str = ""
    google_oauth_client_secret: str = ""
    # Must match an authorized redirect URI in Google Cloud Console
    auth_google_redirect_uri: str = ""
    auth_allowed_email_domain: str = "getmysa.com"
    # Random secret for signing session cookies (required when AUTH_ENABLED=true)
    auth_session_secret: str = ""
    auth_session_max_days: int = 7
    # After login redirect (e.g. http://34.63.25.32). If empty, derived from AUTH_GOOGLE_REDIRECT_URI.
    public_app_url: str = ""
    # Use true when serving https:// only
    auth_cookie_secure: bool = False
    auth_cookie_name: str = "utf_session"
    oauth_state_cookie_name: str = "utf_oauth_state"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


settings = Settings()
