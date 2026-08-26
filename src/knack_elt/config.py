import os

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Lowercase deliberately: this is the documented variable name and the one
    # existing deployments set. os.environ is case-sensitive, so capitalising it
    # would be a breaking config change.
    motherduck_api_key: str = os.environ.get('motherduck_api_key', '')  # noqa: SIM112
    knack_app_id: str = Field(default='', alias='KNACK_APP_ID')
    knack_api_key: str = Field(default='', alias='KNACK_API_KEY')
    
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
    )
    
settings = Settings()


