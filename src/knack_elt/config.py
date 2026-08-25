from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
import os


class Settings(BaseSettings):
    motherduck_api_key: str = os.environ.get('motherduck_api_key', '')
    knack_app_id: str = Field(default='', alias='KNACK_APP_ID')
    knack_api_key: str = Field(default='', alias='KNACK_API_KEY')
    
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
    )
    
settings = Settings()


