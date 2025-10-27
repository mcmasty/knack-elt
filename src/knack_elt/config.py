from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    motherduck_api_key: str = os.environ.get('motherduck_api_key', '')

settings = Settings()