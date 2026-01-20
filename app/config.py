from pydantic_settings import BaseSettings
from typing import List
import os


class Settings(BaseSettings):
    mongodb_url: str = "mongodb://localhost:27017/"
    database_name: str = "icann_tlds_db"
    
    icann_username: str = ""
    icann_password: str = ""
    icann_auth_url: str = "https://account-api.icann.org"
    icann_czds_url: str = "https://czds-api.icann.org"
    
    schedule_hours: str = "0,12"
    
    zone_files_dir: str = "/app/zonefiles"
    
    max_concurrent_downloads: int = 10
    upsert_batch_size: int = 5000
    
    whois_enabled: bool = False
    whois_rate_limit: int = 5
    
    log_level: str = "INFO"
    
    @property
    def schedule_hours_list(self) -> List[int]:
        """Parse schedule hours string to list of integers."""
        return [int(h.strip()) for h in self.schedule_hours.split(",")]
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
