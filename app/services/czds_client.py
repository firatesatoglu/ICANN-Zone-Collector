import httpx
import os
import gzip
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
import logging

from app.config import settings

logger = logging.getLogger(__name__)


class CZDSClient:
    """ICANN CZDS API Client for downloading zone files."""
    
    def __init__(self):
        self.username = settings.icann_username
        self.password = settings.icann_password
        self.auth_url = settings.icann_auth_url
        self.czds_url = settings.icann_czds_url
        self.access_token: Optional[str] = None
        self.zone_files_dir = Path(settings.zone_files_dir)
        
        self.zone_files_dir.mkdir(parents=True, exist_ok=True)
    
    async def authenticate(self) -> bool:
        """Authenticate with ICANN and get access token."""
        auth_endpoint = f"{self.auth_url}/api/authenticate"
        
        payload = {
            "username": self.username,
            "password": self.password
        }
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    auth_endpoint, 
                    json=payload, 
                    headers=headers
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self.access_token = data.get("accessToken")
                    logger.info(f"Successfully authenticated as {self.username}")
                    return True
                else:
                    logger.error(
                        f"Authentication failed: {response.status_code} - {response.text}"
                    )
                    return False
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return False
    
    async def get_zone_links(self) -> List[str]:
        """Get list of available zone file download links."""
        if not self.access_token:
            if not await self.authenticate():
                return []
        
        links_url = f"{self.czds_url}/czds/downloads/links"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(links_url, headers=headers)
                
                if response.status_code == 200:
                    zone_links = response.json()
                    logger.info(f"Found {len(zone_links)} zone files available")
                    return zone_links
                elif response.status_code == 401:
                    logger.warning("Access token expired, re-authenticating...")
                    if await self.authenticate():
                        return await self.get_zone_links()
                    return []
                else:
                    logger.error(
                        f"Failed to get zone links: {response.status_code} - {response.text}"
                    )
                    return []
        except Exception as e:
            logger.error(f"Error getting zone links: {str(e)}")
            return []
    
    def extract_tld_from_url(self, url: str) -> str:
        """Extract TLD name from zone download URL."""
        filename = url.rsplit("/", 1)[-1]
        tld = filename.replace(".zone", "")
        return tld
    
    async def download_zone_file(
        self, 
        url: str, 
        tlds_filter: Optional[List[str]] = None
    ) -> Optional[Path]:
        """
        Download a single zone file.
        Returns the path to the downloaded file.
        """
        tld = self.extract_tld_from_url(url)
        
        if tlds_filter and tld not in tlds_filter:
            logger.debug(f"Skipping {tld} - not in filter list")
            return None
        
        if not self.access_token:
            if not await self.authenticate():
                return None
        
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }
        
        try:
            async with httpx.AsyncClient(timeout=300.0) as client:
                logger.info(f"Downloading zone file for {tld}...")
                
                response = await client.get(url, headers=headers, follow_redirects=True)
                
                if response.status_code == 200:
                    content_disposition = response.headers.get("content-disposition", "")
                    if "filename=" in content_disposition:
                        filename = content_disposition.split("filename=")[1].strip('"')
                    else:
                        filename = f"{tld}.txt.gz"
                    
                    file_path = self.zone_files_dir / filename
                    
                    with open(file_path, "wb") as f:
                        f.write(response.content)
                    
                    logger.info(f"Downloaded {tld} zone file to {file_path}")
                    return file_path
                    
                elif response.status_code == 401:
                    logger.warning("Access token expired during download, re-authenticating...")
                    if await self.authenticate():
                        return await self.download_zone_file(url, tlds_filter)
                    return None
                elif response.status_code == 404:
                    logger.warning(f"Zone file not found for {tld}")
                    return None
                else:
                    logger.error(
                        f"Failed to download {tld}: {response.status_code}"
                    )
                    return None
                    
        except Exception as e:
            logger.error(f"Error downloading {tld}: {str(e)}")
            return None
    
    async def download_all_zones(
        self, 
        tlds_filter: Optional[List[str]] = None
    ) -> Dict[str, Path]:
        """
        Download all available zone files.
        Returns dict mapping TLD to file path.
        """
        zone_links = await self.get_zone_links()
        
        if not zone_links:
            logger.warning("No zone files available to download")
            return {}
        
        downloaded = {}
        
        for url in zone_links:
            tld = self.extract_tld_from_url(url)
            
            if tlds_filter and tld not in tlds_filter:
                continue
            
            file_path = await self.download_zone_file(url, tlds_filter)
            
            if file_path:
                downloaded[tld] = file_path
        
        logger.info(f"Downloaded {len(downloaded)} zone files")
        return downloaded


czds_client = CZDSClient()
