import whois
from typing import Optional, Dict, Any
from datetime import datetime
import asyncio
import logging

from app.config import settings

logger = logging.getLogger(__name__)


class WhoisService:
    """
    WHOIS lookup service with rate limiting.
    Uses python-whois library.
    """
    
    def __init__(self):
        self.enabled = settings.whois_enabled
        self.rate_limit = settings.whois_rate_limit
        self._last_query_time: Optional[float] = None
    
    async def _rate_limit_wait(self):
        """Wait if necessary to respect rate limit."""
        if self._last_query_time:
            elapsed = asyncio.get_event_loop().time() - self._last_query_time
            wait_time = (1.0 / self.rate_limit) - elapsed
            if wait_time > 0:
                await asyncio.sleep(wait_time)
        self._last_query_time = asyncio.get_event_loop().time()
    
    def _parse_date(self, date_val) -> Optional[datetime]:
        """Parse date value from WHOIS response."""
        if date_val is None:
            return None
        if isinstance(date_val, list):
            date_val = date_val[0] if date_val else None
        if isinstance(date_val, datetime):
            return date_val
        return None
    
    async def lookup(self, domain: str) -> Optional[Dict[str, Any]]:
        """
        Perform WHOIS lookup for a domain.
        Returns parsed WHOIS data or None on error.
        """
        if not self.enabled:
            return None
        
        await self._rate_limit_wait()
        
        try:
            loop = asyncio.get_event_loop()
            w = await loop.run_in_executor(None, whois.whois, domain)
            
            if w is None:
                return None
            
            return {
                "registrar": w.registrar,
                "creation_date": self._parse_date(w.creation_date),
                "expiration_date": self._parse_date(w.expiration_date),
                "updated_date": self._parse_date(w.updated_date),
                "name_servers": w.name_servers if isinstance(w.name_servers, list) else [w.name_servers] if w.name_servers else [],
                "status": w.status if isinstance(w.status, list) else [w.status] if w.status else [],
                "emails": w.emails if isinstance(w.emails, list) else [w.emails] if w.emails else [],
                "org": w.org,
                "country": w.country,
                "raw": str(w.text) if hasattr(w, 'text') else None
            }
            
        except Exception as e:
            logger.debug(f"WHOIS lookup failed for {domain}: {str(e)}")
            return None


whois_service = WhoisService()
