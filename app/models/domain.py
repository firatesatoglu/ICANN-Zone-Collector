from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime


class DomainBase(BaseModel):
    """Base domain model."""
    domain: str = Field(..., description="Domain name without TLD")
    fqdn: str = Field(..., description="Fully qualified domain name")
    tld: str = Field(..., description="Top level domain")


class DomainInDB(DomainBase):
    """Domain model as stored in database."""
    first_seen: datetime = Field(..., description="First time domain was seen")
    last_seen: datetime = Field(..., description="Last time domain was seen")
    whois: Optional[Dict[str, Any]] = Field(None, description="WHOIS information")
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DomainResponse(DomainInDB):
    """Domain response model."""
    pass


class TLDStats(BaseModel):
    """TLD statistics model."""
    tld: str
    collection: str
    total_domains: int
    earliest_first_seen: Optional[datetime] = None
    latest_first_seen: Optional[datetime] = None
    latest_last_seen: Optional[datetime] = None


class SyncStatus(BaseModel):
    """Sync operation status."""
    sync_id: Optional[str] = None
    status: str
    message: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    tlds_processed: int = 0
    total_domains_processed: int = 0
    errors: List[str] = Field(default_factory=list)


class SyncRequest(BaseModel):
    """Manual sync request."""
    tlds: Optional[List[str]] = Field(
        None, 
        description="List of specific TLDs to sync. Empty means all approved TLDs."
    )


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    mongodb_connected: bool
    icann_authenticated: bool
    scheduler_running: bool
    last_sync: Optional[datetime] = None
    next_sync: Optional[datetime] = None
