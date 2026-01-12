from datetime import datetime
from typing import Optional, List, Dict
from pathlib import Path
import logging
import asyncio
import uuid

from app.services.czds_client import czds_client
from app.services.zone_parser import parse_zone_file
from app.database.mongodb import mongodb
from app.config import settings
from app.models.domain import SyncStatus

logger = logging.getLogger(__name__)


class SyncService:
    """Service to orchestrate zone file sync operations."""
    
    def __init__(self):
        self.last_sync: Optional[datetime] = None
        self.is_syncing: bool = False
        self.current_sync_id: Optional[str] = None
        self.sync_history: Dict[str, SyncStatus] = {}
    
    def start_sync(self, tlds_filter: Optional[List[str]] = None) -> str:
        """
        Start a sync operation and return sync_id immediately.
        The actual sync runs in background.
        """
        if self.is_syncing:
            raise ValueError("Sync already in progress")
        
        sync_id = str(uuid.uuid4())[:8]
        self.current_sync_id = sync_id
        self.is_syncing = True
        
        status = SyncStatus(
            sync_id=sync_id,
            status="running",
            message="Starting sync...",
            started_at=datetime.utcnow()
        )
        self.sync_history[sync_id] = status
        
        # Start async task
        asyncio.create_task(self._run_sync(sync_id, tlds_filter))
        
        return sync_id
    
    async def _run_sync(
        self, 
        sync_id: str,
        tlds_filter: Optional[List[str]] = None
    ):
        """
        Internal method to perform the actual sync.
        """
        status = self.sync_history[sync_id]
        
        try:
            # Authenticate with ICANN
            status.message = "Authenticating with ICANN..."
            if not await czds_client.authenticate():
                status.status = "error"
                status.message = "Failed to authenticate with ICANN"
                return
            
            # Get zone links
            status.message = "Getting zone file links..."
            zone_links = await czds_client.get_zone_links()
            
            if not zone_links:
                status.status = "error"
                status.message = "No zone files available"
                return
            
            logger.info(f"[{sync_id}] Found {len(zone_links)} zone files to process")
            
            total_domains = 0
            processed_tlds = 0
            
            for url in zone_links:
                tld = czds_client.extract_tld_from_url(url)
                
                # Apply filter if provided
                if tlds_filter and tld not in tlds_filter:
                    continue
                
                try:
                    status.message = f"Downloading {tld}..."
                    
                    # Download zone file
                    file_path = await czds_client.download_zone_file(url)
                    
                    if not file_path:
                        status.errors.append(f"Failed to download {tld}")
                        continue
                    
                    status.message = f"Parsing {tld}..."
                    
                    # Parse zone file
                    parsed_tld, domains = parse_zone_file(file_path)
                    
                    if not domains:
                        logger.warning(f"[{sync_id}] No domains found in {tld}")
                        continue
                    
                    status.message = f"Storing {len(domains):,} domains for {tld}..."
                    
                    # Ensure indexes
                    await mongodb.ensure_indexes(tld)
                    
                    # Store in MongoDB
                    result = await mongodb.upsert_domains(
                        tld=tld,
                        domains=domains,
                        zone_file_date=datetime.utcnow()
                    )
                    
                    total_domains += len(domains)
                    processed_tlds += 1
                    status.tlds_processed = processed_tlds
                    status.total_domains_processed = total_domains
                    
                    logger.info(
                        f"[{sync_id}] Processed {tld}: {result['inserted']} new, "
                        f"{result['updated']} updated"
                    )
                    
                    # Save sync statistics to zone_sync_stats collection
                    await mongodb.save_sync_stats(
                        tld=tld,
                        inserted=result['inserted'],
                        updated=result['updated']
                    )
                    
                    # Clean up zone file to save disk space
                    try:
                        file_path.unlink()
                    except Exception:
                        pass
                    
                except Exception as e:
                    error_msg = f"Error processing {tld}: {str(e)}"
                    logger.error(f"[{sync_id}] {error_msg}")
                    status.errors.append(error_msg)
            
            status.status = "completed"
            status.message = f"Sync completed: {processed_tlds} TLDs, {total_domains:,} domains"
            status.completed_at = datetime.utcnow()
            
            self.last_sync = datetime.utcnow()
            
            logger.info(f"[{sync_id}] {status.message}")
            
        except Exception as e:
            status.status = "error"
            status.message = f"Sync failed: {str(e)}"
            status.errors.append(str(e))
            logger.error(f"[{sync_id}] Sync failed: {str(e)}")
        
        finally:
            self.is_syncing = False
            self.current_sync_id = None
    
    def get_status(self, sync_id: Optional[str] = None) -> Optional[SyncStatus]:
        """Get sync status by sync_id or current/last sync."""
        if sync_id:
            return self.sync_history.get(sync_id)
        
        # Return current or most recent
        if self.current_sync_id:
            return self.sync_history.get(self.current_sync_id)
        
        # Return most recent completed
        if self.sync_history:
            return list(self.sync_history.values())[-1]
        
        return None
    
    def get_all_syncs(self) -> List[SyncStatus]:
        """Get all sync history."""
        return list(self.sync_history.values())


# Singleton instance
sync_service = SyncService()
