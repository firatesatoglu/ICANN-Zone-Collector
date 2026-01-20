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
    """Service to orchestrate zone file sync operations with parallel processing."""
    
    def __init__(self):
        self.last_sync: Optional[datetime] = None
        self.is_syncing: bool = False
        self.current_sync_id: Optional[str] = None
        self.sync_history: Dict[str, SyncStatus] = {}
        self._lock = asyncio.Lock()
        self._processed_count = 0
        self._total_domains = 0
    
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
        self._processed_count = 0
        self._total_domains = 0
        
        status = SyncStatus(
            sync_id=sync_id,
            status="running",
            message="Starting sync...",
            started_at=datetime.utcnow()
        )
        self.sync_history[sync_id] = status
        
        asyncio.create_task(self._run_sync(sync_id, tlds_filter))
        
        return sync_id
    
    async def _process_single_tld(
        self,
        url: str,
        sync_id: str,
        status: SyncStatus,
        semaphore: asyncio.Semaphore
    ) -> Dict:
        """Process a single TLD with semaphore control for concurrency."""
        async with semaphore:
            tld = czds_client.extract_tld_from_url(url)
            result = {"tld": tld, "success": False, "domains": 0, "inserted": 0, "updated": 0}
            
            try:
                file_path = await czds_client.download_zone_file(url)
                
                if not file_path:
                    async with self._lock:
                        status.errors.append(f"Failed to download {tld}")
                    return result
                
                parsed_tld, domains = parse_zone_file(file_path)
                
                if not domains:
                    logger.warning(f"[{sync_id}] No domains found in {tld}")
                    try:
                        file_path.unlink()
                    except Exception:
                        pass
                    return result
                
                await mongodb.ensure_indexes(tld)
                
                upsert_result = await mongodb.upsert_domains(
                    tld=tld,
                    domains=domains,
                    zone_file_date=datetime.utcnow()
                )
                
                await mongodb.save_sync_stats(
                    tld=tld,
                    inserted=upsert_result['inserted'],
                    updated=upsert_result['updated']
                )
                
                await mongodb.save_sync_metadata(tld=tld, domain_count=len(domains))
                
                async with self._lock:
                    self._processed_count += 1
                    self._total_domains += len(domains)
                    status.tlds_processed = self._processed_count
                    status.total_domains_processed = self._total_domains
                    status.message = f"Processing... {self._processed_count} TLDs done"
                
                logger.info(
                    f"[{sync_id}] Processed {tld}: {upsert_result['inserted']} new, "
                    f"{upsert_result['updated']} updated, {len(domains):,} total"
                )
                
                result["success"] = True
                result["domains"] = len(domains)
                result["inserted"] = upsert_result['inserted']
                result["updated"] = upsert_result['updated']
                
                try:
                    file_path.unlink()
                except Exception:
                    pass
                
            except Exception as e:
                error_msg = f"Error processing {tld}: {str(e)}"
                logger.error(f"[{sync_id}] {error_msg}")
                async with self._lock:
                    status.errors.append(error_msg)
            
            return result
    
    async def _run_sync(
        self, 
        sync_id: str,
        tlds_filter: Optional[List[str]] = None
    ):
        """
        Internal method to perform the actual sync with PARALLEL processing.
        Uses asyncio.Semaphore to limit concurrent downloads.
        """
        status = self.sync_history[sync_id]
        
        try:
            status.message = "Authenticating with ICANN..."
            if not await czds_client.authenticate():
                status.status = "error"
                status.message = "Failed to authenticate with ICANN"
                return
            
            status.message = "Getting zone file links..."
            zone_links = await czds_client.get_zone_links()
            
            if not zone_links:
                status.status = "error"
                status.message = "No zone files available"
                return
            
            if tlds_filter:
                zone_links = [
                    url for url in zone_links 
                    if czds_client.extract_tld_from_url(url) in tlds_filter
                ]
            
            total_tlds = len(zone_links)
            logger.info(f"[{sync_id}] Found {total_tlds} zone files to process (parallel: {settings.max_concurrent_downloads})")
            status.message = f"Processing {total_tlds} TLDs in parallel..."
            
            semaphore = asyncio.Semaphore(settings.max_concurrent_downloads)
            
            tasks = [
                self._process_single_tld(url, sync_id, status, semaphore)
                for url in zone_links
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            successful = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
            total_domains = sum(r.get("domains", 0) for r in results if isinstance(r, dict))
            total_inserted = sum(r.get("inserted", 0) for r in results if isinstance(r, dict))
            total_updated = sum(r.get("updated", 0) for r in results if isinstance(r, dict))
            
            status.status = "completed"
            status.message = f"Sync completed: {successful}/{total_tlds} TLDs, {total_domains:,} domains ({total_inserted:,} new, {total_updated:,} updated)"
            status.completed_at = datetime.utcnow()
            status.tlds_processed = successful
            status.total_domains_processed = total_domains
            
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
        
        if self.current_sync_id:
            return self.sync_history.get(self.current_sync_id)
        
        if self.sync_history:
            return list(self.sync_history.values())[-1]
        
        return None
    
    def get_all_syncs(self) -> List[SyncStatus]:
        """Get all sync history."""
        return list(self.sync_history.values())


sync_service = SyncService()
