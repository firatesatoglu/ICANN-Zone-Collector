from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime

from app.models.domain import (
    TLDStats, 
    SyncStatus, 
    SyncRequest, 
    HealthResponse
)
from app.database.mongodb import mongodb
from app.services.sync_service import sync_service
from app.services.czds_client import czds_client
from app.scheduler import is_scheduler_running, get_next_run_time

health_router = APIRouter(tags=["Health"])
sync_router = APIRouter(tags=["Sync"])
zones_router = APIRouter(tags=["Zones"])
newly_registered_router = APIRouter(tags=["Newly Registered"])



@health_router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    mongodb_connected = False
    try:
        if mongodb.client:
            await mongodb.client.admin.command("ping")
            mongodb_connected = True
    except Exception:
        pass
    
    icann_authenticated = czds_client.access_token is not None
    
    return HealthResponse(
        status="healthy" if mongodb_connected else "degraded",
        mongodb_connected=mongodb_connected,
        icann_authenticated=icann_authenticated,
        scheduler_running=is_scheduler_running(),
        last_sync=sync_service.last_sync,
        next_sync=get_next_run_time()
    )



@sync_router.post("/sync")
async def trigger_sync(request: Optional[SyncRequest] = None):
    """
    Trigger a manual zone file sync. if you give list of tlds in request it will sync only those tlds. if you don't give any tld it will sync all tlds.
    Returns immediately with sync_id. Use /sync/status?sync_id=xxx to check progress.
    """
    if sync_service.is_syncing:
        raise HTTPException(
            status_code=409,
            detail="Sync already in progress"
        )
    
    tlds_filter = request.tlds if request else None
    
    try:
        sync_id = sync_service.start_sync(tlds_filter)
        return {
            "sync_id": sync_id,
            "status": "started",
            "message": "Sync started in background. Use /sync/status?sync_id=" + sync_id + " to check progress."
        }
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@sync_router.get("/sync/status", response_model=Optional[SyncStatus])
async def get_sync_status(sync_id: Optional[str] = None):
    """Get sync status by sync_id or current/last sync."""
    status = sync_service.get_status(sync_id)
    if not status:
        raise HTTPException(
            status_code=404,
            detail="No sync found" + (f" with id {sync_id}" if sync_id else "")
        )
    return status



@zones_router.get("/tlds", response_model=List[str])
async def list_tlds():
    """List all TLDs with data in the database."""
    return await mongodb.get_all_tlds()


@zones_router.get("/tlds/{tld}/stats", response_model=TLDStats)
async def get_tld_stats(tld: str):
    """Get statistics for a specific TLD."""
    collections = await mongodb.list_tld_collections()
    collection_name = mongodb.get_collection_name(tld)
    
    if collection_name not in collections:
        raise HTTPException(
            status_code=404,
            detail=f"TLD '{tld}' not found"
        )
    
    stats = await mongodb.get_tld_stats(tld)
    return TLDStats(**stats)


@zones_router.get("/tlds/{tld}/domains")
async def get_tld_domains(
    tld: str,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Items per page")
):
    """
    Get all domains for a specific TLD with pagination.
    Returns domain.tld format with all details including DNS records.
    """
    collections = await mongodb.list_tld_collections()
    collection_name = mongodb.get_collection_name(tld)
    
    if collection_name not in collections:
        raise HTTPException(
            status_code=404,
            detail=f"TLD '{tld}' not found"
        )
    
    result = await mongodb.get_domains_by_tld(tld, page, page_size)
    return result


@zones_router.get("/zone-links")
async def get_zone_links():
    """
    Get available zone file download links from ICANN.
    Returns total count and list of zones with TLD name and download URL.
    """
    links = await czds_client.get_zone_links()
    
    zones = []
    for link in links:
        tld = czds_client.extract_tld_from_url(link)
        zones.append({
            "zone": tld,
            "download_link": link
        })
    
    return {
        "total": len(zones),
        "zones": zones
    }



@newly_registered_router.get("/newly-registered")
async def get_newly_registered_domains(
    tld: Optional[str] = Query(None, description="Filter by specific TLD (optional, all TLDs if empty)"),
    days_back: int = Query(1, ge=1, le=365, description="Number of days back to search"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Items per page")
):
    """
    Get newly registered domains from the last X days.
    
    If TLD is specified, returns domains only from that TLD.
    If TLD is not specified, returns domains from all TLDs.
    
    Returns domains sorted by first_seen date (newest first).
    """
    from datetime import timedelta
    
    now = datetime.utcnow()
    end_date = datetime(now.year, now.month, now.day) + timedelta(days=1)
    start_date = end_date - timedelta(days=days_back)
    
    query = {
        "first_seen": {
            "$gte": start_date,
            "$lt": end_date
        }
    }
    
    if tld:
        collections_list = await mongodb.list_tld_collections()
        collection_name = mongodb.get_collection_name(tld)
        if collection_name not in collections_list:
            raise HTTPException(status_code=404, detail=f"TLD '{tld}' not found")
        collection_names = [collection_name]
    else:
        collection_names = await mongodb.list_tld_collections()
    
    total = 0
    for coll_name in collection_names:
        collection = mongodb.db[coll_name]
        total += await collection.count_documents(query)
    
    skip = (page - 1) * page_size
    remaining_skip = skip
    remaining_limit = page_size
    domains = []
    
    for coll_name in collection_names:
        if remaining_limit <= 0:
            break
            
        collection = mongodb.db[coll_name]
        tld_name = coll_name.replace("_tld", "").replace("_", ".")
        coll_count = await collection.count_documents(query)
        
        if remaining_skip >= coll_count:
            remaining_skip -= coll_count
            continue
        
        cursor = collection.find(query).sort("first_seen", -1).skip(remaining_skip).limit(remaining_limit)
        remaining_skip = 0
        
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            doc["tld"] = tld_name
            domains.append(doc)
            remaining_limit -= 1
            if remaining_limit <= 0:
                break
    
    tlds_to_check = [tld] if tld else None
    sync_gap_info = await mongodb.check_sync_gaps(tlds=tlds_to_check, max_gap_hours=48)
    
    response = {
        "search_params": {
            "days_back": days_back,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "tld": tld or "all"
        },
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 0,
        "domains": domains
    }
    
    if sync_gap_info.get("has_gaps"):
        response["warning"] = sync_gap_info.get("warning")
        response["sync_gaps"] = {
            "stale_tlds": sync_gap_info.get("stale_tlds", []),
            "never_synced_tlds": sync_gap_info.get("never_synced_tlds", [])
        }
    
    return response


@newly_registered_router.get("/newly-registered/stats")
async def get_newly_registered_stats(
    days_back: int = Query(7, ge=1, le=365, description="Number of days back"),
    tld: Optional[str] = Query(None, description="Optional TLD filter")
):
    """
    Get sync statistics from the zone_sync_stats collection.
    
    Returns aggregated inserted/updated counts per TLD and per day for the specified period.
    
    - **days_back**: Number of days to look back (1-365)
    - **tld**: Optional TLD to filter results
    """
    stats = await mongodb.get_sync_stats(days_back=days_back, tld=tld)
    
    return stats
