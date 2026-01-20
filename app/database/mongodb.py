from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import UpdateOne
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import logging

from app.config import settings

logger = logging.getLogger(__name__)


class MongoDB:
    client: Optional[AsyncIOMotorClient] = None
    db: Optional[AsyncIOMotorDatabase] = None
    
    async def connect(self):
        """Connect to MongoDB."""
        logger.info(f"Connecting to MongoDB at {settings.mongodb_url}")
        self.client = AsyncIOMotorClient(settings.mongodb_url)
        self.db = self.client[settings.database_name]
        
        await self.client.admin.command("ping")
        logger.info(f"Connected to MongoDB database: {settings.database_name}")
    
    async def disconnect(self):
        """Disconnect from MongoDB."""
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")
    
    def get_collection_name(self, tld: str) -> str:
        """Get collection name for a TLD."""
        safe_tld = tld.lower().replace(".", "_").replace("-", "_")
        return f"{safe_tld}_tld"
    
    async def ensure_indexes(self, tld: str):
        """Ensure indexes exist for a TLD collection."""
        collection_name = self.get_collection_name(tld)
        collection = self.db[collection_name]
        
        await collection.create_index("domain", unique=True)
        await collection.create_index("first_seen")
        await collection.create_index("last_seen")
        await collection.create_index("fqdn")
        await collection.create_index("dns_records.ns")
        
        logger.debug(f"Ensured indexes for collection: {collection_name}")
    
    async def upsert_domains(
        self, 
        tld: str, 
        domains: Dict[str, Any],
        zone_file_date: datetime
    ) -> Dict[str, int]:
        """
        Upsert domains into the TLD collection.
        domains is a dict of domain_name -> DomainRecord
        Returns count of inserted and updated domains.
        """
        if not domains:
            return {"inserted": 0, "updated": 0}
        
        collection_name = self.get_collection_name(tld)
        collection = self.db[collection_name]
        
        operations = []
        now = datetime.utcnow()
        
        for domain_name, record in domains.items():
            fqdn = f"{domain_name}.{tld}"
            
            dns_records = {}
            if hasattr(record, 'ns') and record.ns:
                dns_records["ns"] = record.ns
            if hasattr(record, 'a') and record.a:
                dns_records["a"] = record.a
            if hasattr(record, 'aaaa') and record.aaaa:
                dns_records["aaaa"] = record.aaaa
            if hasattr(record, 'ds') and record.ds:
                dns_records["ds"] = record.ds
            
            update_data = {
                "$setOnInsert": {
                    "domain": domain_name,
                    "fqdn": fqdn,
                    "tld": tld,
                    "first_seen": now
                },
                "$set": {
                    "last_seen": now,
                    "metadata": {
                        "source": "icann_czds",
                        "zone_file_date": zone_file_date
                    }
                }
            }
            
            if dns_records:
                update_data["$set"]["dns_records"] = dns_records
            
            operations.append(
                UpdateOne(
                    {"domain": domain_name},
                    update_data,
                    upsert=True
                )
            )
        
        batch_size = settings.upsert_batch_size
        total_inserted = 0
        total_updated = 0
        
        for i in range(0, len(operations), batch_size):
            batch = operations[i:i + batch_size]
            result = await collection.bulk_write(batch, ordered=False)
            total_inserted += result.upserted_count
            total_updated += result.modified_count
        
        logger.info(
            f"TLD {tld}: Inserted {total_inserted}, Updated {total_updated} domains"
        )
        
        return {"inserted": total_inserted, "updated": total_updated}
    
    async def get_tld_stats(self, tld: str) -> Dict[str, Any]:
        """Get statistics for a TLD collection."""
        collection_name = self.get_collection_name(tld)
        collection = self.db[collection_name]
        
        total_count = await collection.count_documents({})
        
        pipeline = [
            {
                "$group": {
                    "_id": None,
                    "earliest_first_seen": {"$min": "$first_seen"},
                    "latest_first_seen": {"$max": "$first_seen"},
                    "latest_last_seen": {"$max": "$last_seen"}
                }
            }
        ]
        
        stats_cursor = collection.aggregate(pipeline)
        stats = await stats_cursor.to_list(length=1)
        
        if stats:
            return {
                "tld": tld,
                "collection": collection_name,
                "total_domains": total_count,
                "earliest_first_seen": stats[0].get("earliest_first_seen"),
                "latest_first_seen": stats[0].get("latest_first_seen"),
                "latest_last_seen": stats[0].get("latest_last_seen")
            }
        
        return {
            "tld": tld,
            "collection": collection_name,
            "total_domains": total_count
        }
    
    async def list_tld_collections(self) -> List[str]:
        """List all TLD collections in the database."""
        collections = await self.db.list_collection_names()
        tld_collections = [c for c in collections if c.endswith("_tld")]
        return tld_collections
    
    async def get_all_tlds(self) -> List[str]:
        """Get list of all TLDs from collection names."""
        collections = await self.list_tld_collections()
        return [c.replace("_tld", "").replace("_", ".") for c in collections]
    
    async def get_domains_by_tld(
        self, 
        tld: str, 
        page: int = 1, 
        page_size: int = 100
    ) -> Dict[str, Any]:
        """
        Get all domains for a specific TLD with pagination.
        Returns domains with all their details.
        """
        collection_name = self.get_collection_name(tld)
        collection = self.db[collection_name]
        
        skip = (page - 1) * page_size
        
        total = await collection.count_documents({})
        
        cursor = collection.find({}).skip(skip).limit(page_size).sort("domain", 1)
        
        domains = []
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            domains.append(doc)
        
        return {
            "tld": tld,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
            "domains": domains
        }
    
    async def save_sync_stats(
        self, 
        tld: str, 
        inserted: int, 
        updated: int,
        sync_time: datetime = None
    ) -> Dict[str, Any]:
        """
        Save sync statistics for a TLD to the zone_sync_stats collection.
        
        Args:
            tld: The TLD that was synced
            inserted: Number of new domains inserted
            updated: Number of existing domains updated
            sync_time: Timestamp of the sync (defaults to now)
        
        Returns:
            The inserted document ID
        """
        if sync_time is None:
            sync_time = datetime.utcnow()
        
        collection = self.db["zone_sync_stats"]
        
        doc = {
            "tld": tld,
            "inserted": inserted,
            "updated": updated,
            "sync_time": sync_time,
            "total_changes": inserted + updated
        }
        
        result = await collection.insert_one(doc)
        logger.info(f"Saved sync stats for {tld}: inserted={inserted}, updated={updated}")
        
        return {"id": str(result.inserted_id)}
    
    async def get_sync_stats(
        self, 
        days_back: int = 7,
        tld: str = None,
        start_date: datetime = None,
        end_date: datetime = None
    ) -> Dict[str, Any]:
        """
        Get aggregated sync statistics from the zone_sync_stats collection.
        
        Args:
            days_back: Number of days to look back (ignored if start_date/end_date provided)
            tld: Optional TLD filter
            start_date: Optional start date for date range query
            end_date: Optional end date for date range query
        
        Returns:
            Aggregated statistics by TLD and date
        """
        collection = self.db["zone_sync_stats"]
        
        if start_date and end_date:
            date_filter = {"sync_time": {"$gte": start_date, "$lte": end_date}}
        else:
            cutoff_date = datetime.utcnow() - timedelta(days=days_back)
            date_filter = {"sync_time": {"$gte": cutoff_date}}
        
        match_stage = date_filter.copy()
        if tld:
            match_stage["tld"] = tld
        
        pipeline = [
            {"$match": match_stage},
            {
                "$group": {
                    "_id": "$tld",
                    "total_inserted": {"$sum": "$inserted"},
                    "total_updated": {"$sum": "$updated"},
                    "total_changes": {"$sum": "$total_changes"},
                    "sync_count": {"$sum": 1},
                    "first_sync": {"$min": "$sync_time"},
                    "last_sync": {"$max": "$sync_time"}
                }
            },
            {"$sort": {"total_changes": -1}}
        ]
        
        cursor = collection.aggregate(pipeline)
        tld_stats = []
        grand_total_inserted = 0
        grand_total_updated = 0
        
        async for doc in cursor:
            tld_stats.append({
                "tld": doc["_id"],
                "total_inserted": doc["total_inserted"],
                "total_updated": doc["total_updated"],
                "total_changes": doc["total_changes"],
                "sync_count": doc["sync_count"],
                "first_sync": doc["first_sync"].isoformat() if doc["first_sync"] else None,
                "last_sync": doc["last_sync"].isoformat() if doc["last_sync"] else None
            })
            grand_total_inserted += doc["total_inserted"]
            grand_total_updated += doc["total_updated"]
        
        daily_pipeline = [
            {"$match": match_stage},
            {
                "$group": {
                    "_id": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$sync_time"}
                    },
                    "inserted": {"$sum": "$inserted"},
                    "updated": {"$sum": "$updated"},
                    "total_changes": {"$sum": "$total_changes"}
                }
            },
            {"$sort": {"_id": -1}}
        ]
        
        daily_cursor = collection.aggregate(daily_pipeline)
        daily_stats = []
        async for doc in daily_cursor:
            daily_stats.append({
                "date": doc["_id"],
                "inserted": doc["inserted"],
                "updated": doc["updated"],
                "total_changes": doc["total_changes"]
            })
        
        return {
            "days_back": days_back,
            "tld_filter": tld,
            "summary": {
                "total_inserted": grand_total_inserted,
                "total_updated": grand_total_updated,
                "total_changes": grand_total_inserted + grand_total_updated,
                "tld_count": len(tld_stats)
            },
            "by_tld": tld_stats,
            "by_date": daily_stats
        }
    
    async def save_sync_metadata(
        self,
        tld: str,
        domain_count: int,
        sync_time: datetime = None
    ) -> Dict[str, Any]:
        """
        Save sync metadata for a TLD to track sync history.
        Used for false positive prevention by detecting sync gaps.
        """
        if sync_time is None:
            sync_time = datetime.utcnow()
        
        collection = self.db["zone_sync_metadata"]
        
        await collection.update_one(
            {"tld": tld},
            {
                "$set": {
                    "last_sync": sync_time,
                    "domain_count": domain_count
                },
                "$inc": {"sync_count": 1},
                "$setOnInsert": {"first_sync": sync_time}
            },
            upsert=True
        )
        
        return {"tld": tld, "sync_time": sync_time}
    
    async def check_sync_gaps(
        self,
        tlds: List[str] = None,
        max_gap_hours: int = 48
    ) -> Dict[str, Any]:
        """
        Check for sync gaps that could cause false positives.
        Returns TLDs that haven't been synced within max_gap_hours.
        """
        collection = self.db["zone_sync_metadata"]
        
        cutoff_time = datetime.utcnow() - timedelta(hours=max_gap_hours)
        
        query = {"last_sync": {"$lt": cutoff_time}}
        if tlds:
            query["tld"] = {"$in": tlds}
        
        cursor = collection.find(query)
        
        stale_tlds = []
        async for doc in cursor:
            stale_tlds.append({
                "tld": doc["tld"],
                "last_sync": doc["last_sync"].isoformat() if doc.get("last_sync") else None,
                "hours_since_sync": int((datetime.utcnow() - doc["last_sync"]).total_seconds() / 3600) if doc.get("last_sync") else None
            })
        
        all_synced_tlds = set()
        async for doc in collection.find({}, {"tld": 1}):
            all_synced_tlds.add(doc["tld"])
        
        db_tlds = set(await self.get_all_tlds())
        never_synced = db_tlds - all_synced_tlds
        
        has_gaps = len(stale_tlds) > 0 or len(never_synced) > 0
        
        return {
            "has_gaps": has_gaps,
            "max_gap_hours": max_gap_hours,
            "stale_tlds": stale_tlds,
            "never_synced_tlds": list(never_synced),
            "warning": "Data may contain false positives for TLDs with gaps" if has_gaps else None
        }


mongodb = MongoDB()
