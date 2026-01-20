# Zone Collector Service

ICANN CZDS (Centralized Zone Data Service) zone file collector and newly registered domain detection service.

## Features

- Automatic zone file download from ICANN CZDS
- Gzip compressed zone file parsing
- MongoDB domain upsert (insert/update)
- Newly registered domain detection (`first_seen` tracking)
- Sync statistics (`zone_sync_stats` collection)
- Scheduled automatic sync (APScheduler)
- Memory usage monitoring

## Installation

### Requirements

- Python 3.11+
- MongoDB 5.0+
- ICANN CZDS account

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file:

```bash
# MongoDB
MONGODB_URL=mongodb://user:pass@localhost:27017/
MONGODB_DB=icann_tlds_db

# ICANN CZDS Credentials
ICANN_USERNAME=your_email@example.com
ICANN_PASSWORD=your_password

# Sync Schedule (Istanbul timezone)
SCHEDULE_HOURS=6,12,18

# Zone File Directory
ZONE_FILES_DIR=./zonefiles
```

> ⚠️ **Docker ICANN_PASSWORD:** If your password contains `$`, escape it as `\$` or use single quotes.

## Running

### Local

```bash
uvicorn app.main:app --reload --port 8002
```

### Docker

```bash
docker build -t zone-collector .
docker run -p 8002:8000 --env-file .env zone-collector
```

## API Endpoints

### Health & Status

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Swagger UI |
| `/api/v1/health` | GET | Health check |
| `/api/v1/sync/status` | GET | Active sync status |

### Zone Sync

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/sync` | POST | Start manual sync |
| `/api/v1/sync/status/{sync_id}` | GET | Specific sync status |

### TLD Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/tlds` | GET | List available TLDs |
| `/api/v1/tlds/{tld}/stats` | GET | TLD statistics |
| `/api/v1/tlds/{tld}/domains` | GET | TLD domains (paginated) |
| `/api/v1/zones/links` | GET | Active zone file links |

### Newly Registered Domains

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/newly-registered` | GET | Newly registered domains |
| `/newly-registered/stats` | GET | Sync statistics |

#### `/newly-registered` Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `days_back` | int | 7 | Days to look back (1-365) |
| `tld` | str | null | TLD filter (optional) |
| `page` | int | 1 | Page number |
| `page_size` | int | 100 | Records per page |

#### `/newly-registered/stats` Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `days_back` | int | 7 | Days to look back (1-365) |
| `tld` | str | null | TLD filter (optional) |

## MongoDB Schema

### Domain Collections (`{tld}_tld`)

Separate collection for each TLD:

```javascript
// Collection: com_tld
{
  "_id": ObjectId("..."),
  "domain": "example",           // Domain name (without TLD)
  "first_seen": ISODate("..."),  // First seen date
  "last_seen": ISODate("..."),   // Last seen date
  "dns_records": { ... },        // DNS records (optional)
  "metadata": {
    "source": "icann_czds",
    "zone_file_date": ISODate("...")
  }
}

// Indexes
{ "domain": 1 }         // Unique index
{ "first_seen": -1 }    // For new domain queries
{ "last_seen": -1 }     // For activity queries
```

### Sync Statistics (`zone_sync_stats`)

Statistics for each sync operation:

```javascript
{
  "_id": ObjectId("..."),
  "tld": "com",
  "inserted": 1500,              // New domains inserted
  "updated": 500,                // Domains updated
  "total_changes": 2000,         // Total changes
  "sync_time": ISODate("...")    // Sync timestamp
}
```

## Architecture

```
zone-collector/
├── app/
│   ├── main.py              # FastAPI app, lifespan, memory monitoring
│   ├── config.py            # Settings (Pydantic)
│   ├── scheduler.py         # APScheduler scheduled tasks
│   ├── api/
│   │   └── routes.py        # API endpoints
│   ├── database/
│   │   └── mongodb.py       # MongoDB operations
│   └── services/
│       ├── icann_client.py  # ICANN CZDS API client
│       ├── zone_parser.py   # Zone file parser
│       └── sync_service.py  # Sync orchestration
├── zonefiles/               # Downloaded zone files
├── requirements.txt
├── Dockerfile
└── .env
```

## Sync Flow

```mermaid
sequenceDiagram
    participant Scheduler
    participant SyncService
    participant ICANN
    participant Parser
    participant MongoDB

    Scheduler->>SyncService: trigger_sync()
    SyncService->>ICANN: authenticate()
    ICANN-->>SyncService: JWT token
    SyncService->>ICANN: get_zone_links()
    ICANN-->>SyncService: zone file URLs
    
    loop For each TLD
        SyncService->>ICANN: download_zone(url)
        ICANN-->>SyncService: zone.gz
        SyncService->>Parser: parse_zone_file()
        Parser-->>SyncService: domains[]
        SyncService->>MongoDB: upsert_domains()
        MongoDB-->>SyncService: {inserted, updated}
        SyncService->>MongoDB: save_sync_stats()
    end
```

## Monitoring

The service logs memory usage every 5 minutes:

```
📊 Memory Usage - Process: 69.1 MB (RSS), 425178.5 MB (VMS) | System: 5133/16384 MB available (68.7% used)
```

## Error Handling

- ICANN authentication errors: 401/403 → retry with new token
- Zone file download errors: Log and continue
- MongoDB connection errors: Exponential backoff retry
- Parse errors: Log and skip that TLD
