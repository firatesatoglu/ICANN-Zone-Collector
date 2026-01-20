# Zone Collector Service

ICANN CZDS (Centralized Zone Data Service) zone file collector and newly registered domain detection service.

![API Documentation](docs/zone-collector-api-docs.png)

## Features

- **Parallel zone file download** from ICANN CZDS (configurable concurrency)
- Gzip compressed zone file parsing (streaming, memory-efficient)
- MongoDB domain upsert with bulk write operations
- Newly registered domain detection (`first_seen` tracking)
- Sync statistics (`zone_sync_stats` collection)
- **Sync gap detection** for false positive prevention
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
MONGODB_URL=mongodb://user:pass@localhost:27017/
DATABASE_NAME=icann_tlds_db

ICANN_USERNAME=your_email@example.com
ICANN_PASSWORD=your_password

SCHEDULE_HOURS=0,12
ZONE_FILES_DIR=./zonefiles

MAX_CONCURRENT_DOWNLOADS=10
UPSERT_BATCH_SIZE=5000
```

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

### Health & Sync

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/sync` | POST | Start manual sync |
| `/sync/status` | GET | Current sync status |

### TLD Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/tlds` | GET | List available TLDs |
| `/tlds/{tld}/stats` | GET | TLD statistics |
| `/tlds/{tld}/domains` | GET | TLD domains (paginated) |
| `/zone-links` | GET | Available zone file links |

### Newly Registered Domains

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/newly-registered` | GET | Newly registered domains |
| `/newly-registered/stats` | GET | Sync statistics |

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `days_back` | int | 7 | Days to look back (1-365) |
| `tld` | str | null | TLD filter |
| `page` | int | 1 | Page number |
| `page_size` | int | 100 | Records per page |

## Performance Optimizations

### Parallel Processing

TLDs are processed in parallel using `asyncio.Semaphore`:

```python
max_concurrent_downloads = 10  # Configurable
```

### Bulk Write Operations

MongoDB writes use bulk operations:

```python
upsert_batch_size = 5000  # Configurable
ordered = False  # Continues on error
```

### Chunked Parsing (Memory Optimized)

Large zone files (1M+ domains) are processed in 50K chunks to prevent OOM:

```python
for tld, domains, is_last in parse_zone_file_chunked(file):
    # Process 50K domains at a time
    await mongodb.upsert_domains(tld, domains)
    del domains  # Free memory
```

This allows processing files like `vip.txt.gz` (1.5M domains) without OOM.

## MongoDB Schema

### Domain Collections (`{tld}_tld`)

```javascript
{
  "domain": "example",
  "fqdn": "example.com",
  "first_seen": ISODate(),
  "last_seen": ISODate(),
  "dns_records": { "ns": [...], "a": [...] },
  "metadata": { "source": "icann_czds" }
}
```

### Sync Statistics (`zone_sync_stats`)

```javascript
{
  "tld": "com",
  "inserted": 1500,
  "updated": 500,
  "sync_time": ISODate()
}
```

### Sync Metadata (`zone_sync_metadata`)

```javascript
{
  "tld": "com",
  "last_sync": ISODate(),
  "domain_count": 150000000,
  "sync_count": 42
}
```

## Architecture

```
zone-collector/
├── app/
│   ├── main.py              # FastAPI app, memory monitoring
│   ├── config.py            # Settings (Pydantic)
│   ├── scheduler.py         # APScheduler
│   ├── api/routes.py        # API endpoints
│   ├── database/mongodb.py  # MongoDB operations
│   └── services/
│       ├── czds_client.py   # ICANN API client
│       ├── zone_parser.py   # Streaming parser
│       └── sync_service.py  # Parallel sync orchestration
└── requirements.txt
```
