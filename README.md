# Zone Collector Service

ICANN CZDS zone file collector service for phishing detection.

## Setup

```bash
pip install -r requirements.txt
```

## Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

## Run

```bash
uvicorn app.main:app --reload --port 8000
```

## Docker

```bash
docker build -t zone-collector .
docker run -p 8001:8000 --env-file .env zone-collector
```
