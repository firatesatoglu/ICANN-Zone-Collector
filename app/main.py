from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
import asyncio
import psutil
import os

from app.config import settings
from app.database.mongodb import mongodb
from app.api.routes import health_router, sync_router, zones_router, newly_registered_router
from app.scheduler import init_scheduler, start_scheduler, stop_scheduler

logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

memory_monitor_task = None


async def log_memory_usage():
    """Log memory usage every 5 minutes."""
    while True:
        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            
            rss_mb = memory_info.rss / (1024 * 1024)
            
            vms_mb = memory_info.vms / (1024 * 1024)
            
            system_memory = psutil.virtual_memory()
            total_mb = system_memory.total / (1024 * 1024)
            available_mb = system_memory.available / (1024 * 1024)
            percent_used = system_memory.percent
            
            logger.info(
                f"📊 Memory Usage - "
                f"Process: {rss_mb:.1f} MB (RSS), {vms_mb:.1f} MB (VMS) | "
                f"System: {available_mb:.0f}/{total_mb:.0f} MB available ({percent_used}% used)"
            )
            
        except Exception as e:
            logger.error(f"Failed to get memory info: {e}")
        
        await asyncio.sleep(300)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global memory_monitor_task
    
    logger.info("Starting Zone Collector Service...")
    
    await mongodb.connect()
    
    init_scheduler()
    start_scheduler()
    
    memory_monitor_task = asyncio.create_task(log_memory_usage())
    
    logger.info("Zone Collector Service started successfully")
    
    yield
    
    logger.info("Shutting down Zone Collector Service...")
    
    if memory_monitor_task:
        memory_monitor_task.cancel()
        try:
            await memory_monitor_task
        except asyncio.CancelledError:
            pass
    
    stop_scheduler()
    await mongodb.disconnect()
    logger.info("Zone Collector Service stopped")


app = FastAPI(
    title="Zone Collector Service",
    description="ICANN CZDS zone file collector",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/",
    redoc_url="/redoc"
)

app.include_router(health_router)
app.include_router(sync_router)
app.include_router(zones_router)
app.include_router(newly_registered_router)