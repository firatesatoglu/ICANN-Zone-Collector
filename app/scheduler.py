from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from typing import Optional
import logging
import pytz

from app.config import settings
from app.services.sync_service import sync_service

logger = logging.getLogger(__name__)

scheduler: Optional[AsyncIOScheduler] = None

ISTANBUL_TZ = pytz.timezone('Europe/Istanbul')


async def scheduled_sync_job():
    """Job function for scheduled sync."""
    logger.info("Starting scheduled zone sync...")
    try:
        sync_id = sync_service.start_sync()
        logger.info(f"Scheduled sync started with id: {sync_id}")
    except Exception as e:
        logger.error(f"Scheduled sync failed: {str(e)}")


def init_scheduler() -> AsyncIOScheduler:
    """Initialize and configure the scheduler."""
    global scheduler
    
    scheduler = AsyncIOScheduler(timezone=ISTANBUL_TZ)
    
    for hour in settings.schedule_hours_list:
        trigger = CronTrigger(hour=hour, minute=0, timezone=ISTANBUL_TZ)
        scheduler.add_job(
            scheduled_sync_job,
            trigger=trigger,
            id=f"zone_sync_{hour:02d}",
            name=f"Zone Sync at {hour:02d}:00 (Istanbul)",
            replace_existing=True
        )
        logger.info(f"Scheduled zone sync at {hour:02d}:00 (Istanbul time)")
    
    return scheduler


def start_scheduler():
    """Start the scheduler."""
    global scheduler
    if scheduler and not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started")


def stop_scheduler():
    """Stop the scheduler."""
    global scheduler
    if scheduler and scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped")


def get_next_run_time() -> Optional[datetime]:
    """Get the next scheduled run time."""
    global scheduler
    if scheduler:
        jobs = scheduler.get_jobs()
        if jobs:
            next_times = [job.next_run_time for job in jobs if job.next_run_time]
            if next_times:
                return min(next_times)
    return None


def is_scheduler_running() -> bool:
    """Check if scheduler is running."""
    global scheduler
    return scheduler is not None and scheduler.running
