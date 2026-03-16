"""
DB Change Detection Poller - Phase 2

Polls ProductChangeLog table for pending changes and groups them
into ChangeBundle objects for downstream processing.

Design:
- Polling-based (not CDC) for simplicity and compatibility
- Batch retrieval to minimize database round trips
- Groups changes by product_code for batch processing
- Marks changes as processed after successful retrieval
- Configurable polling interval and batch size
"""

import time
from datetime import datetime
from typing import List, Dict, Optional
from collections import defaultdict

from db.sql_client import get_sql_client, SQLServerClient
from parsers.models import DBChangeRecord, ChangeBundle
from utils.logger import get_logger

logger = get_logger(__name__)


class PollerConfig:
    """Configuration for change detection poller."""
    
    def __init__(
        self,
        poll_interval_seconds: int = 30,
        batch_size: int = 50,
        max_retries: int = 3,
        retry_backoff_seconds: int = 5
    ):
        """
        Initialize poller configuration.
        
        Args:
            poll_interval_seconds: Time between polling attempts
            batch_size: Number of changes to fetch per poll
            max_retries: Maximum retry attempts on failure
            retry_backoff_seconds: Base delay for exponential backoff
        """
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
    
    @classmethod
    def from_env(cls) -> 'PollerConfig':
        """
        Create configuration from environment variables.
        
        Environment Variables:
            POLLER_INTERVAL_SECONDS: Polling interval (default: 30)
            POLLER_BATCH_SIZE: Batch size (default: 50)
            POLLER_MAX_RETRIES: Max retries (default: 3)
            POLLER_RETRY_BACKOFF: Retry backoff (default: 5)
        """
        import os
        
        return cls(
            poll_interval_seconds=int(os.getenv("POLLER_INTERVAL_SECONDS", "30")),
            batch_size=int(os.getenv("POLLER_BATCH_SIZE", "50")),
            max_retries=int(os.getenv("POLLER_MAX_RETRIES", "3")),
            retry_backoff_seconds=int(os.getenv("POLLER_RETRY_BACKOFF", "5"))
        )


class ChangePoller:
    """
    Polls ProductChangeLog table and groups changes by product.
    
    Workflow:
    1. Poll get_pending_changes() periodically
    2. Convert raw SQL results to DBChangeRecord objects
    3. Group changes by product_code into ChangeBundle
    4. Mark all changes as processed
    5. Return bundles for downstream processing
    """
    
    def __init__(
        self,
        sql_client: Optional[SQLServerClient] = None,
        config: Optional[PollerConfig] = None
    ):
        """
        Initialize change poller.
        
        Args:
            sql_client: SQL Server client (defaults to singleton)
            config: Poller configuration (defaults to env-based)
        """
        self.sql_client = sql_client or get_sql_client()
        self.config = config or PollerConfig.from_env()
        self._running = False
        self._last_poll_time: Optional[datetime] = None
        
        logger.info(
            f"ChangePoller initialized: "
            f"interval={self.config.poll_interval_seconds}s, "
            f"batch_size={self.config.batch_size}"
        )
    
    def poll_once(self) -> List[ChangeBundle]:
        """
        Execute single polling cycle.
        
        Returns:
            List of ChangeBundle objects grouped by product_code
        
        Raises:
            Exception: If polling fails after max retries
        """
        logger.info(f"Starting poll cycle (batch_size={self.config.batch_size})")
        
        try:
            # Fetch pending changes from database
            raw_changes = self.sql_client.get_pending_changes(
                batch_size=self.config.batch_size
            )
            
            if not raw_changes:
                logger.info("No pending changes found")
                self._last_poll_time = datetime.utcnow()
                return []
            
            logger.info(f"Retrieved {len(raw_changes)} pending changes")
            
            # Convert raw SQL results to Pydantic models
            change_records = self._parse_change_records(raw_changes)
            
            # Group by product_code
            bundles = self._group_by_product(change_records)
            
            # Mark all changes as processed
            self._mark_processed([rec.change_log_id for rec in change_records])
            
            self._last_poll_time = datetime.utcnow()
            logger.info(
                f"Poll cycle complete: {len(bundles)} bundles, "
                f"{len(change_records)} total changes"
            )
            
            return bundles
        
        except Exception as e:
            logger.error(f"Poll cycle failed: {e}", exc_info=True)
            raise
    
    def start_polling(self, callback=None) -> None:
        """
        Start continuous polling loop.
        
        Args:
            callback: Optional function to call with each batch of bundles
                     Signature: callback(bundles: List[ChangeBundle]) -> None
        
        Note:
            Blocks until stop_polling() is called or interrupt received.
            Use in separate thread/process for non-blocking operation.
        """
        self._running = True
        logger.info("Starting continuous polling loop")
        
        while self._running:
            try:
                bundles = self.poll_once()
                
                if bundles and callback:
                    logger.debug(f"Invoking callback with {len(bundles)} bundles")
                    callback(bundles)
                
                # Sleep until next poll
                time.sleep(self.config.poll_interval_seconds)
            
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, stopping poller")
                self.stop_polling()
                break
            
            except Exception as e:
                logger.error(f"Error in polling loop: {e}", exc_info=True)
                
                # Exponential backoff on error
                backoff = self.config.retry_backoff_seconds
                logger.info(f"Retrying in {backoff} seconds...")
                time.sleep(backoff)
    
    def stop_polling(self) -> None:
        """Stop continuous polling loop."""
        logger.info("Stopping polling loop")
        self._running = False
    
    def _parse_change_records(self, raw_changes: List[Dict]) -> List[DBChangeRecord]:
        """
        Convert raw SQL results to Pydantic DBChangeRecord objects.
        
        Args:
            raw_changes: List of dicts from SQL query
        
        Returns:
            List of validated DBChangeRecord objects
        """
        records = []
        
        for raw in raw_changes:
            try:
                # Map SQL column names to Pydantic field names
                # GetPendingChanges returns: change_id, product_code, product_name,
                # source_table, column_name, op_type, old_value, new_value, changed_by, changed_at
                record = DBChangeRecord(
                    change_log_id=raw["change_id"],
                    product_code=raw["product_code"],
                    change_timestamp=raw["changed_at"].isoformat() 
                        if hasattr(raw["changed_at"], 'isoformat')
                        else str(raw["changed_at"]),
                    source_table=raw["source_table"],
                    operation_type=raw["op_type"],
                    column_name=raw["column_name"],
                    old_value=raw.get("old_value"),
                    new_value=raw.get("new_value"),
                    changed_by=raw.get("changed_by")
                )
                records.append(record)
            
            except Exception as e:
                logger.error(
                    f"Failed to parse change record {raw.get('change_id')}: {e}",
                    exc_info=True
                )
                # Continue processing other records
                continue
        
        logger.debug(f"Successfully parsed {len(records)}/{len(raw_changes)} records")
        return records
    
    def _group_by_product(self, records: List[DBChangeRecord]) -> List[ChangeBundle]:
        """
        Group change records by product_code into bundles.
        
        Args:
            records: List of DBChangeRecord objects
        
        Returns:
            List of ChangeBundle objects
        """
        # Group by product_code
        grouped: Dict[str, List[DBChangeRecord]] = defaultdict(list)
        for record in records:
            grouped[record.product_code].append(record)
        
        # Create bundles
        bundles = []
        detected_at = datetime.utcnow().isoformat()
        
        for product_code, changes in grouped.items():
            bundle = ChangeBundle(
                product_code=product_code,
                changes=changes,
                detected_at=detected_at
            )
            bundles.append(bundle)
            
            logger.debug(
                f"Created bundle for {product_code}: "
                f"{len(changes)} changes across {len(bundle.get_affected_tables())} tables"
            )
        
        return bundles
    
    def _mark_processed(self, change_log_ids: List[int]) -> None:
        """
        Mark all change records as processed.
        
        Args:
            change_log_ids: List of ChangeLogID values to mark
        
        Raises:
            Exception: If marking fails after retries
        """
        if not change_log_ids:
            return
        
        logger.debug(f"Marking {len(change_log_ids)} changes as processed")
        
        retries = 0
        while retries < self.config.max_retries:
            try:
                for change_id in change_log_ids:
                    self.sql_client.mark_change_processed(change_id)
                
                logger.info(f"Successfully marked {len(change_log_ids)} changes as processed")
                return
            
            except Exception as e:
                retries += 1
                logger.warning(
                    f"Failed to mark changes as processed (attempt {retries}/{self.config.max_retries}): {e}"
                )
                
                if retries >= self.config.max_retries:
                    logger.error("Max retries reached, changes not marked as processed")
                    raise
                
                # Exponential backoff
                backoff = self.config.retry_backoff_seconds * (2 ** (retries - 1))
                time.sleep(backoff)
    
    def get_status(self) -> Dict:
        """
        Get current poller status.
        
        Returns:
            Dictionary with status information
        """
        return {
            "running": self._running,
            "last_poll_time": self._last_poll_time.isoformat() if self._last_poll_time else None,
            "poll_interval_seconds": self.config.poll_interval_seconds,
            "batch_size": self.config.batch_size,
            "max_retries": self.config.max_retries
        }


# Singleton instance
_poller_instance: Optional[ChangePoller] = None


def get_change_poller(
    sql_client: Optional[SQLServerClient] = None,
    config: Optional[PollerConfig] = None
) -> ChangePoller:
    """
    Get singleton ChangePoller instance.
    
    Args:
        sql_client: Optional SQL client (uses singleton if not provided)
        config: Optional poller config (uses env-based if not provided)
    
    Returns:
        Shared ChangePoller instance
    """
    global _poller_instance
    
    if _poller_instance is None:
        _poller_instance = ChangePoller(sql_client=sql_client, config=config)
    
    return _poller_instance
