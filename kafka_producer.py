#!/usr/bin/env python3
"""
Async Kafka Producer for Email Scraper
Handles job submission and progress updates to Kafka topics
"""

import json
import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from aiokafka import AIOKafkaProducer
from kafka_config import get_kafka_config, get_topic_name, MESSAGE_TYPES

logger = logging.getLogger(__name__)

class AsyncKafkaProducer:
    """Async Kafka producer for email scraper job management."""
    
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self._config = get_kafka_config()
    
    async def start(self):
        """Start the Kafka producer."""
        if self.producer is None:
            self.producer = AIOKafkaProducer(**self._config)
            await self.producer.start()
            logger.info("Kafka producer started")
    
    async def stop(self):
        """Stop the Kafka producer."""
        if self.producer:
            await self.producer.stop()
            self.producer = None
            logger.info("Kafka producer stopped")
    
    async def send_job(self, job_data: Dict[str, Any]) -> bool:
        """Send a new job to the email-jobs topic."""
        try:
            await self.start()
            
            message = {
                "job_id": job_data["job_id"],
                "timestamp": datetime.utcnow().isoformat(),
                "type": "job_submission",
                "data": job_data
            }
            
            await self.producer.send_and_wait(
                topic=get_topic_name("EMAIL_JOBS"),
                key=job_data["job_id"].encode('utf-8'),
                value=json.dumps(message).encode('utf-8')
            )
            
            logger.info(f"Job {job_data['job_id']} sent to Kafka")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send job to Kafka: {e}")
            return False
    
    async def send_progress_update(self, job_id: str, progress_data: Dict[str, Any]) -> bool:
        """Send progress update to job-progress topic."""
        try:
            await self.start()
            
            message = {
                "job_id": job_id,
                "timestamp": datetime.utcnow().isoformat(),
                "type": MESSAGE_TYPES["PROGRESS_UPDATE"],
                "data": progress_data
            }
            
            await self.producer.send_and_wait(
                topic=get_topic_name("JOB_PROGRESS"),
                key=job_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8')
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send progress update for job {job_id}: {e}")
            return False
    
    async def send_job_status(self, job_id: str, status: str, data: Dict[str, Any] = None) -> bool:
        """Send job status update to job-status topic."""
        try:
            await self.start()
            
            message = {
                "job_id": job_id,
                "timestamp": datetime.utcnow().isoformat(),
                "type": "status_update",
                "status": status,
                "data": data or {}
            }
            
            await self.producer.send_and_wait(
                topic=get_topic_name("JOB_STATUS"),
                key=job_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8')
            )
            
            logger.info(f"Job {job_id} status update sent: {status}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send status update for job {job_id}: {e}")
            return False
    
    async def send_company_processed(self, job_id: str, company_data: Dict[str, Any]) -> bool:
        """Send individual company processing result."""
        try:
            await self.start()
            
            message = {
                "job_id": job_id,
                "timestamp": datetime.utcnow().isoformat(),
                "type": MESSAGE_TYPES["COMPANY_PROCESSED"],
                "data": company_data
            }
            
            await self.producer.send_and_wait(
                topic=get_topic_name("JOB_PROGRESS"),
                key=job_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8')
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send company processed for job {job_id}: {e}")
            return False
    
    async def send_job_result(self, job_id: str, result_data: Dict[str, Any]) -> bool:
        """Send final job result to job-results topic."""
        try:
            await self.start()
            
            message = {
                "job_id": job_id,
                "timestamp": datetime.utcnow().isoformat(),
                "type": "job_result",
                "data": result_data
            }
            
            await self.producer.send_and_wait(
                topic=get_topic_name("JOB_RESULTS"),
                key=job_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8')
            )
            
            logger.info(f"Job {job_id} result sent to Kafka")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send job result for job {job_id}: {e}")
            return False

# Global producer instance
kafka_producer = AsyncKafkaProducer()

def get_kafka_producer() -> AsyncKafkaProducer:
    """Get the global Kafka producer instance."""
    return kafka_producer 