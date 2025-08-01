#!/usr/bin/env python3
"""
Kafka Configuration for Async Email Scraper
Defines topics and configuration for job management and progress streaming
"""

import os
from typing import Dict, Any

# Kafka Topics
TOPICS = {
    "EMAIL_JOBS": "email-jobs",           # Job submission queue
    "JOB_PROGRESS": "job-progress",       # Real-time progress updates
    "JOB_RESULTS": "job-results",         # Final job results
    "JOB_STATUS": "job-status"            # Job state changes
}

# Kafka Configuration
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "client_id": "email-scraper-api",
    "acks": "all",  # Wait for all replicas to acknowledge
}

# Consumer Configuration
CONSUMER_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group_id": "email-scraper-workers",
    "auto_offset_reset": "earliest",
    "enable_auto_commit": False,  # Manual commit for better control
    "max_poll_records": 100,
    "session_timeout_ms": 30000,
    "heartbeat_interval_ms": 3000,
}

# Job Status Constants
JOB_STATUS = {
    "PENDING": "pending",
    "RUNNING": "running", 
    "COMPLETED": "completed",
    "FAILED": "failed",
    "CANCELLED": "cancelled"
}

# Message Types for Progress Updates
MESSAGE_TYPES = {
    "JOB_STARTED": "job_started",
    "PROGRESS_UPDATE": "progress_update",
    "COMPANY_PROCESSED": "company_processed",
    "JOB_COMPLETED": "job_completed",
    "JOB_FAILED": "job_failed",
    "ERROR": "error"
}

def get_kafka_config() -> Dict[str, Any]:
    """Get Kafka producer configuration."""
    return KAFKA_CONFIG.copy()

def get_consumer_config(group_id: str = None) -> Dict[str, Any]:
    """Get Kafka consumer configuration."""
    config = CONSUMER_CONFIG.copy()
    if group_id:
        config["group_id"] = group_id
    return config

def get_topic_name(topic_key: str) -> str:
    """Get topic name by key."""
    return TOPICS.get(topic_key, topic_key) 