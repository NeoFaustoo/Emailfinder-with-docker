#!/usr/bin/env python3
"""
Async Email Scraper Worker
Consumes jobs from Kafka and processes them using the enhanced email scraper
"""

import json
import asyncio
import logging
import aiofiles
import pandas as pd
import re
import time
import uuid
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from pathlib import Path
from aiokafka import AIOKafkaConsumer
from kafka_config import get_consumer_config, get_topic_name, JOB_STATUS, MESSAGE_TYPES
from kafka_producer import AsyncKafkaProducer

# Import the enhanced scraper functions
from enhanced_email_scraper import (
    process_single_company_worker,
    get_field_value,
    clean_url,
    discover_emails_for_domain,
    ProcessingResult,
    FileResultWriter
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AsyncEmailWorker:
    """Async worker that consumes jobs from Kafka and processes them using the enhanced scraper."""
    
    def __init__(self, worker_id: str = None):
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer = AsyncKafkaProducer()
        self.running = False
        self.file_writer = FileResultWriter()
        
    async def start(self):
        """Start the worker."""
        logger.info(f"Starting async email worker: {self.worker_id}")
        
        # Start Kafka consumer
        config = get_consumer_config(f"email-worker-{self.worker_id}")
        self.consumer = AIOKafkaConsumer(
            get_topic_name("EMAIL_JOBS"),
            **config
        )
        await self.consumer.start()
        
        # Start producer
        await self.producer.start()
        
        self.running = True
        logger.info(f"Worker {self.worker_id} started successfully")
    
    async def stop(self):
        """Stop the worker."""
        logger.info(f"Stopping worker: {self.worker_id}")
        self.running = False
        
        if self.consumer:
            await self.consumer.stop()
        
        await self.producer.stop()
        logger.info(f"Worker {self.worker_id} stopped")
    
    async def process_job(self, job_data: Dict[str, Any]):
        """Process a single job using the enhanced scraper."""
        job_id = job_data["job_id"]
        files = job_data["files_processed"]
        config = job_data["config"]
        
        logger.info(f"Worker {self.worker_id} processing job {job_id}")
        
        try:
            # Send job started status
            await self.producer.send_job_status(job_id, JOB_STATUS["RUNNING"])
            
            total_emails = 0
            total_processed = 0
            all_results = []
            
            for file_path in files:
                file_emails, file_processed, file_results = await self.process_file(
                    job_id, file_path, config
                )
                total_emails += file_emails
                total_processed += file_processed
                all_results.extend(file_results)
                
                # Send progress update
                progress_data = {
                    "file_processed": file_path,
                    "total_emails": total_emails,
                    "total_processed": total_processed,
                    "progress_percent": (len(files) - files.index(file_path) - 1) / len(files) * 100
                }
                await self.producer.send_progress_update(job_id, progress_data)
            
            # Update the original file with results
            if all_results:
                await self.update_file_with_results(file_path, all_results)
            
            # Send completion
            result_data = {
                "total_emails": total_emails,
                "total_processed": total_processed,
                "files_processed": files,
                "processing_time": time.time() - job_data["start_time"]
            }
            
            await self.producer.send_job_result(job_id, result_data)
            await self.producer.send_job_status(job_id, JOB_STATUS["COMPLETED"])
            
            logger.info(f"Job {job_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Error processing job {job_id}: {e}")
            await self.producer.send_job_status(job_id, JOB_STATUS["FAILED"], {"error": str(e)})
    
    async def process_file(self, job_id: str, file_path: str, config: Dict[str, Any]) -> tuple[int, int, List[Dict]]:
        """Process a single file using the enhanced scraper."""
        logger.info(f"Processing file: {file_path}")
        
        total_emails = 0
        total_processed = 0
        all_results = []
        
        try:
            # Read file based on extension
            companies_data = await self.load_companies_data(file_path)
            
            # Process companies in batches
            batch_size = config.get("batch_size", 500)
            workers = config.get("workers", 150)
            verbose = config.get("verbose", False)
            
            if config.get("limit"):
                companies_data = companies_data[:config["limit"]]
            
            # Process in batches
            for i in range(0, len(companies_data), batch_size):
                batch = companies_data[i:i + batch_size]
                
                # Process batch with concurrency limit
                semaphore = asyncio.Semaphore(workers)
                tasks = []
                
                for company_data in batch:
                    task = self.process_company_with_semaphore(job_id, company_data, semaphore, verbose)
                    tasks.append(task)
                
                # Wait for batch to complete
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Count results and send updates
                for result in results:
                    if isinstance(result, dict) and "emails_found" in result:
                        total_emails += len(result["emails_found"])
                        total_processed += 1
                        all_results.append(result)
                        
                        # Send company processed update
                        await self.producer.send_company_processed(job_id, result)
                        
                        if verbose and result["emails_found"]:
                            logger.info(f"Found {len(result['emails_found'])} emails for {result['name']}")
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            raise
        
        return total_emails, total_processed, all_results
    
    async def load_companies_data(self, file_path: str) -> List[Dict]:
        """Load companies data from various file formats."""
        companies_data = []
        
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
            companies_data = df.to_dict('records')
        elif file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
            companies_data = df.to_dict('records')
        elif file_path.endswith('.ndjson'):
            data = []
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                async for line in f:
                    data.append(json.loads(line.strip()))
            companies_data = data
        else:
            raise ValueError(f"Unsupported file format: {file_path}")
        
        logger.info(f"Loaded {len(companies_data)} companies from {file_path}")
        return companies_data
    
    async def process_company_with_semaphore(self, job_id: str, company_data: dict, semaphore: asyncio.Semaphore, verbose: bool) -> Dict[str, Any]:
        """Process a company with semaphore for concurrency control."""
        async with semaphore:
            return await self.process_company(job_id, company_data, verbose)
    
    async def process_company(self, job_id: str, company_data: dict, verbose: bool) -> Dict[str, Any]:
        """Process a single company using the enhanced scraper."""
        try:
            # Use the enhanced scraper's processing function
            result = process_single_company_worker(
                company_data, 
                verbose=verbose, 
                start_time=time.time()
            )
            
            # Add timestamp for Kafka
            result["timestamp"] = datetime.utcnow().isoformat()
            
            return result
            
        except Exception as e:
            logger.debug(f"Error processing company {company_data.get('name', 'Unknown')}: {e}")
            
            # Extract basic info for error case
            name = get_field_value(company_data, ['name', 'company_name', 'raw_name', 'business_name'])
            website = get_field_value(company_data, ['website', 'domain', 'url', 'website_url', 'site_web'])
            
            return {
                "name": name or "Unknown",
                "domain": None,
                "website": website,
                "city": "",
                "industry": "",
                "emails_found": [],
                "discovery_method": "error",
                "success": False,
                "pages_accessed": [],
                "processing_time": 0.0,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def update_file_with_results(self, file_path: str, results: List[Dict]):
        """Update the original file with email results using the enhanced scraper's file writer."""
        try:
            # Convert results to ProcessingResult objects
            processing_results = []
            for result in results:
                processing_result = ProcessingResult(
                    name=result.get("name", ""),
                    domain=result.get("domain"),
                    website=result.get("website"),
                    city=result.get("city", ""),
                    industry=result.get("industry", ""),
                    emails_found=result.get("emails_found", []),
                    discovery_method=result.get("discovery_method", ""),
                    success=result.get("success", False),
                    pages_accessed=result.get("pages_accessed", []),
                    processing_time=result.get("processing_time", 0.0)
                )
                processing_results.append(processing_result)
            
            # Use the enhanced scraper's file writer
            success = self.file_writer.update_file_with_results(file_path, processing_results)
            
            if success:
                logger.info(f"Updated file with results: {file_path}")
            else:
                logger.error(f"Failed to update file with results: {file_path}")
                
        except Exception as e:
            logger.error(f"Error updating file {file_path}: {e}")
    
    async def run(self):
        """Main worker loop."""
        logger.info(f"Worker {self.worker_id} starting main loop")
        
        try:
            async for message in self.consumer:
                if not self.running:
                    break
                
                try:
                    # Parse job data
                    job_message = json.loads(message.value.decode('utf-8'))
                    job_data = job_message["data"]
                    
                    # Process job
                    await self.process_job(job_data)
                    
                    # Commit offset
                    await self.consumer.commit()
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    # Continue processing other messages
                    
        except Exception as e:
            logger.error(f"Worker {self.worker_id} error: {e}")
        finally:
            await self.stop()

async def main():
    """Main function to run the worker."""
    worker = AsyncEmailWorker()
    
    try:
        await worker.start()
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await worker.stop()

if __name__ == "__main__":
    asyncio.run(main()) 