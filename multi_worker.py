#!/usr/bin/env python3
"""
Multi-Worker Email Processor
Runs multiple workers in a single process for high concurrency
"""

import json
import asyncio
import logging
import time
import uuid
import os
import signal
import sys
from typing import Dict, Any, List
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from kafka_config import get_consumer_config, get_topic_name, JOB_STATUS
from kafka_producer import AsyncKafkaProducer

# Import the enhanced scraper's async functions
from enhanced_email_scraper import (
    process_single_company_worker_async,
    close_aiohttp_session,
    get_aiohttp_session
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmailWorker:
    """Email worker that processes files directly without uploads."""
    
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.consumer: AIOKafkaConsumer = None
        self.producer = AsyncKafkaProducer()
        self.running = False
        
    async def start(self):
        """Start the worker."""
        logger.info(f"Starting email worker: {self.worker_id}")
        
        # Initialize aiohttp session
        await get_aiohttp_session()
        
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
    
    def setup_job_logger(self, job_id: str):
        """Setup separate logger for each job"""
        job_logger = logging.getLogger(f"job_{job_id}")
        job_logger.setLevel(logging.INFO)
        
        # Create job-specific log file
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"job_{job_id}.log")
        
        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        job_logger.addHandler(file_handler)
        
        return job_logger
    
    async def process_file_directly(self, file_path: str, job_id: str, config: Dict[str, Any]):
        """Process file directly without uploads"""
        job_logger = self.setup_job_logger(job_id)
        job_logger.info(f"Starting direct file processing: {file_path}")
        
        try:
            # Read file content
            if not os.path.exists(file_path):
                job_logger.error(f"File not found: {file_path}")
                return False
            
            # Read file based on type
            companies = []
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.ndjson':
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                companies.append(json.loads(line))
                            except json.JSONDecodeError:
                                continue
            
            elif file_ext in ['.xlsx', '.xls']:
                import pandas as pd
                df = pd.read_excel(file_path)
                companies = df.to_dict('records')
            
            elif file_ext == '.csv':
                import pandas as pd
                df = pd.read_csv(file_path)
                companies = df.to_dict('records')
            
            else:
                job_logger.error(f"Unsupported file type: {file_ext}")
                return False
            
            job_logger.info(f"Loaded {len(companies)} companies from {file_path}")
            
            # Process companies in batches
            batch_size = config.get("batch_size", 100)
            workers = config.get("workers", 50)
            verbose = config.get("verbose", True)
            
            total_processed = 0
            total_successes = 0
            total_emails = 0
            job_start_time = time.time()
            
            # Process in batches
            for i in range(0, len(companies), batch_size):
                batch = companies[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                job_logger.info(f"Processing batch {batch_num}: {len(batch)} companies")
                
                # Process batch with semaphore for concurrency control
                semaphore = asyncio.Semaphore(workers)
                
                async def process_company_async(company_data: dict) -> dict:
                    async with semaphore:
                        result = await process_single_company_worker_async(
                            company_data, 
                            verbose=verbose
                        )
                        return result
                
                # Process all companies in batch concurrently
                tasks = [process_company_async(company) for company in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results and update file
                valid_results = []
                for result in results:
                    if isinstance(result, dict):
                        valid_results.append(result)
                        total_processed += 1
                        if result.get('success'):
                            total_successes += 1
                            total_emails += len(result.get('emails_found', []))
                        
                        # Log individual results
                        if verbose and result.get('emails_found'):
                            job_logger.info(f"SUCCESS: {result.get('name')} -> {', '.join(result.get('emails_found', [])[:3])}")
                        elif verbose and not result.get('success'):
                            job_logger.info(f"FAILED: {result.get('name')} - {result.get('discovery_method')}")
                
                # Update file with batch results
                await self.update_file_with_results(file_path, valid_results, file_ext)
                
                # Send detailed progress update with stats
                progress_data = {
                    "total_processed": total_processed,
                    "total_successes": total_successes,
                    "total_emails": total_emails,
                    "batch_completed": batch_num,
                    "batch_size": len(valid_results),
                    "batch_successes": sum(1 for r in valid_results if r.get('success')),
                    "timestamp": datetime.utcnow().isoformat(),
                    "stats": {
                        "success_rate": (total_successes / total_processed * 100) if total_processed > 0 else 0,
                        "emails_per_company": (total_emails / total_successes) if total_successes > 0 else 0,
                        "processing_speed": total_processed / (time.time() - job_start_time) if time.time() > job_start_time else 0
                    }
                }
                await self.producer.send_progress_update(job_id, progress_data)
                
                job_logger.info(f"Batch {batch_num} completed: {len(valid_results)} processed, {sum(1 for r in valid_results if r.get('success'))} successes")
            
            job_logger.info(f"File processing completed: {total_processed} total, {total_successes} successes, {total_emails} emails")
            return True
            
        except Exception as e:
            job_logger.error(f"Error processing file {file_path}: {e}")
            return False
    
    async def update_file_with_results(self, file_path: str, results: list, file_ext: str):
        """Update file with only emails - keep it clean"""
        try:
            if file_ext == '.ndjson':
                # Read original data
                original_data = []
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            original_data.append(json.loads(line))
                
                # Create results lookup
                results_lookup = {result['name']: result for result in results}
                
                # Update data with only emails
                updated_data = []
                for company in original_data:
                    name = company.get('name') or company.get('company_name') or company.get('raw_name', '')
                    
                    if name in results_lookup:
                        result = results_lookup[name]
                        # Add only emails to company data
                        company['emails_found'] = result.get('emails_found', [])
                    
                    updated_data.append(company)
                
                # Write updated data
                with open(file_path, 'w', encoding='utf-8') as f:
                    for company in updated_data:
                        f.write(json.dumps(company, ensure_ascii=False) + '\n')
                
                logger.info(f"Updated file with emails only: {file_path}")
                
            elif file_ext in ['.xlsx', '.xls', '.csv']:
                import pandas as pd
                
                # Read original data
                if file_ext in ['.xlsx', '.xls']:
                    df = pd.read_excel(file_path)
                else:
                    df = pd.read_csv(file_path)
                
                # Create results lookup
                results_lookup = {result['name']: result for result in results}
                
                # Add only emails column
                df['emails_found'] = ''
                
                # Update with only emails
                for idx, row in df.iterrows():
                    name = row.get('name') or row.get('company_name') or row.get('raw_name', '')
                    
                    if name in results_lookup:
                        result = results_lookup[name]
                        df.at[idx, 'emails_found'] = ', '.join(result.get('emails_found', []))
                
                # Write updated data
                if file_ext in ['.xlsx', '.xls']:
                    df.to_excel(file_path, index=False)
                else:
                    df.to_csv(file_path, index=False)
                
                logger.info(f"Updated file with emails only: {file_path}")
                
        except Exception as e:
            logger.error(f"Error updating file {file_path}: {e}")
    
    async def process_job(self, job_data: Dict[str, Any]):
        """Process a single job"""
        job_id = job_data["job_id"]
        
        # Handle both old and new job formats
        if "file_path" in job_data:
            # New direct processing format
            file_path = job_data["file_path"]
            config = job_data["config"]
        elif "files_processed" in job_data:
            # Old upload format - skip for now, we want direct processing only
            logger.warning(f"Job {job_id} uses old upload format, skipping")
            return
        else:
            logger.error(f"Job {job_id} has invalid format: {job_data}")
            return
        
        logger.info(f"Worker {self.worker_id} processing job {job_id}: {file_path}")
        
        try:
            # Send job started status
            await self.producer.send_job_status(job_id, JOB_STATUS["RUNNING"])
            
            # Process file directly
            success = await self.process_file_directly(file_path, job_id, config)
            
            if success:
                # Send completion
                await self.producer.send_job_status(job_id, JOB_STATUS["COMPLETED"])
                logger.info(f"Job {job_id} completed successfully")
            else:
                # Send error
                await self.producer.send_job_status(job_id, JOB_STATUS["FAILED"], {"error": "Processing failed"})
                logger.error(f"Job {job_id} failed")
            
        except Exception as e:
            logger.error(f"Error processing job {job_id}: {e}")
            await self.producer.send_job_status(job_id, JOB_STATUS["FAILED"], {"error": str(e)})
    
    async def run(self):
        """Main worker loop"""
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

class MultiWorkerManager:
    """Manages multiple workers in a single process"""
    
    def __init__(self, num_workers: int = 300):
        self.num_workers = num_workers
        self.workers: List[EmailWorker] = []
        self.running = False
        
    async def start(self):
        """Start all workers"""
        logger.info(f"Starting {self.num_workers} workers...")
        
        # Create workers
        for i in range(self.num_workers):
            worker = EmailWorker(f"worker-{i:03d}")
            self.workers.append(worker)
        
        # Start all workers
        start_tasks = [worker.start() for worker in self.workers]
        await asyncio.gather(*start_tasks)
        
        self.running = True
        logger.info(f"All {self.num_workers} workers started successfully")
    
    async def stop(self):
        """Stop all workers"""
        logger.info(f"Stopping {self.num_workers} workers...")
        self.running = False
        
        # Stop all workers
        stop_tasks = [worker.stop() for worker in self.workers]
        await asyncio.gather(*stop_tasks)
        
        # Close aiohttp session
        await close_aiohttp_session()
        
        logger.info(f"All {self.num_workers} workers stopped")
    
    async def run(self):
        """Run all workers"""
        try:
            await self.start()
            
            # Run all workers concurrently
            run_tasks = [worker.run() for worker in self.workers]
            await asyncio.gather(*run_tasks)
            
        except Exception as e:
            logger.error(f"MultiWorkerManager error: {e}")
        finally:
            await self.stop()

# Signal handler for graceful shutdown
def signal_handler(signum, frame):
    logger.info("Received shutdown signal, stopping workers...")
    sys.exit(0)

async def main():
    """Main function to run multiple workers"""
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Get number of workers from command line or use default
    num_workers = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    
    logger.info(f"Starting Multi-Worker Email Processor with {num_workers} workers")
    
    manager = MultiWorkerManager(num_workers)
    
    try:
        await manager.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await manager.stop()

if __name__ == "__main__":
    asyncio.run(main()) 