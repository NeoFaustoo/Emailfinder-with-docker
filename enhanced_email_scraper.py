#!/usr/bin/env python3
"""
Robust Email Scraper - Production Ready
High-performance email discovery with comprehensive filtering and worker management
"""

import asyncio
import aiohttp
import re
import time
import json
import html
import unicodedata
import base64
from typing import List, Dict, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse, unquote
from dataclasses import dataclass, asdict
import random
import logging
from collections import defaultdict
import gc
import weakref

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# User agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

# Compiled regex patterns for maximum performance
class CompiledPatterns:
    """Centralized compiled regex patterns"""
    
    def __init__(self):
        # Main email patterns
        self.email_main = re.compile(r'\b[a-zA-Z0-9._%+-]{1,64}@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b', re.IGNORECASE)
        self.email_enhanced = re.compile(r'(?:email|mail|contact|e-mail|courriel)?\s*([a-zA-Z0-9._%+-]{1,64}@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE)
        self.email_spaced = re.compile(r'([a-zA-Z0-9._%+-]+)\s*@\s*([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE)
        
        # JavaScript and obfuscated patterns
        self.js_concat = re.compile(r'["\']([a-zA-Z0-9._%+-]+)["\']\s*\+\s*["\']@["\']\s*\+\s*["\']([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})["\']', re.IGNORECASE)
        self.mailto = re.compile(r'mailto:\s*["\']?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})["\']?', re.IGNORECASE)
        self.email_with_entities = re.compile(r'([a-zA-Z0-9._%+-]+&#64;[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE)
        
        # Base64 and encoding patterns
        self.base64 = re.compile(r'[A-Za-z0-9+/]{20,}={0,2}')
        self.hex_encoded = re.compile(r'\\x[0-9a-fA-F]{2}', re.IGNORECASE)
        self.unicode_encoded = re.compile(r'\\u[0-9a-fA-F]{4}', re.IGNORECASE)
        
        # Domain validation
        self.domain_valid = re.compile(r'^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        self.ip_address = re.compile(r'@[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$')
        
        # Comprehensive fake/spam patterns (non-redundant)
        self.fake_patterns = [
            # Sentry/Wix/Platform spam
            re.compile(r'.*@sentry.*\.wix.*', re.IGNORECASE),
            re.compile(r'.*@.*\.wixpress\.com$', re.IGNORECASE),
            re.compile(r'.*@fragel.*\.wix.*', re.IGNORECASE),
            re.compile(r'.*@.*\.herokuapp\.com$', re.IGNORECASE),
            re.compile(r'.*@.*\.vercel\.app$', re.IGNORECASE),
            
            # Long hex/number patterns (generated emails)
            re.compile(r'^[a-f0-9]{16,}@', re.IGNORECASE),
            re.compile(r'^[0-9a-z]{20,}@', re.IGNORECASE),
            re.compile(r'.*[0-9]{10,}.*@', re.IGNORECASE),
            re.compile(r'^[0-9]{8,}@', re.IGNORECASE),
            
            # Invalid concatenated domains (fixed pattern)
            re.compile(r'.*@.*\.com[a-z]{2,}', re.IGNORECASE),  # like domain.comspam
            re.compile(r'.*@.*\.[a-z]{2,}[0-9]{2,}$', re.IGNORECASE),  # like domain.com123
            
            # System/automated emails
            re.compile(r'.*noreply.*', re.IGNORECASE),
            re.compile(r'.*no-reply.*', re.IGNORECASE),
            re.compile(r'.*ne-pas-repondre.*', re.IGNORECASE),
            re.compile(r'.*mailer-daemon.*', re.IGNORECASE),
            re.compile(r'.*postmaster.*', re.IGNORECASE),
            re.compile(r'.*webmaster.*', re.IGNORECASE),
            
            # Test/example domains
            re.compile(r'.*@example\.com$', re.IGNORECASE),
            re.compile(r'.*@test\.', re.IGNORECASE),
            re.compile(r'.*@localhost', re.IGNORECASE),
            re.compile(r'.*@.*\.test$', re.IGNORECASE),
            re.compile(r'.*@.*\.local$', re.IGNORECASE),
            
            # File extensions as domains
            re.compile(r'.*@.*\.(png|jpg|jpeg|gif|svg|pdf|doc|txt|css|js|html|xml)$', re.IGNORECASE),
            
            # Social media notifications
            re.compile(r'.*@.*facebook.*', re.IGNORECASE),
            re.compile(r'.*@.*twitter.*', re.IGNORECASE),
            re.compile(r'.*@.*linkedin.*', re.IGNORECASE),
            re.compile(r'.*@.*instagram.*', re.IGNORECASE),
            
            # Privacy/legal patterns
            re.compile(r'.*privacy.*@', re.IGNORECASE),
            re.compile(r'.*legal.*@', re.IGNORECASE),
            re.compile(r'.*abuse.*@', re.IGNORECASE),
            
            # Suspicious character patterns
            re.compile(r'^[._-]+@', re.IGNORECASE),
            re.compile(r'@[._-]+', re.IGNORECASE),
            re.compile(r'.*\.{3,}.*@', re.IGNORECASE),
            
            # UUID-like patterns
            re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}@', re.IGNORECASE),
        ]
        
        # French business email patterns (whitelist)
        self.french_business = re.compile(r'^(?:contact|info|commercial|vente|ventes|direction|accueil|secretariat|administration|rh|ressources-humaines|communication|marketing|service-client|support|technique|comptabilite|finance|juridique)@', re.IGNORECASE)

# Global patterns instance
PATTERNS = CompiledPatterns()

@dataclass
class EmailResult:
    """Enhanced result data structure"""
    company_name: str
    domain: Optional[str]
    website: str
    emails: List[str]
    success: bool
    processing_time: float
    pages_accessed: List[str]
    error: Optional[str] = None
    extraction_stats: Dict[str, int] = None
    
    def __post_init__(self):
        if self.extraction_stats is None:
            self.extraction_stats = {}

class WorkerManager:
    """Advanced worker management with proper future tracking"""
    
    def __init__(self, max_workers: int = 300):
        self.max_workers = min(max_workers, 300)  # Cap at 300
        self.semaphore = asyncio.Semaphore(self.max_workers)
        self.active_tasks: Set[asyncio.Task] = set()
        self.completed_count = 0
        self.failed_count = 0
        
    async def submit_task(self, coro):
        """Submit task with proper tracking"""
        async with self.semaphore:
            task = asyncio.create_task(coro)
            self.active_tasks.add(task)
            
            try:
                result = await task
                self.completed_count += 1
                return result
            except Exception as e:
                self.failed_count += 1
                logger.error(f"Task failed: {e}")
                raise
            finally:
                self.active_tasks.discard(task)
    
    async def process_batch(self, coroutines):
        """Process batch of coroutines with proper cleanup"""
        if not coroutines:
            return []
        
        # Create tasks with tracking
        tasks = []
        for coro in coroutines:
            task = asyncio.create_task(self.submit_task(coro))
            tasks.append(task)
            self.active_tasks.add(task)
        
        try:
            # Wait for all tasks with timeout
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results and exceptions
            processed_results = []
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Task exception: {result}")
                    processed_results.append(None)
                else:
                    processed_results.append(result)
            
            return processed_results
        
        finally:
            # Cleanup tasks
            for task in tasks:
                self.active_tasks.discard(task)
                if not task.done():
                    task.cancel()
            
            # Force garbage collection for large batches
            if len(tasks) > 50:
                gc.collect()
    
    async def cleanup(self):
        """Cleanup all remaining tasks"""
        if self.active_tasks:
            logger.info(f"Cleaning up {len(self.active_tasks)} remaining tasks")
            for task in list(self.active_tasks):
                if not task.done():
                    task.cancel()
            
            # Wait for cancellation
            if self.active_tasks:
                await asyncio.gather(*self.active_tasks, return_exceptions=True)
            
            self.active_tasks.clear()

class SessionManager:
    """Advanced aiohttp session management with connection pooling"""
    
    def __init__(self, max_connections: int = 500, max_connections_per_host: int = 100):
        self.max_connections = max_connections
        self.max_connections_per_host = max_connections_per_host
        self.session: Optional[aiohttp.ClientSession] = None
        self.connector: Optional[aiohttp.TCPConnector] = None
        
    async def initialize(self):
        """Initialize session with optimized settings"""
        if self.session is None or self.session.closed:
            # Create optimized connector
            self.connector = aiohttp.TCPConnector(
                limit=self.max_connections,
                limit_per_host=self.max_connections_per_host,
                ttl_dns_cache=300,
                use_dns_cache=True,
                ssl=False,
                enable_cleanup_closed=True,
                keepalive_timeout=30
            )
            
            # Create session with timeout
            timeout = aiohttp.ClientTimeout(total=8.0, connect=3.0)
            self.session = aiohttp.ClientSession(
                connector=self.connector,
                timeout=timeout,
                headers={
                    'User-Agent': random.choice(USER_AGENTS),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5,fr;q=0.3',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
            )
    
    async def fetch_page_content(self, url: str, max_retries: int = 2) -> Tuple[Optional[str], List[str], bool]:
        """Fetch complete page content with comprehensive error handling"""
        if not self.session:
            await self.initialize()
        
        errors = []
        
        for attempt in range(max_retries + 1):
            try:
                # Progressive timeout
                timeout = aiohttp.ClientTimeout(total=5 + attempt * 2)
                
                async with self.session.get(url, timeout=timeout, ssl=False, allow_redirects=True) as response:
                    if response.status == 200:
                        # Check content type
                        content_type = response.headers.get('content-type', '').lower()
                        if any(ct in content_type for ct in ['text/html', 'text/plain', 'application/xhtml', 'text/']):
                            
                            # Read content completely with size limit
                            content_chunks = []
                            content_size = 0
                            max_size = 10 * 1024 * 1024  # 10MB limit
                            
                            async for chunk in response.content.iter_chunked(8192):
                                if chunk:
                                    content_chunks.append(chunk)
                                    content_size += len(chunk)
                                    if content_size > max_size:
                                        errors.append("content_too_large")
                                        break
                            
                            if content_size <= max_size:
                                try:
                                    # Decode content with fallback encodings
                                    full_content = b''.join(content_chunks)
                                    
                                    # Try UTF-8 first, then fallback
                                    try:
                                        content = full_content.decode('utf-8')
                                    except UnicodeDecodeError:
                                        content = full_content.decode('latin1', errors='ignore')
                                    
                                    return content, errors, True
                                    
                                except Exception as decode_error:
                                    errors.append(f"decode_error: {decode_error}")
                        else:
                            errors.append(f"invalid_content_type: {content_type}")
                    
                    elif response.status in [403, 429]:
                        # Rate limiting or forbidden - try with different UA
                        if attempt < max_retries:
                            headers = {'User-Agent': random.choice(USER_AGENTS)}
                            async with self.session.get(url, headers=headers, timeout=timeout, ssl=False) as retry_response:
                                if retry_response.status == 200:
                                    content = await retry_response.text()
                                    return content, errors, True
                        errors.append(f"http_{response.status}")
                    
                    else:
                        errors.append(f"http_{response.status}")
            
            except asyncio.TimeoutError:
                errors.append("timeout")
                if attempt < max_retries:
                    await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
            
            except aiohttp.ClientError as e:
                errors.append(f"client_error: {type(e).__name__}")
                if attempt < max_retries:
                    await asyncio.sleep(0.2)
            
            except Exception as e:
                errors.append(f"unexpected_error: {type(e).__name__}")
                break
        
        return None, errors, False
    
    async def close(self):
        """Close session and connector"""
        if self.session and not self.session.closed:
            await self.session.close()
        if self.connector:
            await self.connector.close()

class EmailExtractor:
    """Advanced email extraction with comprehensive pattern matching"""
    
    @staticmethod
    def decode_obfuscated_content(content: str) -> str:
        """Decode various obfuscation methods"""
        decoded = content
        
        try:
            # HTML entity decoding
            decoded = html.unescape(decoded)
            
            # Unicode normalization
            decoded = unicodedata.normalize('NFKD', decoded)
            
            # Hex decoding
            if '\\x' in decoded:
                try:
                    decoded = decoded.encode().decode('unicode_escape')
                except:
                    pass
            
            # Unicode decoding
            if '\\u' in decoded:
                try:
                    decoded = decoded.encode().decode('unicode_escape')
                except:
                    pass
        
        except Exception:
            pass
        
        return decoded
    
    @staticmethod
    def is_valid_business_email(email: str, company_domain: str = None) -> bool:
        """Enhanced business email validation - only accepts company domain emails"""
        if not email or '@' not in email:
            return False
        
        email = email.strip().lower()
        
        # Basic format check
        if not PATTERNS.email_main.match(email):
            return False
        
        try:
            local_part, email_domain = email.split('@', 1)
        except ValueError:
            return False
        
        # STRICT DOMAIN MATCHING: Email must be from company domain
        if company_domain:
            company_domain = company_domain.lower().strip()
            # Remove www. prefix from company domain
            company_domain = re.sub(r'^www\.', '', company_domain)
            
            # Email domain must exactly match company domain or be a subdomain
            if email_domain != company_domain and not email_domain.endswith('.' + company_domain):
                return False
        
        # Apply fake pattern filters (always check)
        for fake_pattern in PATTERNS.fake_patterns:
            if fake_pattern.match(email):
                return False
        
        # Domain validation
        if not PATTERNS.domain_valid.match(email_domain):
            return False
        
        # Local part validation
        if len(local_part) < 2 or len(local_part) > 30:
            return False
        
        # Check for French business emails (priority whitelist)
        is_french_business = PATTERNS.french_business.match(email) is not None
        
        # Additional checks for non-business emails
        if not is_french_business:
            # Too many numbers check (max 60% numbers)
            number_count = sum(c.isdigit() for c in local_part)
            if number_count > len(local_part) * 0.6:
                return False
            
            # Must not be all numbers
            if local_part.isdigit():
                return False
            
            # Must not start with numbers only
            if len(local_part) > 2 and local_part[:3].isdigit():
                return False
        
        # IP address domain check
        if PATTERNS.ip_address.search(email):
            return False
        
        return True
    
    @staticmethod
    def is_valid_email(email: str) -> bool:
        """Legacy method - use is_valid_business_email instead"""
        return EmailExtractor.is_valid_business_email(email)
    
    @staticmethod
    def extract_emails_from_content(content: str, company_domain: str = None) -> Tuple[List[str], Dict[str, int]]:
        """Extract emails from content with comprehensive pattern matching"""
        if not content:
            return [], {}
        
        all_emails = set()
        stats = defaultdict(int)
        
        # Decode obfuscated content
        decoded_content = EmailExtractor.decode_obfuscated_content(content)
        stats['content_decoded'] = 1 if decoded_content != content else 0
        
        # Apply all email patterns
        patterns_to_check = [
            ('main', PATTERNS.email_main),
            ('enhanced', PATTERNS.email_enhanced),
            ('spaced', PATTERNS.email_spaced),
            ('js_concat', PATTERNS.js_concat),
            ('mailto', PATTERNS.mailto),
            ('entities', PATTERNS.email_with_entities)
        ]
        
        for pattern_name, pattern in patterns_to_check:
            matches = pattern.findall(decoded_content)
            if matches:
                stats[f'pattern_{pattern_name}'] = len(matches)
                
                for match in matches:
                    if isinstance(match, tuple):
                        if len(match) == 2:
                            # Spaced email pattern
                            email = f"{match[0]}@{match[1]}"
                            all_emails.add(email.lower())
                        else:
                            all_emails.update([m.lower() for m in match if '@' in str(m)])
                    else:
                        # Handle entity-encoded emails
                        if '&#64;' in str(match):
                            email = str(match).replace('&#64;', '@')
                            all_emails.add(email.lower())
                        else:
                            all_emails.add(str(match).lower())
        
        # Base64 decoding with validation
        base64_matches = PATTERNS.base64.findall(decoded_content)
        stats['base64_found'] = len(base64_matches)
        
        for b64_str in base64_matches[:10]:  # Limit for performance
            try:
                decoded = base64.b64decode(b64_str + '==').decode('utf-8', errors='ignore')
                if '@' in decoded and '.' in decoded:
                    b64_emails = PATTERNS.email_main.findall(decoded)
                    if b64_emails:
                        stats['base64_decoded'] += 1
                        all_emails.update([e.lower() for e in b64_emails])
            except:
                continue
        
        # Validate all emails with domain matching
        valid_emails = []
        for email in all_emails:
            if EmailExtractor.is_valid_business_email(email, company_domain):
                valid_emails.append(email)
        
        stats['raw_emails'] = len(all_emails)
        stats['valid_emails'] = len(valid_emails)
        
        return list(set(valid_emails)), dict(stats)

class EmailScraper:
    """High-performance email scraper with advanced worker management"""
    
    def __init__(self, max_workers: int = 300):
        self.max_workers = min(max_workers, 300)
        self.session_manager = SessionManager()
        self.worker_manager = WorkerManager(max_workers)
        self.domain_email_map: Dict[str, List[str]] = {}
        self.processing_stats = {
            'total_processed': 0,
            'successful': 0,
            'total_emails': 0,
            'start_time': None
        }
        
    async def __aenter__(self):
        await self.session_manager.initialize()
        self.processing_stats['start_time'] = time.time()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.worker_manager.cleanup()
        await self.session_manager.close()
    
    def clean_domain(self, website: str) -> Optional[str]:
        """Extract and validate domain from website"""
        if not website:
            return None
        
        website = website.strip().lower()
        
        # Remove common prefixes
        if website.startswith(('http://', 'https://')):
            pass
        elif website.startswith('www.'):
            website = 'https://' + website
        else:
            website = 'https://' + website
        
        try:
            parsed = urlparse(website)
            domain = parsed.netloc
            
            # Clean domain
            domain = re.sub(r'^www\.', '', domain)
            domain = re.sub(r':\d+$', '', domain)
            
            # Validate domain
            if PATTERNS.domain_valid.match(domain) and '.' in domain:
                return domain
        except:
            pass
        
        return None
    
    async def scrape_domain_comprehensive(self, domain: str) -> Tuple[List[str], List[str], Dict[str, int]]:
        """Comprehensive domain scraping with multiple page types"""
        start_time = time.time()
        logger.info(f"🔍 WORKER START: Scraping domain {domain}")
        
        all_emails = set()
        pages_accessed = []
        stats = defaultdict(int)
        
        # Priority URLs with French specificity
        urls_to_check = [
            f"https://{domain}",
            f"https://{domain}/contact",
            f"https://{domain}/nous-contacter", 
            f"https://{domain}/contactez-nous",
            f"https://{domain}/about",
            f"https://{domain}/a-propos",
            f"https://{domain}/equipe",
            f"https://{domain}/team",
            f"https://www.{domain}",
            f"https://www.{domain}/contact",
            f"http://{domain}",  # Fallback to HTTP
        ]
        
        logger.info(f"📋 WORKER {domain}: Generated {len(urls_to_check)} URLs to scrape")
        
        # Process URLs concurrently
        async def fetch_and_extract(url):
            logger.debug(f"🌐 WORKER {domain}: Fetching {url}")
            content, errors, success = await self.session_manager.fetch_page_content(url)
            if success and content:
                emails, extraction_stats = EmailExtractor.extract_emails_from_content(content, domain)
                if emails:
                    logger.info(f"📧 WORKER {domain}: Found {len(emails)} emails on {url}: {emails}")
                else:
                    logger.debug(f"❌ WORKER {domain}: No emails found on {url}")
                return url, emails, extraction_stats, True
            else:
                logger.debug(f"🚫 WORKER {domain}: Failed to fetch {url}")
            return url, [], {}, False
        
        # Create coroutines for worker manager
        fetch_coroutines = [fetch_and_extract(url) for url in urls_to_check]
        
        # Process with worker manager
        results = await self.worker_manager.process_batch(fetch_coroutines)
        
        # Collect results
        for result in results:
            if result and len(result) == 4:
                url, emails, extraction_stats, success = result
                if success:
                    pages_accessed.append(url)
                    all_emails.update(emails)
                    
                    # Merge stats
                    for key, value in extraction_stats.items():
                        stats[key] += value
        
        # Store domain mapping
        final_emails = list(all_emails)
        elapsed_time = time.time() - start_time
        
        logger.info(f"✅ WORKER COMPLETE: {domain} - Found {len(final_emails)} emails in {elapsed_time:.2f}s from {len(pages_accessed)} pages")
        if final_emails:
            logger.info(f"📧 WORKER {domain}: Final emails: {final_emails}")
            self.domain_email_map[domain] = final_emails
        
        return final_emails, pages_accessed, dict(stats)
    
    async def process_company(self, company_data: Dict) -> EmailResult:
        """Process single company with comprehensive error handling"""
        start_time = time.time()
        company_name = company_data.get('name', 'Unknown')
        domain = company_data.get('domain', company_data.get('website', ''))
        
        logger.info(f"🏢 PROCESSING: {company_name} | Domain: {domain}")
        
        # Extract company info
        name = (
            company_data.get('name') or 
            company_data.get('company_name') or 
            company_data.get('raw_name') or 
            'Unknown'
        )
        website = (
            company_data.get('website') or 
            company_data.get('domain') or 
            company_data.get('url') or 
            ''
        )
        
        # Clean domain
        domain = self.clean_domain(website)
        
        if not domain:
            return EmailResult(
                company_name=name,
                domain=None,
                website=website,
                emails=[],
                success=False,
                processing_time=time.time() - start_time,
                pages_accessed=[],
                error="Invalid or missing domain"
            )
        
        try:
            # Scrape emails comprehensively
            emails, pages_accessed, extraction_stats = await self.scrape_domain_comprehensive(domain)
            
            # Update stats
            self.processing_stats['total_processed'] += 1
            if emails:
                self.processing_stats['successful'] += 1
                self.processing_stats['total_emails'] += len(emails)
            
            # Log result
            processing_time = time.time() - start_time
            if len(emails) > 0:
                logger.info(f"✅ SUCCESS: {name} - Found {len(emails)} emails in {processing_time:.2f}s: {emails}")
            else:
                logger.warning(f"❌ NO EMAILS: {name} - No emails found after {processing_time:.2f}s")
            
            return EmailResult(
                company_name=name,
                domain=domain,
                website=website,
                emails=emails,
                success=len(emails) > 0,
                processing_time=processing_time,
                pages_accessed=pages_accessed,
                extraction_stats=extraction_stats
            )
        
        except Exception as e:
            logger.error(f"Error processing {name}: {e}")
            return EmailResult(
                company_name=name,
                domain=domain,
                website=website,
                emails=[],
                success=False,
                processing_time=time.time() - start_time,
                pages_accessed=[],
                error=str(e)
            )
    
    async def process_companies_batch(self, companies: List[Dict]) -> List[EmailResult]:
        """Process multiple companies with advanced worker management"""
        if not companies:
            return []
        
        logger.info(f"Processing batch of {len(companies)} companies with {self.max_workers} workers")
        
        # Create coroutines
        process_coroutines = [self.process_company(company) for company in companies]
        
        # Process with worker manager
        results = await self.worker_manager.process_batch(process_coroutines)
        
        # Filter valid results
        valid_results = []
        for result in results:
            if isinstance(result, EmailResult):
                valid_results.append(result)
            else:
                # Create error result for failed processing
                valid_results.append(EmailResult(
                    company_name="Processing Error",
                    domain=None,
                    website="",
                    emails=[],
                    success=False,
                    processing_time=0.0,
                    pages_accessed=[],
                    error="Processing failed"
                ))
        
        return valid_results
    
    def get_stats(self) -> Dict:
        """Get processing statistics"""
        current_time = time.time()
        elapsed = current_time - self.processing_stats['start_time'] if self.processing_stats['start_time'] else 0
        
        return {
            'total_processed': self.processing_stats['total_processed'],
            'successful': self.processing_stats['successful'],
            'success_rate': (self.processing_stats['successful'] / max(self.processing_stats['total_processed'], 1)) * 100,
            'total_emails': self.processing_stats['total_emails'],
            'unique_domains': len(self.domain_email_map),
            'processing_time': elapsed,
            'rate_per_minute': (self.processing_stats['total_processed'] / max(elapsed / 60, 0.1)),
            'active_workers': len(self.worker_manager.active_tasks),
            'worker_completed': self.worker_manager.completed_count,
            'worker_failed': self.worker_manager.failed_count
        }
    
    def save_domain_email_mapping(self, filename: str = "domain_email_mapping.json"):
        """Save domain to email mapping for reference"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.domain_email_map, f, indent=2, ensure_ascii=False)
            logger.info(f"Domain-email mapping saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save mapping: {e}")

# API Interface Functions
async def scrape_single_company(company_data: Dict, max_workers: int = 100) -> Dict:
    """API endpoint for single company"""
    async with EmailScraper(max_workers=max_workers) as scraper:
        result = await scraper.process_company(company_data)
        return asdict(result)

async def scrape_companies_batch(companies: List[Dict], max_workers: int = 300) -> Tuple[List[Dict], Dict]:
    """API endpoint for batch processing with domain mapping"""
    async with EmailScraper(max_workers=max_workers) as scraper:
        results = await scraper.process_companies_batch(companies)
        stats = scraper.get_stats()
        
        # Save domain mapping
        scraper.save_domain_email_mapping()
        
        return [asdict(result) for result in results], stats

def update_input_file_with_emails(input_file: str, results: List[Dict]) -> bool:
    """Update input file by adding emails to corresponding companies"""
    try:
        # Read original file
        original_data = []
        file_ext = input_file.lower().split('.')[-1]
        
        if file_ext == 'json' or file_ext == 'ndjson':
            with open(input_file, 'r', encoding='utf-8') as f:
                if file_ext == 'ndjson':
                    for line in f:
                        if line.strip():
                            original_data.append(json.loads(line.strip()))
                else:
                    original_data = json.load(f)
        
        elif file_ext == 'csv':
            import csv
            with open(input_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                original_data = list(reader)
        
        else:
            logger.error(f"Unsupported file format: {file_ext}")
            return False
        
        # Create lookup dictionary
        results_lookup = {}
        for result in results:
            name = result.get('company_name', '')
            if name:
                results_lookup[name] = result
        
        # Update original data
        updated_count = 0
        for item in original_data:
            # Try different name fields
            name_fields = ['name', 'company_name', 'raw_name', 'business_name']
            company_name = None
            
            for field in name_fields:
                if field in item and item[field]:
                    company_name = item[field]
                    break
            
            if company_name and company_name in results_lookup:
                result = results_lookup[company_name]
                
                # Add email fields
                item['emails_found'] = result['emails']
                item['email_count'] = len(result['emails'])
                item['emails_scraped_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
                item['scraping_success'] = result['success']
                item['pages_accessed'] = result.get('pages_accessed', [])
                item['processing_time'] = result.get('processing_time', 0)
                
                if result['emails']:
                    updated_count += 1
        
        # Write updated file
        if file_ext == 'json':
            with open(input_file, 'w', encoding='utf-8') as f:
                json.dump(original_data, f, indent=2, ensure_ascii=False)
        
        elif file_ext == 'ndjson':
            with open(input_file, 'w', encoding='utf-8') as f:
                for item in original_data:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
        
        elif file_ext == 'csv':
            import csv
            if original_data:
                fieldnames = list(original_data[0].keys())
                with open(input_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(original_data)
        
        logger.info(f"Updated {updated_count} companies with emails in {input_file}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to update file {input_file}: {e}")
        return False

# Synchronous wrappers for easier integration
def scrape_company_sync(company_data: Dict, max_workers: int = 100) -> Dict:
    """Synchronous wrapper for single company"""
    return asyncio.run(scrape_single_company(company_data, max_workers))

def scrape_companies_sync(companies: List[Dict], max_workers: int = 300) -> Tuple[List[Dict], Dict]:
    """Synchronous wrapper for batch processing"""
    return asyncio.run(scrape_companies_batch(companies, max_workers))

def process_file_and_update(input_file: str, max_workers: int = 300, batch_size: int = 50) -> Dict:
    """Process entire file and update it with email results"""
    logger.info(f"Processing file: {input_file}")
    
    try:
        # Load companies from file
        companies = []
        file_ext = input_file.lower().split('.')[-1]
        
        if file_ext == 'json':
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    companies = data
                else:
                    companies = [data]
        
        elif file_ext == 'ndjson':
            with open(input_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        companies.append(json.loads(line.strip()))
        
        elif file_ext == 'csv':
            import csv
            with open(input_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                companies = list(reader)
        
        if not companies:
            return {'error': 'No companies found in file'}
        
        logger.info(f"Found {len(companies)} companies to process")
        
        # Process in batches to manage memory
        all_results = []
        total_stats = {
            'total_processed': 0,
            'successful': 0,
            'total_emails': 0,
            'processing_time': 0
        }
        
        start_time = time.time()
        
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(companies)-1)//batch_size + 1}")
            
            # Process batch
            batch_results, batch_stats = scrape_companies_sync(batch, max_workers)
            all_results.extend(batch_results)
            
            # Update stats
            total_stats['total_processed'] += batch_stats.get('total_processed', 0)
            total_stats['successful'] += batch_stats.get('successful', 0)
            total_stats['total_emails'] += batch_stats.get('total_emails', 0)
            
            # Small delay between batches to prevent overwhelming
            time.sleep(0.1)
        
        total_stats['processing_time'] = time.time() - start_time
        
        # Update original file with results
        update_success = update_input_file_with_emails(input_file, all_results)
        
        logger.info(f"Processing complete. Success rate: {total_stats['successful']}/{total_stats['total_processed']}")
        
        return {
            'success': True,
            'file_updated': update_success,
            'stats': total_stats,
            'results_count': len(all_results)
        }
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        return {'error': str(e), 'success': False}

# Enhanced batch processing with memory management
async def process_large_dataset(companies: List[Dict], max_workers: int = 300, batch_size: int = 100, 
                               progress_callback=None) -> Tuple[List[Dict], Dict]:
    """Process large datasets with memory management and progress tracking"""
    
    all_results = []
    total_stats = {
        'total_processed': 0,
        'successful': 0,
        'total_emails': 0,
        'batches_processed': 0,
        'start_time': time.time()
    }
    
    total_batches = (len(companies) - 1) // batch_size + 1
    logger.info(f"🚀 BATCH PROCESSING START: {len(companies)} companies in {total_batches} batches of {batch_size} with {max_workers} workers")
    
    for i in range(0, len(companies), batch_size):
        batch = companies[i:i + batch_size]
        batch_num = i // batch_size + 1
        batch_start_time = time.time()
        
        # Log batch details
        batch_companies = [comp.get('name', comp.get('company_name', 'Unknown')) for comp in batch]
        logger.info(f"📦 BATCH {batch_num}/{total_batches} START: Processing {len(batch)} companies: {batch_companies[:3]}{'...' if len(batch_companies) > 3 else ''}")
        
        try:
            # Process batch
            async with EmailScraper(max_workers=max_workers) as scraper:
                batch_results = await scraper.process_companies_batch(batch)
                batch_stats = scraper.get_stats()
            
            # Convert to dict format
            batch_results_dict = [asdict(result) for result in batch_results]
            all_results.extend(batch_results_dict)
            
            # Update total stats
            batch_processed = len(batch_results)
            batch_successful = sum(1 for r in batch_results if r.success)
            batch_emails = sum(len(r.emails) for r in batch_results)
            batch_time = time.time() - batch_start_time
            
            total_stats['total_processed'] += batch_processed
            total_stats['successful'] += batch_successful
            total_stats['total_emails'] += batch_emails
            total_stats['batches_processed'] += 1
            
            # Detailed batch completion log
            rate_per_min = (batch_processed / batch_time) * 60 if batch_time > 0 else 0
            logger.info(f"✅ BATCH {batch_num}/{total_batches} COMPLETE: {batch_processed} processed, {batch_successful} successful, {batch_emails} emails found in {batch_time:.1f}s ({rate_per_min:.1f} companies/min)")
            
            # Progress callback
            if progress_callback:
                progress = {
                    'batch': batch_num,
                    'total_batches': total_batches,
                    'processed': total_stats['total_processed'],
                    'total': len(companies),
                    'successful': total_stats['successful'],
                    'emails_found': total_stats['total_emails']
                }
                await progress_callback(progress)
            
            # Memory management
            if batch_num % 5 == 0:  # Every 5 batches
                gc.collect()
                logger.info(f"Memory cleanup after batch {batch_num}")
            
            # Small delay to prevent overwhelming servers
            await asyncio.sleep(0.1)
            
        except Exception as e:
            logger.error(f"Error in batch {batch_num}: {e}")
            continue
    
    total_stats['processing_time'] = time.time() - total_stats['start_time']
    total_stats['rate_per_minute'] = total_stats['total_processed'] / (total_stats['processing_time'] / 60)
    
    logger.info(f"Large dataset processing complete: {total_stats['successful']}/{total_stats['total_processed']} successful")
    
    return all_results, total_stats

# Example usage and testing
async def main():
    """Example usage with comprehensive testing"""
    
    # Test data
    test_companies = [
        {"name": "Google", "website": "google.com"},
        {"name": "Microsoft", "website": "microsoft.com"},
        {"name": "Apple", "website": "apple.com"},
        {"name": "OpenAI", "website": "openai.com"},
        {"name": "Anthropic", "website": "anthropic.com"}
    ]
    
    print("🚀 Advanced Email Scraper Test")
    print("=" * 60)
    
    start_time = time.time()
    
    # Test batch processing
    async with EmailScraper(max_workers=100) as scraper:
        results = await scraper.process_companies_batch(test_companies)
        stats = scraper.get_stats()
    
    total_time = time.time() - start_time
    
    print(f"📊 Results Summary:")
    print(f"   Companies processed: {len(results)}")
    print(f"   Successful: {stats['successful']}/{stats['total_processed']}")
    print(f"   Success rate: {stats['success_rate']:.1f}%")
    print(f"   Total emails found: {stats['total_emails']}")
    print(f"   Processing time: {total_time:.2f}s")
    print(f"   Rate: {stats['rate_per_minute']:.1f} companies/minute")
    print(f"   Worker efficiency: {stats['worker_completed']}/{stats['worker_completed'] + stats['worker_failed']}")
    
    print("\n📧 Detailed Results:")
    for result in results:
        status = "✅" if result.success else "❌"
        emails_str = ", ".join(result.emails[:3]) + ("..." if len(result.emails) > 3 else "")
        print(f"   {status} {result.company_name}: {len(result.emails)} emails")
        if result.emails:
            print(f"      → {emails_str}")
        if result.pages_accessed:
            print(f"      → Pages: {len(result.pages_accessed)}")

def main_sync():
    """Synchronous main for easier testing"""
    asyncio.run(main())

if __name__ == "__main__":
    main_sync()