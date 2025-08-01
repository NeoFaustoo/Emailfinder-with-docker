#!/usr/bin/env python3
"""
Enhanced Email Discovery Script - Optimized for Speed and Accuracy
Works with NDJSON files, Excel (.xlsx, .xls), and CSV files
Comprehensive email extraction with advanced obfuscation detection
Parallel processing and optimized HTTP handling
"""

import os
import json
import csv
import re
import argparse
import glob
import random
import time
from datetime import datetime
from typing import List, Tuple, Dict, Optional, Set
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import defaultdict
import base64
import html
from urllib.parse import urlparse, urljoin, unquote
from queue import Queue
import ssl
import logging
import xml.etree.ElementTree as ET
import hashlib
import unicodedata

# Import required libraries
import requests
from bs4 import BeautifulSoup, Comment
import urllib3
import warnings

# Suppress all warnings for clean output
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# Add pandas for Excel/CSV support
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
    warnings.filterwarnings('ignore', message='Columns .* have mixed types')
    import pandas.errors
    warnings.filterwarnings('ignore', category=pandas.errors.DtypeWarning)
except ImportError:
    print("WARNING: pandas not installed. Excel/CSV support will be disabled.")
    print("Install with: pip install pandas openpyxl")
    PANDAS_AVAILABLE = False
    pd = None

# Global compiled regex patterns for performance
COMPILED_PATTERNS = {}
def compile_patterns():
    """Compile all regex patterns once for better performance."""
    global COMPILED_PATTERNS
    
    COMPILED_PATTERNS.update({
        # Enhanced email pattern with better boundary detection
        'email_main': re.compile(r'\b[a-zA-Z0-9._%+-]{1,64}@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b', re.IGNORECASE),
        
        # Email with word boundaries for concatenated text
        'email_enhanced': re.compile(r'(?:email|mail|contact|e-mail|courriel)?\s*([a-zA-Z0-9._%+-]{1,64}@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE),
        
        # Spaced email pattern
        'email_spaced': re.compile(r'([a-zA-Z0-9._%+-]+)\s*@\s*([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE),
        
        # JavaScript email patterns
        'js_concat': re.compile(r'["\']([a-zA-Z0-9._%+-]+)["\']\s*\+\s*["\']@["\']\s*\+\s*["\']([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})["\']', re.IGNORECASE),
        'js_quotes': re.compile(r'["\']([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})["\']', re.IGNORECASE),
        'mailto': re.compile(r'mailto:\s*["\']?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})["\']?', re.IGNORECASE),
        
        # Obfuscated patterns
        'hex_encoded': re.compile(r'\\x[0-9a-fA-F]{2}', re.IGNORECASE),
        'unicode_encoded': re.compile(r'\\u[0-9a-fA-F]{4}', re.IGNORECASE),
        'rot13_email': re.compile(r'\b[a-zA-Z0-9._%+-]{1,64}@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'),
        
        # Base64 pattern (more specific)
        'base64': re.compile(r'[A-Za-z0-9+/]{20,}={0,2}'),
        
        # Domain validation
        'domain_valid': re.compile(r'^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
        
        # UUID patterns for filtering
        'uuid_32': re.compile(r'^[a-fA-F0-9]{32,}$'),
        'uuid_long': re.compile(r'^[a-fA-F0-9]{30,}$'),
        
        # Invalid patterns
        'ip_address': re.compile(r'@[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$'),
        'fake_hex_pattern': re.compile(r'^[a-f0-9]{16,}@', re.IGNORECASE),
        'fake_long_alnum': re.compile(r'^[0-9a-z]{20,}@', re.IGNORECASE),
        'fake_many_numbers': re.compile(r'.*[0-9]{10,}.*@', re.IGNORECASE),
        'concatenated_domain': re.compile(r'@.*\.[a-z]{2,}[a-z0-9]+$', re.IGNORECASE),
        'wix_spam': re.compile(r'@.*fragel.*\.wix', re.IGNORECASE),
        
        # Domain validation improvements
        'consecutive_dots': re.compile(r'\.\.'),
        'numeric_tld': re.compile(r'\.[0-9]+$'),
        'excessive_numbers': re.compile(r'[0-9]{5,}'),

        # French business email patterns
        'french_business': re.compile(r'^(?:contact|info|commercial|vente|ventes|direction|accueil|secretariat|administration|rh|ressources-humaines|communication|marketing|service-client|support|technique|comptabilite|finance|juridique)@', re.IGNORECASE),

        # ENHANCED MAILTO PATTERNS:
        'mailto_simple': re.compile(r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE),
        'mailto_with_quotes': re.compile(r'mailto:["\']?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})["\']?', re.IGNORECASE),
        'mailto_in_href': re.compile(r'href\s*=\s*["\']mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE),
        
        # ENHANCED URL-BASED PATTERNS:
        'email_in_any_url': re.compile(r'https?://[^"\s<>]*?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE),
        'email_in_src': re.compile(r'src\s*=\s*["\'][^"\']*?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE),
        
        # VISIBLE TEXT PATTERNS (for emails that appear as plain text):
        'email_standalone': re.compile(r'(?:^|[\s\n\r\t>])([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})(?=[\s\n\r\t<]|$)', re.IGNORECASE),
        
        # HTML ENTITY DECODED PATTERNS:
        'email_with_entities': re.compile(r'([a-zA-Z0-9._%+-]+&#64;[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE),
        'email_at_entity': re.compile(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE),
    })

# Initialize patterns
compile_patterns()

# Enhanced User-Agent rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
]

# Social media domains to filter out
SOCIAL_MEDIA_DOMAINS = {
    'facebook.com', 'fb.com', 'instagram.com', 'twitter.com', 'x.com', 
    'linkedin.com', 'youtube.com', 'tiktok.com', 'snapchat.com',
    'pinterest.com', 'whatsapp.com', 'telegram.org', 'discord.com',
    'reddit.com', 'tumblr.com', 'flickr.com', 'vimeo.com', 'dailymotion.com'
}

# Connection pool for HTTP session reuse
class HTTPSessionManager:
    """Manages HTTP sessions with connection pooling and reuse."""
    
    def __init__(self, max_sessions=200):
        self.sessions = Queue()
        self.max_sessions = max_sessions
        self.lock = threading.Lock()
        
        # Pre-populate sessions
        for _ in range(min(10, max_sessions)):
            self.sessions.put(self._create_session())
    
    def _create_session(self):
        """Create a new configured session."""
        session = requests.Session()
        
        # Configure session
        session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'no-cache'
        })
        
        # Configure adapter with connection pooling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=0
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        return session
    
    def get_session(self):
        """Get a session from the pool."""
        try:
            session = self.sessions.get_nowait()
            # Update User-Agent for each request
            session.headers['User-Agent'] = random.choice(USER_AGENTS)
            return session
        except:
            return self._create_session()
    
    def return_session(self, session):
        """Return a session to the pool."""
        if session and self.sessions.qsize() < self.max_sessions:
            try:
                self.sessions.put_nowait(session)
            except:
                session.close()
        elif session:
            session.close()

# Global session manager
session_manager = HTTPSessionManager()

class PerformanceMonitor:
    """Enhanced performance monitoring with detailed analytics."""
    
    def __init__(self, enabled=False):
        self.enabled = enabled
        if not enabled:
            return
            
        self.start_time = time.time()
        self.processing_times = []
        self.success_count = 0
        self.total_count = 0
        self.method_stats = defaultdict(int)
        self.domain_stats = defaultdict(lambda: {'times': [], 'pages_accessed': set(), 'attempts': 0, 'emails_found': 0})
        self.not_found_domains = []
        self.no_domain_cases = []
        self.obfuscated_patterns = set()
        self.error_stats = defaultdict(int)
        self.page_type_stats = defaultdict(int)
        
    def record_company_processing(self, company_name, domain, website, processing_time, emails_found, method, pages_accessed=None, errors=None):
        """Record comprehensive processing statistics."""
        if not self.enabled:
            return
            
        self.total_count += 1
        email_count = len(emails_found) if emails_found else 0
        
        self.processing_times.append({
            'name': company_name,
            'domain': domain,
            'website': website,
            'time': processing_time,
            'emails_found': email_count,
            'method': method,
            'success': email_count > 0,
            'pages_accessed': pages_accessed or [],
            'errors': errors or []
        })
        
        if email_count > 0:
            self.success_count += 1
            
        # Track method and domain statistics
        self.method_stats[method] += 1
        
        if domain:
            stats = self.domain_stats[domain]
            stats['times'].append(processing_time)
            stats['attempts'] += 1
            stats['emails_found'] += email_count
            if pages_accessed:
                stats['pages_accessed'].update(pages_accessed)
                
        # Track error statistics
        if errors:
            for error in errors:
                self.error_stats[error] += 1
                
        # Track investigation cases
        if method == 'not_found' and domain:
            self.not_found_domains.append({
                'name': company_name,
                'domain': domain,
                'pages_accessed': pages_accessed or []
            })
        elif method == 'no_domain':
            self.no_domain_cases.append({
                'name': company_name,
                'original_website': website
            })
    
    def record_page_type_success(self, page_type, found_emails):
        """Record which types of pages are most successful."""
        if not self.enabled:
            return
        
        if found_emails:
            self.page_type_stats[f"{page_type}_success"] += 1
        self.page_type_stats[f"{page_type}_total"] += 1
    
    def generate_report(self):
        """Generate comprehensive performance report."""
        if not self.enabled:
            return
        
        try:
            total_time = time.time() - self.start_time
            
            print(f"\n" + "="*80)
            print(f"ENHANCED PERFORMANCE MONITORING REPORT")
            print(f"="*80)
            
            # Overall statistics
            print(f"[OVERALL] Processing completed in {total_time/60:.1f} minutes")
            print(f"[OVERALL] Total companies processed: {self.total_count}")
            
            if self.total_count > 0:
                success_rate = self.success_count / self.total_count * 100
                print(f"[OVERALL] Success rate: {self.success_count}/{self.total_count} ({success_rate:.1f}%)")
                
                if self.processing_times:
                    avg_time = sum(p['time'] for p in self.processing_times) / len(self.processing_times)
                    processing_rate = self.total_count / (total_time / 60)
                    print(f"[OVERALL] Average processing time: {avg_time:.2f}s per company")
                    print(f"[OVERALL] Processing rate: {processing_rate:.1f} companies/minute")
                    
                    # Email discovery statistics
                    total_emails = sum(p['emails_found'] for p in self.processing_times)
                    avg_emails_per_success = total_emails / max(self.success_count, 1)
                    print(f"[OVERALL] Total emails discovered: {total_emails}")
                    print(f"[OVERALL] Average emails per successful company: {avg_emails_per_success:.1f}")
            
            # Method breakdown with efficiency metrics
            print(f"\n[METHODS] Discovery method breakdown:")
            method_items = sorted(self.method_stats.items(), key=lambda x: x[1], reverse=True)
            for method, count in method_items:
                percentage = (count / self.total_count * 100) if self.total_count > 0 else 0
                print(f"   {method:<15} | {count:>4} companies ({percentage:>5.1f}%)")
            
            # Page type effectiveness
            if self.page_type_stats:
                print(f"\n[PAGE_TYPES] Page type effectiveness:")
                page_types = set()
                for key in self.page_type_stats.keys():
                    if key.endswith('_total'):
                        page_types.add(key[:-6])
                
                for page_type in sorted(page_types):
                    total = self.page_type_stats.get(f"{page_type}_total", 0)
                    success = self.page_type_stats.get(f"{page_type}_success", 0)
                    if total > 0:
                        success_rate = (success / total) * 100
                        print(f"   {page_type:<15} | {success:>3}/{total:>3} success ({success_rate:>5.1f}%)")
            
            # Domain performance analysis (top performers)
            if self.domain_stats:
                print(f"\n[PERFORMANCE] Top performing domains:")
                
                # Calculate domain performance metrics
                domain_metrics = []
                for domain, stats in self.domain_stats.items():
                    if stats['times'] and stats['attempts'] > 0:
                        avg_time = sum(stats['times']) / len(stats['times'])
                        success_rate = (stats['emails_found'] > 0) * 100  # Convert boolean to percentage
                        emails_per_attempt = stats['emails_found'] / stats['attempts']
                        domain_metrics.append((domain, avg_time, success_rate, emails_per_attempt, stats['attempts']))
                
                # Sort by emails per attempt (most productive first)
                domain_metrics.sort(key=lambda x: x[3], reverse=True)
                
                print(f"   {'Domain':<25} | {'Avg Time':<8} | {'Success':<7} | {'Emails/Attempt':<13} | {'Attempts'}")
                print(f"   {'-'*25} | {'-'*8} | {'-'*7} | {'-'*13} | {'-'*8}")
                
                for domain, avg_time, success_rate, emails_per_attempt, attempts in domain_metrics[:10]:
                    print(f"   {domain:<25} | {avg_time:>6.2f}s | {success_rate:>5.1f}% | {emails_per_attempt:>11.2f} | {attempts:>8}")
            
            # Error analysis
            if self.error_stats:
                print(f"\n[ERRORS] Error breakdown:")
                error_items = sorted(self.error_stats.items(), key=lambda x: x[1], reverse=True)
                for error, count in error_items[:10]:  # Top 10 errors
                    percentage = (count / self.total_count * 100) if self.total_count > 0 else 0
                    print(f"   {error:<30} | {count:>4} ({percentage:>5.1f}%)")
            
            # Investigation recommendations
            if self.not_found_domains:
                print(f"\n[INVESTIGATION] Domains for manual review ({len(self.not_found_domains)} cases):")
                print(f"   High-priority domains where websites were accessible but no emails found:")
                
                # Sort by domain quality (fewer subdomains = higher priority)
                investigation_cases = sorted(
                    self.not_found_domains[:15], 
                    key=lambda x: x['domain'].count('.')
                )
                
                for i, case in enumerate(investigation_cases, 1):
                    pages_str = ', '.join(case['pages_accessed'][:2]) if case['pages_accessed'] else 'main page only'
                    print(f"   {i:>2}. {case['domain']:<25} | {case['name'][:30]:<30} | Tested: {pages_str}")
                
                if len(self.not_found_domains) > 15:
                    print(f"   ... and {len(self.not_found_domains) - 15} more cases")
            
            # Recommendations
            print(f"\n[RECOMMENDATIONS]")
            if self.total_count > 0:
                if self.success_count / self.total_count < 0.15:
                    print(f"   • Success rate is low ({self.success_count/self.total_count*100:.1f}%) - consider:")
                    print(f"     - Increasing worker count for faster processing")
                    print(f"     - Adding more contact page URL patterns")
                    print(f"     - Implementing JavaScript rendering for dynamic sites")
                
                fastest_method = max(self.method_stats.items(), key=lambda x: x[1])[0] if self.method_stats else None
                if fastest_method:
                    print(f"   • Most successful method: {fastest_method}")
                
                if total_time > 0:
                    rate = self.total_count / (total_time / 60)
                    if rate < 5:
                        print(f"   • Processing rate is slow ({rate:.1f}/min) - consider increasing workers")
                    elif rate > 50:
                        print(f"   • Processing rate is very fast ({rate:.1f}/min) - excellent performance!")
            
            print(f"\n" + "="*80)
            print(f"ENHANCED MONITORING COMPLETE")
            print(f"="*80)
            
        except Exception as e:
            print(f"\n❌ Error generating monitoring report: {e}")

def get_random_user_agent():
    """Get a random User-Agent for requests."""
    return random.choice(USER_AGENTS)

def convert_file_to_ndjson(input_file: str) -> str:
    """Convert Excel/CSV file to NDJSON format with better error handling."""
    if not PANDAS_AVAILABLE:
        raise ImportError("pandas is required for Excel/CSV support. Install with: pip install pandas openpyxl")
    
    file_ext = os.path.splitext(input_file)[1].lower()
    output_file = os.path.splitext(input_file)[0] + '.ndjson'
    
    print(f"Converting {file_ext} file to NDJSON: {input_file} -> {output_file}")
    
    try:
        if file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(input_file)
        elif file_ext == '.csv':
            # Try different encodings for CSV
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
            df = None
            for encoding in encodings:
                try:
                    df = pd.read_csv(input_file, encoding=encoding)
                    print(f"Successfully read CSV with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                raise ValueError("Could not read CSV file with any supported encoding")
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
        
        print(f"Found {len(df)} rows with columns: {list(df.columns)}")
        
        # Convert to NDJSON with better handling
        with open(output_file, 'w', encoding='utf-8') as f:
            for _, row in df.iterrows():
                row_dict = row.to_dict()
                
                # Clean up NaN values and convert types properly
                cleaned_dict = {}
                for key, value in row_dict.items():
                    if pd.isna(value):
                        cleaned_dict[key] = None
                    elif isinstance(value, (int, float)):
                        cleaned_dict[key] = value
                    else:
                        cleaned_dict[key] = str(value).strip() if str(value).strip() else None
                
                f.write(json.dumps(cleaned_dict, ensure_ascii=False) + '\n')
        
        print(f"Conversion complete: {output_file}")
        return output_file
        
    except Exception as e:
        print(f"Error converting file: {e}")
        return None

def clean_url(url: str) -> str:
    """Enhanced URL cleaning with better edge case handling."""
    if not url or not isinstance(url, str):
        return None
    
    try:
        url = url.strip()
        
        # Handle empty or invalid URLs
        if not url or len(url) < 4:
            return None
            
        if '?' in url:
            url = url.split('?')[0]
            
        # Decode URL-encoded characters
        try:
            url = unquote(url)
        except:
            pass
        
        # Remove common prefixes that aren't protocols
        prefixes_to_remove = ['www.', 'http.', 'https.']
        for prefix in prefixes_to_remove:
            if url.startswith(prefix) and not url.startswith('http'):
                url = url[len(prefix):]
        
        # Add protocol if missing
        if not url.startswith(('http://', 'https://')):
            # Check if it looks like a domain
            if '.' in url and not url.startswith('/'):
                url = 'https://' + url
            elif url.startswith('www.'):
                url = 'https://' + url
            else:
                return None
        
        parsed = urlparse(url)
        domain = parsed.netloc
        
        if not domain:
            # Try to extract domain from path if netloc is empty
            if parsed.path and '.' in parsed.path:
                domain = parsed.path.split('/')[0]
            else:
                return None
        
        # Clean domain
        domain = domain.lower()
        domain = re.sub(r'^www\.', '', domain)
        domain = re.sub(r':\d+$', '', domain)  # Remove port
        
        # Filter out social media and invalid domains
        if domain in SOCIAL_MEDIA_DOMAINS:
            return None
            
        # Enhanced domain validation using compiled regex
        if not COMPILED_PATTERNS['domain_valid'].match(domain):
            return None
        
        # Additional checks for suspicious domains
        if domain.count('.') > 3:  # Too many subdomains
            return None
            
        if len(domain) > 253:  # Domain too long
            return None
            
        # Check for IP addresses
        if re.match(r'^\d+\.\d+\.\d+\.\d+$', domain):
            return None
            
        return domain
        
    except Exception:
        return None

def is_valid_email(email: str) -> bool:
    """Enhanced email validation with smart fake email filtering."""
    if not email or not isinstance(email, str):
        return False
    
    email = email.strip().lower()
    
    # Basic @ check
    if '@' not in email or email.count('@') != 1:
        return False
    
    try:
        local_part, domain = email.split('@', 1)
    except ValueError:
        return False
    
    # Check for French business emails (give priority)
    is_french_business = COMPILED_PATTERNS['french_business'].match(email) is not None
    
    # ENHANCED: Detect fake/spam email patterns
    fake_patterns = [
        # Sentry/Wix spam patterns
        r'.*@sentry.*\.wix.*',
        r'.*@.*\.wixpress\.com$',
        r'.*@fragel.*\.wix.*',
        r'.*@fragel\..*',
        
        # Long hex/number patterns (fake generated emails)
        r'^[a-f0-9]{16,}@',  # Long hex strings in local part
        r'^[0-9a-z]{20,}@',  # Very long alphanumeric local parts
        r'.*[0-9]{10,}.*@',  # Contains 10+ consecutive numbers
        
        # Invalid concatenated emails
        r'.*@.*\.com[a-z]+',  # Like .comatelier, .frfacebook
        r'.*@.*\.[a-z]{2,}[a-z0-9]+$',  # Domain.extension+garbage
        
        # Common spam/test patterns
        r'.*noreply.*',
        r'.*no-reply.*',
        r'.*ne-pas-repondre.*',
        r'.*@example\.com$',
        r'.*@test\.',
        r'.*@localhost',
        r'.*mailer-daemon.*',
        
        # File extensions as domains
        r'.*@.*\.(png|jpg|jpeg|gif|svg|pdf|doc|txt|css|js)$',
        
        # Numbers-only or mostly numbers local part
        r'^[0-9]+@',
        

    ]
    
    # Apply strict filtering (even for French business emails for these patterns)
    for pattern in fake_patterns:
        if re.match(pattern, email, re.IGNORECASE):
            return False
    
    # Known invalid domains and TLDs
    invalid_domains = {
        'sentry.io', 'wixpress.com', 'sentry-next.wixpress.com',
        'test.com', 'localhost', 'test.fr', 'test.net', 'example.com',
        'fragel.wixpress.com', 'fragel.io'  # Added specific spam domains
    }
    
    invalid_tlds = {
        'png', 'jpg', 'jpeg', 'gif', 'svg', 'bmp', 'ico', 'webp',
        'pdf', 'doc', 'docx', 'txt', 'csv', 'tmp', 'test', 'localhost', 'internal'
    }
    
    # Check domain and TLD
    domain_lower = domain.lower()
    tld = domain_lower.split('.')[-1] if '.' in domain_lower else ''
    
    if tld in invalid_tlds or domain_lower in invalid_domains:
        return False
    
    # ENHANCED: Check for malformed domains
    # Domain should not contain consecutive dots or end with numbers only
    if '..' in domain_lower or domain_lower.endswith('.'):
        return False
    
    # Domain parts validation
    domain_parts = domain_lower.split('.')
    if len(domain_parts) < 2:
        return False
    
    # TLD should not be only numbers
    if domain_parts[-1].isdigit():
        return False
    
    # Check for UUID-like patterns using compiled regex
    if COMPILED_PATTERNS['uuid_32'].match(local_part) or COMPILED_PATTERNS['uuid_long'].match(local_part):
        return False
    
    # ENHANCED: Local part validation
    if not is_french_business:
        # Additional checks for non-business emails
        
        # Local part too long (suspicious)
        if len(local_part) > 30:
            return False
            
        # Too many numbers in local part (but allow some)
        number_count = sum(c.isdigit() for c in local_part)
        if number_count > len(local_part) * 0.6:  # More than 60% numbers
            return False
        
        # Check for IP address domains
        if COMPILED_PATTERNS['ip_address'].search(email):
            return False
    
    # Basic format validation
    if not COMPILED_PATTERNS['email_main'].match(email):
        return False
    
    # Length checks
    if len(local_part) > 64 or len(local_part) < 2:
        return False
    
    if len(domain_lower) > 253:
        return False
    
    # ENHANCED: Additional suspicious pattern checks
    if not is_french_business:
        # Check for excessive dots
        if local_part.count('.') > 3:
            return False
            
        if local_part.startswith('.') or local_part.endswith('.'):
            return False
            
        if '..' in local_part:  # Consecutive dots
            return False
        
        # Check for suspicious character combinations
        if re.search(r'[0-9]{5,}', local_part):  # 5+ consecutive numbers
            return False
    
    return True

def decode_obfuscated_content(content: str) -> str:
    """Decode various types of obfuscated content with smart filtering."""
    decoded_content = content
    
    try:
        # HTML entity decoding
        decoded_content = html.unescape(decoded_content)
        
        # Unicode normalization
        decoded_content = unicodedata.normalize('NFKD', decoded_content)
        
        # Hex decoding (\x encoded)
        if '\\x' in decoded_content:
            try:
                decoded_content = decoded_content.encode().decode('unicode_escape')
            except:
                pass
        
        # Unicode decoding (\u encoded)
        if '\\u' in decoded_content:
            try:
                decoded_content = decoded_content.encode().decode('unicode_escape')
            except:
                pass
    
        
    except:
        pass
    
    return decoded_content

def fetch_website_content(url: str, max_retries: int = 2) -> Tuple[str, List[str]]:
    """Enhanced website content fetching with session reuse and better error handling."""
    errors = []
    session = None
    
    try:
        session = session_manager.get_session()
        
        for attempt in range(max_retries + 1):
            try:
                # Progressive timeout strategy
                timeout = 2 + (attempt * 0.5)  # 2s, 2.5s, 3s

                
                # Add small delay for retries
                if attempt > 0:
                    delay = random.uniform(0.1, 0.3) * attempt
                    time.sleep(delay)
                
                # Make request with streaming to handle large pages
                response = session.get(
                    url, 
                    timeout=timeout, 
                    verify=False,
                    allow_redirects=True,
                    stream=True
                )
                
                # Check content length to avoid downloading massive files
                content_length = response.headers.get('content-length')
                if content_length and int(content_length) > 10 * 1024 * 1024:  # 10MB limit
                    errors.append(f"content_too_large_{content_length}")
                    return None, errors
                
                # Read content with size limit
                content = ""
                size = 0
                max_size = 5 * 1024 * 1024  # 5MB limit
                
                for chunk in response.iter_content(chunk_size=8192, decode_unicode=True):
                    if chunk:
                        content += chunk
                        size += len(chunk)
                        if size > max_size:
                            errors.append("content_size_exceeded")
                            break
                
                # Check for successful response
                if response.status_code == 200:
                    # Validate content type (more permissive)
                    content_type = response.headers.get('content-type', '').lower()
                    if any(ct in content_type for ct in ['text/html', 'text/plain', 'application/xhtml', 'text/']):
                        return content, errors
                    elif not content_type:  # No content type specified, try anyway
                        if '<html' in content.lower() or '<!doctype' in content.lower():
                            return content, errors
                
                # Handle redirects manually if needed
                elif response.status_code in [301, 302, 303, 307, 308]:
                    redirect_url = response.headers.get('location')
                    if redirect_url:
                        if not redirect_url.startswith('http'):
                            redirect_url = urljoin(url, redirect_url)
                        # Avoid infinite redirects
                        if redirect_url != url:
                            return fetch_website_content(redirect_url, max_retries=0)
                
                # Rate limiting or forbidden
                elif response.status_code in [403, 429]:
                    errors.append(f"http_{response.status_code}")
                    if attempt < max_retries:
                        time.sleep(random.uniform(0.5, 1.0))
                        continue
                
                else:
                    errors.append(f"http_{response.status_code}")
                    
            except requests.exceptions.SSLError:
                errors.append("ssl_error")
                # Try HTTP if HTTPS failed
                if url.startswith('https://'):
                    http_url = url.replace('https://', 'http://')
                    try:
                        response = session.get(http_url, timeout=timeout, verify=False)
                        if response.status_code == 200:
                            return response.text, errors
                    except:
                        pass
                        
            except requests.exceptions.Timeout:
                errors.append("timeout")
                continue
                
            except requests.exceptions.ConnectionError:
                errors.append("connection_error")
                continue
                
            except requests.exceptions.RequestException as e:
                errors.append(f"request_error_{type(e).__name__}")
                continue
                
            except Exception as e:
                errors.append(f"unexpected_error_{type(e).__name__}")
                continue
                
    finally:
        if session:
            session_manager.return_session(session)
    
    return None, errors

def extract_emails_from_html(html_content: str, domain: str = None) -> Tuple[List[str], Dict[str, int]]:
    """Comprehensive single-pass email extraction with advanced obfuscation detection."""
    if not html_content:
        return [], {}
    compile_patterns()
    all_emails = set()
    extraction_stats = defaultdict(int)
    
    # Decode obfuscated content first
    decoded_content = decode_obfuscated_content(html_content)
    extraction_stats['content_decoded'] = 1 if decoded_content != html_content else 0
    
    # 1. Single-pass regex extraction using compiled patterns
    for pattern_name, pattern in [
        ('main', COMPILED_PATTERNS['email_main']),
        ('enhanced', COMPILED_PATTERNS['email_enhanced']),
        ('spaced', COMPILED_PATTERNS['email_spaced']),
        ('js_concat', COMPILED_PATTERNS['js_concat']),
        ('js_quotes', COMPILED_PATTERNS['js_quotes']),
        ('mailto', COMPILED_PATTERNS['mailto'])
    ]:
        matches = pattern.findall(decoded_content)
        if matches:
            extraction_stats[f'pattern_{pattern_name}'] = len(matches)
            
            for match in matches:
                if isinstance(match, tuple):
                    if len(match) == 2:  # Split email pattern
                        email = f"{match[0]}@{match[1]}"
                        all_emails.add(email.lower())
                    else:
                        all_emails.update([m.lower() for m in match if '@' in m])
                else:
                    all_emails.add(match.lower())
    
    # 2. Base64 decoding with better filtering
    base64_matches = COMPILED_PATTERNS['base64'].findall(decoded_content)
    extraction_stats['base64_found'] = len(base64_matches)
    
    for b64_str in base64_matches[:5]:  # Limit processing for performance
        try:
            decoded = base64.b64decode(b64_str + '==').decode('utf-8', errors='ignore')
            if '@' in decoded and '.' in decoded:
                b64_emails = COMPILED_PATTERNS['email_main'].findall(decoded)
                if b64_emails:
                    extraction_stats['base64_decoded'] += 1
                    all_emails.update([e.lower() for e in b64_emails])
        except:
            continue
    
    # 3. Enhanced BeautifulSoup processing (single parse)
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements to reduce noise
        for element in soup(['script', 'style']):
            element.decompose()
        
        # Extract from mailto links
        mailto_links = soup.find_all('a', href=re.compile(r'^mailto:', re.I))
        if mailto_links:
            extraction_stats['mailto_links'] = len(mailto_links)
            for mailto in mailto_links:
                href = mailto.get('href', '')
                if href.startswith('mailto:'):
                    email = href.replace('mailto:', '').split('?')[0].split('&')[0]
                    all_emails.add(email.lower())
        
        # Enhanced CSS selector targeting
        contact_selectors = [
            '.contact-email, .email, #email, [data-email]',
            '.contact-info, .contact-details, .contact-widget',
            '.footer-email, .email-address, footer',
            '.contact-form, form',
            'header, .nav, .navbar, .navigation',
            '.top-bar, .header-info, .site-header',
            '.widget-contact, .contact-section',
            '[itemprop*="email"], [itemtype*="ContactPoint"]'
        ]
        
        for selector in contact_selectors:
            try:
                elements = soup.select(selector)
                if elements:
                    extraction_stats[f'selector_{selector.split(",")[0].replace(".", "").replace("#", "")}'] = len(elements)
                    
                    for element in elements:
                        # Check text content
                        text = element.get_text(strip=True)
                        if text and '@' in text:
                            text_emails = COMPILED_PATTERNS['email_main'].findall(text)
                            all_emails.update([e.lower() for e in text_emails])
                        
                        # Check all attributes for emails
                        for attr_name, attr_value in element.attrs.items():
                            if attr_value and isinstance(attr_value, str) and '@' in attr_value:
                                attr_emails = COMPILED_PATTERNS['email_main'].findall(attr_value)
                                all_emails.update([e.lower() for e in attr_emails])
            except:
                continue
        
        # JSON-LD structured data with error handling
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        if json_ld_scripts:
            extraction_stats['json_ld_scripts'] = len(json_ld_scripts)
            for script in json_ld_scripts:
                try:
                    if script.string:
                        json_data = json.loads(script.string)
                        json_str = json.dumps(json_data)
                        json_emails = COMPILED_PATTERNS['email_main'].findall(json_str)
                        if json_emails:
                            extraction_stats['json_ld_emails'] += len(json_emails)
                            all_emails.update([e.lower() for e in json_emails])
                except:
                    continue
        
        # Microdata extraction
        microdata_elements = soup.find_all(attrs={'itemprop': re.compile(r'email|contact', re.I)})
        if microdata_elements:
            extraction_stats['microdata_elements'] = len(microdata_elements)
            for element in microdata_elements:
                text = element.get_text(strip=True)
                if text and '@' in text:
                    micro_emails = COMPILED_PATTERNS['email_main'].findall(text)
                    all_emails.update([e.lower() for e in micro_emails])
        
        # Comments processing (sometimes emails are hidden in comments)
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            if '@' in comment:
                comment_emails = COMPILED_PATTERNS['email_main'].findall(comment)
                if comment_emails:
                    extraction_stats['comment_emails'] += len(comment_emails)
                    all_emails.update([e.lower() for e in comment_emails])
        
        # Enhanced French contact section detection
        french_contact_selectors = [
            re.compile(r'contact|coordonnees|nous-contacter|contactez', re.I),
            re.compile(r'equipe|team|staff|direction', re.I),
            re.compile(r'legal|mentions|politique', re.I)  # Legal pages often have contact emails
        ]
        
        for pattern in french_contact_selectors:
            sections = soup.find_all(['div', 'section', 'footer', 'aside'], 
                                   class_=pattern)
            sections.extend(soup.find_all(['div', 'section', 'footer', 'aside'], 
                                        id=pattern))
            
            if sections:
                for section in sections:
                    section_text = section.get_text()
                    if section_text and '@' in section_text:
                        section_emails = COMPILED_PATTERNS['email_main'].findall(section_text)
                        all_emails.update([e.lower() for e in section_emails])
        
    except Exception as e:
        extraction_stats['soup_error'] = 1
    
    # 4. Validate and clean emails
    raw_email_list = list(all_emails)
    extraction_stats['raw_emails_found'] = len(raw_email_list)
    
    # First pass: basic cleaning
    cleaned_emails = clean_extracted_emails(raw_email_list)
    extraction_stats['after_cleaning'] = len(cleaned_emails)
    
    # Second pass: strict validation
    valid_emails = []
    for email in cleaned_emails:
        if is_valid_email(email):
            valid_emails.append(email)
    
    extraction_stats['final_valid'] = len(valid_emails)
    extraction_stats['filtered_out'] = len(raw_email_list) - len(valid_emails)
    
    return valid_emails, dict(extraction_stats)

def get_sitemap_urls(domain: str) -> List[str]:
    """Enhanced sitemap discovery with better error handling."""
    sitemap_urls = set()
    base_url = f"https://{domain}"
    
    # Common sitemap locations (prioritized)
    common_sitemaps = [
        '/sitemap.xml',
        '/sitemap_index.xml',
        '/sitemaps.xml',
        '/sitemap/sitemap.xml'
    ]
    
    # Try robots.txt first (with timeout)
    try:
        session = session_manager.get_session()
        robots_url = f"{base_url}/robots.txt"
        response = session.get(robots_url, timeout=3)
        
        if response.status_code == 200:
            for line in response.text.split('\n'):
                if 'sitemap:' in line.lower():
                    sitemap_url = line.split(':', 1)[1].strip()
                    if sitemap_url:
                        sitemap_urls.add(sitemap_url)
        
        session_manager.return_session(session)
    except:
        pass
    
    # Try common locations (parallel check)
    def check_sitemap_url(sitemap_path):
        try:
            session = session_manager.get_session()
            sitemap_url = urljoin(base_url, sitemap_path)
            response = session.head(sitemap_url, timeout=3)
            session_manager.return_session(session)
            
            if response.status_code == 200:
                return sitemap_url
        except:
            pass
        return None
    
    # Quick parallel check of common locations
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(check_sitemap_url, path) for path in common_sitemaps]
        for future in as_completed(futures, timeout=10):
            try:
                result = future.result()
                if result:
                    sitemap_urls.add(result)
                    break  # Found one, that's enough
            except:
                continue
    
    return list(sitemap_urls)[:2]  # Limit to 2 sitemap URLs

def parse_sitemap(sitemap_url: str) -> List[str]:
    """Enhanced sitemap parsing with better error handling."""
    try:
        session = session_manager.get_session()
        response = session.get(sitemap_url, timeout=5)
        session_manager.return_session(session)
        
        if response.status_code != 200:
            return []
        
        # Parse XML with multiple fallback strategies
        urls = []
        
        try:
            root = ET.fromstring(response.content)
        except ET.ParseError:
            try:
                # Clean content and try again
                content = response.content.decode('utf-8', errors='ignore')
                if content.startswith('\ufeff'):
                    content = content[1:]
                root = ET.fromstring(content.encode('utf-8'))
            except:
                return []
        
        # Handle sitemap index
        if 'sitemapindex' in root.tag:
            for sitemap in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc'):
                if sitemap.text:
                    sub_urls = parse_sitemap(sitemap.text)
                    urls.extend(sub_urls[:5])  # Limit sub-sitemap URLs
        # Handle regular sitemap
        else:
            # Try with namespace
            for url in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc'):
                if url.text:
                    urls.append(url.text)
            
            # If no URLs found, try without namespace
            if not urls:
                for url in root.findall('.//loc'):
                    if url.text:
                        urls.append(url.text)
        
        return urls[:15]  # Limit to 15 URLs
        
    except:
        return []

def get_priority_urls(urls: List[str], domain: str = None) -> List[str]:
    """Enhanced URL prioritization with domain-specific patterns."""
    if not urls:
        return []
    
    # Enhanced priority keywords with scoring
    priority_patterns = {
        # High priority (contact pages)
        'high': [
            'contact', 'nous-contacter', 'contactez-nous', 'contact-us',
            'coordonnees', 'nous-joindre'
        ],
        # Medium priority (about/team pages)
        'medium': [
            'about', 'about-us', 'a-propos', 'qui-sommes-nous',
            'team', 'equipe', 'notre-equipe', 'staff', 'direction'
        ],
        # Low priority (service/legal pages)
        'low': [
            'services', 'prestations', 'offres',
            'legal', 'mentions', 'politique', 'privacy'
        ]
    }
    
    # Categorize URLs with scoring
    url_scores = []
    
    for url in urls:
        url_lower = url.lower()
        score = 0
        category = 'other'
        
        # Check high priority patterns
        for keyword in priority_patterns['high']:
            if keyword in url_lower:
                score += 10
                category = 'high'
                break
        
        if category == 'other':
            # Check medium priority patterns
            for keyword in priority_patterns['medium']:
                if keyword in url_lower:
                    score += 5
                    category = 'medium'
                    break
        
        if category == 'other':
            # Check low priority patterns
            for keyword in priority_patterns['low']:
                if keyword in url_lower:
                    score += 2
                    category = 'low'
                    break
        
        # Bonus for shorter URLs (often more important)
        if len(url.split('/')) <= 4:
            score += 1
        
        # Penalty for very long URLs or those with many parameters
        if '?' in url and len(url.split('?')[1]) > 50:
            score -= 2
        
        url_scores.append((url, score, category))
    
    # Sort by score (highest first)
    url_scores.sort(key=lambda x: x[1], reverse=True)
    
    # Return prioritized URLs with limits
    prioritized = []
    category_limits = {'high': 3, 'medium': 2, 'low': 2, 'other': 1}
    category_counts = defaultdict(int)
    
    for url, score, category in url_scores:
        if category_counts[category] < category_limits[category]:
            prioritized.append(url)
            category_counts[category] += 1
            
        if len(prioritized) >= 8:  # Total limit
            break
    
    return prioritized

def discover_emails_via_sitemap(domain: str) -> Tuple[List[str], List[str], Dict[str, int]]:
    """Enhanced sitemap-based email discovery with comprehensive processing."""
    all_emails = set()
    pages_accessed = []
    stats = defaultdict(int)
    
    # Get sitemap URLs
    sitemap_urls = get_sitemap_urls(domain)
    stats['sitemaps_found'] = len(sitemap_urls)
    
    if not sitemap_urls:
        return [], [], dict(stats)
    
    # Parse sitemaps and collect URLs
    all_urls = []
    for sitemap_url in sitemap_urls:
        urls = parse_sitemap(sitemap_url)
        all_urls.extend(urls)
        stats['sitemap_urls_found'] += len(urls)    
    # Remove duplicates and prioritize
    unique_urls = list(set(all_urls))
    priority_urls = get_priority_urls(unique_urls, domain)
    stats['priority_urls'] = len(priority_urls)
    
    # Process URLs in parallel for better performance
    def process_url(url):
        try:
            content, errors = fetch_website_content(url, max_retries=1)
            if content:
                emails, extraction_stats = extract_emails_from_html(content, domain)
                return url, emails, extraction_stats
        except:
            pass
        return url, [], {}
    
    # Parallel processing of priority URLs
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_url, url) for url in priority_urls]
        
        for future in as_completed(futures, timeout=30):
            try:
                url, emails, extraction_stats = future.result()
                pages_accessed.append(url)
                
                if emails:
                    all_emails.update(emails)
                    stats['successful_pages'] += 1
                    stats['emails_from_sitemap'] += len(emails)
                    
                    # Update extraction stats
                    for key, value in extraction_stats.items():
                        stats[f'extraction_{key}'] += value
                
        
                
            except:
                continue
    
    return list(all_emails), pages_accessed, dict(stats)

def discover_emails_for_domain(domain: str) -> Tuple[List[str], str, List[str], Dict[str, int]]:
    """Enhanced domain email discovery with comprehensive parallel processing."""
    if not domain:
        return [], "no_domain", [], {}
    
    # Initialize all variables at function start to avoid UnboundLocalError
    pages_accessed = []
    stats = defaultdict(int)
    all_emails = set()
    successful_categories = set()
    found_high_priority_emails = False
    
    # Domain variants
    domain_variants = [domain]
    if not domain.startswith('www.'):
        domain_variants.append(f"www.{domain}")
    
    # Enhanced URL patterns with French specificity
    url_patterns = {
        'main': ['', '/'],
        'contact_high': ['/contact', '/nous-contacter', '/contactez-nous', '/contact-us'],
        'contact_medium': ['/contact.html', '/contact.php', '/nous-contacter.html'],
        'about': ['/about', '/a-propos', '/qui-sommes-nous', '/about-us'],
        'team': ['/team', '/equipe', '/notre-equipe', '/staff'],
        'legal': ['/legal', '/mentions-legales', '/politique-confidentialite']
    }
    
    # Function to test a single URL
    def test_url(url_info):
        protocol, domain_variant, path, category = url_info
        url = f"{protocol}://{domain_variant}{path}"
        
        try:
            content, errors = fetch_website_content(url, max_retries=1)
            if content:
                emails, extraction_stats = extract_emails_from_html(content, domain)
                return {
                    'url': url,
                    'emails': emails,
                    'category': category,
                    'extraction_stats': extraction_stats,
                    'success': len(emails) > 0,
                    'errors': errors
                }
        except:
            pass
        
        return {
            'url': url,
            'emails': [],
            'category': category,
            'extraction_stats': {},
            'success': False,
            'errors': ['processing_failed']
        }
    
    # Generate all URL combinations to test
    url_combinations = []
    
    # Start with main pages (both HTTP and HTTPS)
    for protocol in ['https', 'http']:
        for domain_variant in domain_variants:
            for path in url_patterns['main']:
                url_combinations.append((protocol, domain_variant, path, 'main'))
    
    # Add contact pages (HTTPS priority)
    for category, paths in url_patterns.items():
        if category != 'main':
            for domain_variant in domain_variants:
                for path in paths:
                    url_combinations.append(('https', domain_variant, path, category))
    
    # Process URLs in parallel with intelligent early stopping
    found_high_priority_emails = False
    successful_categories = set()
    
   # Smart sequential testing with early stopping
    test_urls = [
        ('https', domain, '/contact', 'contact_high'),
        ('https', domain, '/nous-contacter', 'contact_high'),
        ('https', domain, '', 'main'),
        ('https', f"www.{domain}", '/contact', 'contact_high'),
        ('https', f"www.{domain}", '', 'main'),
        ('http', domain, '', 'main'),
    ]
    
    for protocol, domain_variant, path, category in test_urls:
        if all_emails:
            break  # Stop on first success
            
        url = f"{protocol}://{domain_variant}{path}"
        try:
            content, errors = fetch_website_content(url, max_retries=1)
            if content:
                emails, extraction_stats = extract_emails_from_html(content, domain)
                if emails:
                    all_emails.update(emails)
                    pages_accessed.append(url)
                    successful_categories.add(category)
                    
                    # Add debug info for successful email extraction
                    print(f"DEBUG: Found {len(emails)} emails for {domain} from {url} - {', '.join(emails[:3])}{'...' if len(emails) > 3 else ''}")
                    
                    # Update stats
                    for key, value in extraction_stats.items():
                        stats[f'extraction_{key}'] += value
                    break
                    
            # Track errors
            for error in errors:
                stats[f'error_{error}'] += 1
                
        except Exception as e:
            stats['processing_error'] += 1
            continue
        
    
    
    # If no emails found through direct scraping, try sitemap
    if not all_emails:
        sitemap_emails, sitemap_pages, sitemap_stats = discover_emails_via_sitemap(domain)
        all_emails.update(sitemap_emails)
        pages_accessed.extend(sitemap_pages)
        
        # Merge sitemap stats
        for key, value in sitemap_stats.items():
            stats[f'sitemap_{key}'] += value
        
        if sitemap_emails:
            return list(all_emails), "sitemap", pages_accessed, dict(stats)
    
    # Determine discovery method based on successful categories
    if all_emails:
        if 'main' in successful_categories:
            method = "web_main"
        elif any(cat in successful_categories for cat in ['contact_high', 'contact_medium']):
            method = "web_contact"
        elif 'about' in successful_categories:
            method = "web_about"
        else:
            method = "web_other"
    else:
        method = "not_found"
    
    
    return list(all_emails), method, pages_accessed, dict(stats)

def clean_extracted_emails(emails: List[str]) -> List[str]:
    """Post-process extracted emails to remove concatenated junk."""
    cleaned_emails = []
    
    for email in emails:
        email = email.strip().lower()
        if not email:
            continue
        
        # Fix common concatenation issues
        # Remove everything after the first valid TLD that's followed by non-domain chars
        tld_pattern = r'(\.[a-z]{2,4})([a-z]+)$'
        match = re.search(tld_pattern, email)
        if match:
            # Check if what comes after TLD looks like garbage
            after_tld = match.group(2)
            valid_after_tld = ['com', 'org', 'net', 'edu', 'gov']  # Valid second-level domains
            
            if after_tld not in valid_after_tld and len(after_tld) > 4:
                # Likely garbage, truncate at first TLD
                email = email[:match.start(2)]
        
        # Remove trailing junk patterns
        junk_patterns = [
            r'facebook$',
            r'atelier$', 
            r'contact$',
            r'[0-9]+$',  # Trailing numbers
        ]
        
        for pattern in junk_patterns:
            email = re.sub(pattern, '', email)
        
        # Final validation
        if is_valid_email(email):
            cleaned_emails.append(email)
    
    return list(set(cleaned_emails))  # Remove duplicates

def find_latest_progress_file(output_dir: str) -> tuple:
    """Enhanced progress file discovery with better error handling."""
    progress_files = glob.glob(os.path.join(output_dir, "progress_batch_*.csv"))
    
    if not progress_files:
        return None, 0, set(), 0, 1, []
    
    # Sort by modification time to get the latest
    progress_files.sort(key=os.path.getmtime, reverse=True)
    latest_file = progress_files[0]
    
    print(f"Found latest progress file: {latest_file}")
    
    # Load all progress files efficiently
    all_processed_names = set()
    all_previous_results = []
    total_emails_found = 0
    
    for progress_file in progress_files:
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('name'):
                        all_processed_names.add(row['name'])
                        all_previous_results.append(row)
                        if row.get('emails_found'):
                            # Count emails more efficiently
                            emails = [e.strip() for e in row['emails_found'].split(',') if e.strip()]
                            total_emails_found += len(emails)
        except Exception as e:
            print(f"Warning: Could not read progress file {progress_file}: {e}")
            continue
    
    # Extract batch number
    try:
        filename = os.path.basename(latest_file)
        batch_match = re.search(r'progress_batch_(\d+)_', filename)
        next_batch_number = int(batch_match.group(1)) + 1 if batch_match else 1
    except:
        next_batch_number = 1
    
    companies_count = len(all_processed_names)
    print(f"Resume info: {companies_count} companies processed, {total_emails_found} emails found, next batch: {next_batch_number}")
    
    return latest_file, companies_count, all_processed_names, total_emails_found, next_batch_number, all_previous_results

def save_progress_csv(results, output_dir, batch_number, total_processed):
    """Enhanced progress saving with better data handling."""

    if len(results) < 500 and total_processed % 500 != 0:  # Buffer until 500 results or completion
        return None

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"progress_batch_{batch_number:03d}_{total_processed}_companies_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Write CSV with enhanced error handling
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['name', 'domain', 'website', 'city', 'industry', 'emails_found', 'discovery_method', 'success', 'pages_accessed', 'processing_time']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in results:
                # Enhanced data serialization
                emails_str = ', '.join(result.get('emails_found', [])) if result.get('emails_found') else ''
                pages_str = '; '.join(result.get('pages_accessed', [])) if result.get('pages_accessed') else ''
                
                writer.writerow({
                    'name': result.get('name', ''),
                    'domain': result.get('domain', ''),
                    'website': result.get('website', ''),
                    'city': result.get('city', ''),
                    'industry': result.get('industry', ''),
                    'emails_found': emails_str,
                    'discovery_method': result.get('discovery_method', ''),
                    'success': result.get('success', False),
                    'pages_accessed': pages_str,
                    'processing_time': result.get('processing_time', 0)
                })
        
        print(f"Progress saved: {filepath}")
        return filepath
        
    except Exception as e:
        print(f"Error saving progress: {e}")
        return None

def get_field_value(company_data, field_variants):
    """Enhanced field extraction with better type handling."""
    for variant in field_variants:
        if variant in company_data:
            value = company_data[variant]
            if value and str(value).strip() and str(value).strip().lower() not in ['nan', 'null', 'none']:
                return str(value).strip()
    return None

def process_single_company_worker(company_data, verbose=False, start_time=None, monitor=None):
    """Enhanced single company processing with comprehensive monitoring."""
    processing_start_time = time.time()
    
    # Enhanced field extraction
    name = get_field_value(company_data, ['name', 'company_name', 'raw_name', 'business_name'])
    website = get_field_value(company_data, ['website', 'domain', 'url', 'website_url', 'site_web'])
    city = get_field_value(company_data, ['city', 'ville', 'address', 'location', 'adresse']) or ''
    industry = get_field_value(company_data, ['industry', 'secteur', 'main_category', 'categories', 'category', 'business_type']) or ''
    
    # Handle nested address data
    if not city and company_data.get('detailed_address'):
        city = company_data['detailed_address'].get('city', '')
    
    if verbose and website:
        print(f"DEBUG: Processing {name} with website: '{website}'")

    # Enhanced domain extraction with debug info
    domain = clean_url(website) if website else None

    if verbose:
        if website and not domain:
            print(f"DEBUG: Failed to extract domain from '{website}' for {name} - likely has UTM parameters")
        elif domain:
            print(f"DEBUG: Extracted domain '{domain}' from '{website}' for {name}")
        elif not website:
            print(f"DEBUG: {name} has no website field")
    
    # Process company
    if domain:
        emails, method, pages_accessed, stats = discover_emails_for_domain(domain)
        
        result = {
            'name': name,
            'domain': domain,
            'website': website,
            'city': city,
            'industry': industry,
            'emails_found': emails,
            'discovery_method': method,
            'success': len(emails) > 0,
            'pages_accessed': pages_accessed,
            'processing_time': time.time() - processing_start_time,
            'stats': stats
        }
        
        if emails and verbose:
            current_time = datetime.now().strftime('%H:%M:%S')
            elapsed = ""
            if start_time:
                elapsed_seconds = time.time() - start_time
                elapsed = f" | Elapsed: {elapsed_seconds/60:.1f}min"
            print(f"[{current_time}] SUCCESS: {name} -> {', '.join(emails[:3])}{'...' if len(emails) > 3 else ''}{elapsed}")
        
    else:
        result = {
            'name': name,
            'domain': None,
            'website': website,
            'city': city,
            'industry': industry,
            'emails_found': [],
            'discovery_method': 'no_domain',
            'success': False,
            'pages_accessed': [],
            'processing_time': time.time() - processing_start_time,
            'stats': {}
        }
        
        if verbose:
            if not website:
                print(f"DEBUG: {name} has no website field")
            else:
                print(f"DEBUG: {name} website '{website}' resulted in no_domain")
    
    # Enhanced monitoring
    if monitor:
        processing_time = time.time() - processing_start_time
        errors = []
        if 'stats' in result:
            errors = [key for key, value in result['stats'].items() if key.startswith('error_') and value > 0]
        
        monitor.record_company_processing(
            name, domain, website, processing_time, 
            result['emails_found'], result['discovery_method'], 
            result.get('pages_accessed', []), errors  # Use result data
        )
    
    return result

def process_files(input_files, output_dir="results", workers=150, verbose=False, resume=False, limit=None, max_hours=None, monitor=False):
    """Enhanced file processing with comprehensive optimization and monitoring."""
    start_time = time.time()
    
    # Initialize performance monitor
    performance_monitor = PerformanceMonitor(enabled=monitor)
    
    # Enhanced resume functionality
    processed_names = set()
    all_results = []
    total_emails_found = 0
    batch_number = 1
    
    if resume:
        latest_file, companies_count, processed_names, total_emails_found, batch_number, previous_results = find_latest_progress_file(output_dir)
        if latest_file:
            print(f"✅ RESUMING: {companies_count} companies already processed, {total_emails_found} emails found")
            print(f"✅ Next batch number: {batch_number}")
            
            # Convert previous results to proper format
            for prev_result in previous_results:
                emails_str = prev_result.get('emails_found', '')
                emails_list = [e.strip() for e in emails_str.split(',') if e.strip()] if emails_str else []
                prev_result['emails_found'] = emails_list
                prev_result['success'] = len(emails_list) > 0
                all_results.append(prev_result)
        else:
            print("No previous progress found, starting fresh")
    
    # Enhanced file loading with better error handling
    all_companies = []
    total_loaded = 0
    skipped_processed = 0
    
    print(f"📂 Loading companies from {len(input_files)} file(s)...")
    
    for input_file in input_files:
        print(f"Processing file: {input_file}")
        
        # Convert Excel/CSV to NDJSON if needed
        if input_file.endswith(('.xlsx', '.xls', '.csv')):
            if not PANDAS_AVAILABLE:
                print(f"Skipping {input_file}: pandas not available")
                continue
            ndjson_file = convert_file_to_ndjson(input_file)
            if not ndjson_file:
                print(f"Failed to convert {input_file}")
                continue
            input_file = ndjson_file
        
        # Load NDJSON with enhanced processing
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        line = line.strip()
                        if not line:
                            continue
                            
                        company = json.loads(line)
                        company_name = get_field_value(company, ['name', 'company_name', 'raw_name', 'business_name'])
                        
                        if company_name:
                            total_loaded += 1
                            
                            # Skip if already processed
                            if company_name not in processed_names:
                                all_companies.append(company)
                                
                                # Apply limit if specified
                                if limit and len(all_companies) >= limit:
                                    print(f"🔢 LIMIT REACHED: Processing only {limit} companies")
                                    break
                            else:
                                skipped_processed += 1
                                if verbose:
                                    print(f"SKIP: {company_name} (already processed)")
                                    
                    except json.JSONDecodeError as e:
                        if verbose:
                            print(f"Warning: Invalid JSON on line {line_num} in {input_file}: {e}")
                        continue
                        
                # Break outer loop if limit reached
                if limit and len(all_companies) >= limit:
                    break
                    
        except Exception as e:
            print(f"Error reading {input_file}: {e}")
            continue
    
    total_companies = len(all_companies)
    print(f"📊 LOADED: {total_companies} companies to process")
    print(f"📋 SKIPPED: {skipped_processed} already processed companies")
    
    if total_companies == 0:
        if skipped_processed > 0:
            print("✅ All companies already processed!")
            success_count = len([r for r in all_results if r.get('success', False)])
            success_rate = (success_count / len(processed_names) * 100) if processed_names else 0.0
            print(f"📊 Final stats: {success_count} companies found emails ({success_rate:.1f}% success rate)")
            print(f"📧 Total emails discovered: {total_emails_found}")
        else:
            print("❌ No companies found with valid names")
        return
    
    # Enhanced parallel processing with intelligent batching
    total_processed_count = len(processed_names)
    current_session_count = 0
    current_batch_results = []
    
    print(f"🚀 Starting enhanced processing with {workers} workers...")
    print(f"⚡ Session pool initialized with connection reuse")
    
    # Intelligent worker adjustment based on company count
    effective_workers = min(workers, total_companies, 200)  # Cap at 200 for stability
    
    # Process with enhanced ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        # Submit all tasks with enhanced error handling
        future_to_company = {}
        
        for company in all_companies:
            future = executor.submit(
                process_single_company_worker, 
                company, verbose, start_time, performance_monitor
            )
            future_to_company[future] = company
        
        # Process completed tasks with enhanced monitoring
        completed_count = 0
        last_progress_time = time.time()
        
        for future in as_completed(future_to_company, timeout=max_hours*3600 if max_hours else None):
            try:
                result = future.result()
                current_batch_results.append(result)
                current_session_count += 1
                total_processed_count += 1
                completed_count += 1
                
                # Enhanced progress tracking every 500 companies
                if total_processed_count % 500 == 0:    
                    # Save progress with timing info
                    progress_file = save_progress_csv(current_batch_results, output_dir, batch_number, total_processed_count)
                    
                    # Calculate comprehensive stats
                    batch_emails = sum(len(r.get('emails_found', [])) for r in current_batch_results)
                    total_emails_found += batch_emails
                    batch_success = len([r for r in current_batch_results if r.get('success', False)])
                    
                    # Performance metrics
                    elapsed_time = time.time() - start_time
                    current_rate = current_session_count / elapsed_time * 60 if elapsed_time > 0 else 0
                    remaining = total_companies - current_session_count
                    eta_minutes = remaining / current_rate if current_rate > 0 else 0
                    
                    print(f"📊 BATCH {batch_number} COMPLETE | {batch_success} successes | {batch_emails} emails | ETA: {eta_minutes:.1f}min")
                    
                    # Reset batch
                    all_results.extend(current_batch_results)
                    current_batch_results = []
                    batch_number += 1
                
                # Frequent progress updates every 10 companies
                elif completed_count % 10 == 0:
                    current_time = time.time()
                    time_since_last = current_time - last_progress_time
                    
                    if time_since_last >= 5:  # Every 5 seconds
                        elapsed_time = current_time - start_time
                        current_rate = current_session_count / elapsed_time * 60 if elapsed_time > 0 else 0
                        remaining = total_companies - current_session_count
                        eta_minutes = remaining / current_rate if current_rate > 0 else 0
                        
                        # Quick success count for current batch
                        current_batch_success = len([r for r in current_batch_results if r.get('success', False)])
                        current_batch_emails = sum(len(r.get('emails_found', [])) for r in current_batch_results)
                        
                        print(f"📊 Progress: {current_session_count}/{total_companies} | {current_batch_success} successes | {current_batch_emails} emails | Speed: {current_rate:.1f}/min | ETA: {eta_minutes:.1f}min")
                        last_progress_time = current_time
                
                # Check max hours limit
                if max_hours and (time.time() - start_time) > max_hours * 3600:
                    print(f"\n⏰ Max runtime of {max_hours} hours reached. Stopping gracefully...")
                    # Cancel remaining futures
                    for remaining_future in future_to_company:
                        remaining_future.cancel()
                    break
                
            except Exception as e:
                if verbose:
                    print(f"Error processing company: {e}")
                continue
    
    # Save final batch if any remaining
    if current_batch_results:
        save_progress_csv(current_batch_results, output_dir, batch_number, total_processed_count)
        batch_emails = sum(len(r.get('emails_found', [])) for r in current_batch_results)
        total_emails_found += batch_emails
        all_results.extend(current_batch_results)
    
    # Generate comprehensive final report
    total_time = time.time() - start_time
    success_count = len([r for r in all_results if r.get('success', False)])
    success_rate = (success_count / total_processed_count * 100) if total_processed_count > 0 else 0.0
    
    print(f"\n🎉 === ENHANCED PROCESSING COMPLETE ===")
    print(f"📊 Total companies processed: {total_processed_count}")
    print(f"✅ Companies with emails found: {success_count} ({success_rate:.1f}%)")
    print(f"📧 Total unique emails discovered: {total_emails_found}")
    print(f"⏱️  Total processing time: {total_time/60:.1f} minutes")
    print(f"🚀 Average processing rate: {total_processed_count/(total_time/60):.1f} companies/minute")
    
    # Generate performance monitoring report if enabled
    if monitor:
        performance_monitor.generate_report()
    
    # Generate final consolidated report
    generate_final_report(all_results, output_dir, total_processed_count, total_emails_found, start_time, current_session_count)
    
    print(f"📄 All results saved in: {output_dir}/")


def process_files_with_callback(input_files, output_dir="results", workers=150, verbose=False, resume=False, limit=None, max_hours=None, monitor=False, progress_callback=None, job_id=None):
    """Enhanced file processing with web interface progress callback."""
    start_time = time.time()
    
    # Initialize performance monitor
    performance_monitor = PerformanceMonitor(enabled=monitor)
    
    # Enhanced resume functionality
    processed_names = set()
    all_results = []
    total_emails_found = 0
    batch_number = 1
    
    if resume:
        latest_file, companies_count, processed_names, total_emails_found, batch_number, previous_results = find_latest_progress_file(output_dir)
        if latest_file:
            print(f"✅ RESUMING: {companies_count} companies already processed, {total_emails_found} emails found")
            print(f"✅ Next batch number: {batch_number}")
            
            # Convert previous results to proper format
            for prev_result in previous_results:
                emails_str = prev_result.get('emails_found', '')
                emails_list = [e.strip() for e in emails_str.split(',') if e.strip()] if emails_str else []
                prev_result['emails_found'] = emails_list
                prev_result['success'] = len(emails_list) > 0
                all_results.append(prev_result)
        else:
            print("No previous progress found, starting fresh")
    
    # Enhanced file loading with better error handling
    all_companies = []
    total_loaded = 0
    skipped_processed = 0
    
    print(f"📂 Loading companies from {len(input_files)} file(s)...")
    
    for input_file in input_files:
        print(f"Processing file: {input_file}")
        
        # Convert Excel/CSV to NDJSON if needed
        if input_file.endswith(('.xlsx', '.xls', '.csv')):
            if not PANDAS_AVAILABLE:
                print(f"Skipping {input_file}: pandas not available")
                continue
            ndjson_file = convert_file_to_ndjson(input_file)
            if not ndjson_file:
                print(f"Failed to convert {input_file}")
                continue
            input_file = ndjson_file
        
        # Load NDJSON with enhanced processing
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        line = line.strip()
                        if not line:
                            continue
                            
                        company = json.loads(line)
                        company_name = get_field_value(company, ['name', 'company_name', 'raw_name', 'business_name'])
                        
                        if company_name:
                            total_loaded += 1
                            
                            # Skip if already processed
                            if company_name not in processed_names:
                                all_companies.append(company)
                                
                                # Apply limit if specified
                                if limit and len(all_companies) >= limit:
                                    print(f"🔢 LIMIT REACHED: Processing only {limit} companies")
                                    break
                            else:
                                skipped_processed += 1
                                if verbose:
                                    print(f"SKIP: {company_name} (already processed)")
                                    
                    except json.JSONDecodeError as e:
                        if verbose:
                            print(f"Warning: Invalid JSON on line {line_num} in {input_file}: {e}")
                        continue
                        
                # Break outer loop if limit reached
                if limit and len(all_companies) >= limit:
                    break
                    
        except Exception as e:
            print(f"Error reading {input_file}: {e}")
            continue
    
    total_companies = len(all_companies)
    print(f"📊 LOADED: {total_companies} companies to process")
    print(f"📋 SKIPPED: {skipped_processed} already processed companies")
    
    if total_companies == 0:
        if skipped_processed > 0:
            print("✅ All companies already processed!")
            success_count = len([r for r in all_results if r.get('success', False)])
            success_rate = (success_count / len(processed_names) * 100) if processed_names else 0.0
            print(f"📊 Final stats: {success_count} companies found emails ({success_rate:.1f}% success rate)")
            print(f"📧 Total emails discovered: {total_emails_found}")
        else:
            print("❌ No companies found with valid names")
        return
    
    # Enhanced parallel processing with intelligent batching
    total_processed_count = len(processed_names)
    current_session_count = 0
    current_batch_results = []
    
    print(f"🚀 Starting enhanced processing with {workers} workers...")
    print(f"⚡ Session pool initialized with connection reuse")
    
    # Intelligent worker adjustment based on company count
    effective_workers = min(workers, total_companies, 200)  # Cap at 200 for stability
    
    # Process with enhanced ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        # Submit all tasks with enhanced error handling
        future_to_company = {}
        
        for company in all_companies:
            future = executor.submit(
                process_single_company_worker, 
                company, verbose, start_time, performance_monitor
            )
            future_to_company[future] = company
        
        # Process completed tasks with enhanced monitoring
        completed_count = 0
        last_progress_time = time.time()
        
        for future in as_completed(future_to_company, timeout=max_hours*3600 if max_hours else None):
            try:
                result = future.result()
                current_batch_results.append(result)
                current_session_count += 1
                total_processed_count += 1
                completed_count += 1
                
                # Update emails found count
                if result.get('success', False):
                    total_emails_found += len(result.get('emails_found', []))
                
                # Send progress update to web interface
                if progress_callback:
                    progress_callback(current_session_count, total_companies, total_emails_found)
                
                # Enhanced progress tracking every 500 companies (reduced from 300)
                if current_session_count % 500 == 0:
                    # Save progress with timing info
                    progress_file = save_progress_csv(current_batch_results, output_dir, batch_number, total_processed_count)
                    
                    # Calculate comprehensive stats
                    batch_emails = sum(len(r.get('emails_found', [])) for r in current_batch_results)
                    batch_success = len([r for r in current_batch_results if r.get('success', False)])
                    
                    # Performance metrics
                    elapsed_time = time.time() - start_time
                    current_rate = current_session_count / elapsed_time * 60 if elapsed_time > 0 else 0
                    remaining = total_companies - current_session_count
                    eta_minutes = remaining / current_rate if current_rate > 0 else 0
                    
                    print(f"📊 BATCH {batch_number} COMPLETE | {batch_success} successes | {batch_emails} emails | ETA: {eta_minutes:.1f}min")
                    
                    # Reset batch
                    all_results.extend(current_batch_results)
                    current_batch_results = []
                    batch_number += 1
                
                # Frequent progress updates every 10 companies
                elif completed_count % 10 == 0:
                    current_time = time.time()
                    time_since_last = current_time - last_progress_time
                    
                    if time_since_last >= 5:  # Every 5 seconds
                        elapsed_time = current_time - start_time
                        current_rate = current_session_count / elapsed_time * 60 if elapsed_time > 0 else 0
                        remaining = total_companies - current_session_count
                        eta_minutes = remaining / current_rate if current_rate > 0 else 0
                        
                        # Quick success count for current batch
                        current_batch_success = len([r for r in current_batch_results if r.get('success', False)])
                        current_batch_emails = sum(len(r.get('emails_found', [])) for r in current_batch_results)
                        
                        print(f"📊 Progress: {current_session_count}/{total_companies} | {current_batch_success} successes | {current_batch_emails} emails | Speed: {current_rate:.1f}/min | ETA: {eta_minutes:.1f}min")
                        last_progress_time = current_time
                
                # Check max hours limit
                if max_hours and (time.time() - start_time) > max_hours * 3600:
                    print(f"\n⏰ Max runtime of {max_hours} hours reached. Stopping gracefully...")
                    # Cancel remaining futures
                    for remaining_future in future_to_company:
                        remaining_future.cancel()
                    break
                
            except Exception as e:
                if verbose:
                    print(f"Error processing company: {e}")
                continue
    
    # Save final batch if any remaining
    if current_batch_results:
        save_progress_csv(current_batch_results, output_dir, batch_number, total_processed_count)
        batch_emails = sum(len(r.get('emails_found', [])) for r in current_batch_results)
        all_results.extend(current_batch_results)
    
    # Final progress update
    if progress_callback:
        progress_callback(total_companies, total_companies, total_emails_found)
    
    # Generate comprehensive final report
    total_time = time.time() - start_time
    success_count = len([r for r in all_results if r.get('success', False)])
    success_rate = (success_count / total_processed_count * 100) if total_processed_count > 0 else 0.0
    
    print(f"\n🎉 === ENHANCED PROCESSING COMPLETE ===")
    print(f"📊 Total companies processed: {total_processed_count}")
    print(f"✅ Companies with emails found: {success_count} ({success_rate:.1f}%)")
    print(f"📧 Total unique emails discovered: {total_emails_found}")
    print(f"⏱️  Total processing time: {total_time/60:.1f} minutes")
    print(f"🚀 Average processing rate: {total_processed_count/(total_time/60):.1f} companies/minute")
    
    # Generate performance monitoring report if enabled
    if monitor:
        performance_monitor.generate_report()
    
    # Generate final consolidated report
    generate_final_report(all_results, output_dir, total_processed_count, total_emails_found, start_time, current_session_count)
    
    print(f"📄 All results saved in: {output_dir}/")

def generate_final_report(all_results, output_dir, total_processed_count, total_emails_found, start_time=None, current_session_count=None):
    """Generate comprehensive final report with enhanced analytics."""
    try:
        print(f"\n📋 === GENERATING FINAL REPORT ===")
        
        # Enhanced statistics
        success_count = len([r for r in all_results if r.get('success', False)])
        
        if current_session_count and start_time:
            total_time = time.time() - start_time
            print(f"Current session: {current_session_count} companies in {total_time/60:.1f} minutes")
            print(f"Processing rate: {current_session_count/(total_time/60):.1f} companies/minute")
        
        # Method breakdown with enhanced analytics
        method_stats = defaultdict(int)
        method_emails = defaultdict(int)
        
        for result in all_results:
            method = result.get('discovery_method', 'unknown')
            method_stats[method] += 1
            if result.get('success', False):
                method_emails[method] += len(result.get('emails_found', []))
        
        print(f"\n📈 Method effectiveness breakdown:")
        for method in sorted(method_stats.keys()):
            count = method_stats[method]
            emails = method_emails[method]
            percentage = (count / total_processed_count * 100) if total_processed_count > 0 else 0
            avg_emails = (emails / count) if count > 0 else 0
            print(f"   {method:<15} | {count:>4} companies ({percentage:>5.1f}%) | {emails:>4} emails | {avg_emails:.1f} avg")
        
        # Generate enhanced CSV report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        final_filename = f"FINAL_enhanced_results_{total_processed_count}_companies_{total_emails_found}_emails_{timestamp}.csv"
        final_filepath = os.path.join(output_dir, final_filename)
        
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"💾 Generating comprehensive CSV report...")
        
        with open(final_filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'name', 'domain', 'website', 'city', 'industry', 
                'emails_found', 'email_count', 'discovery_method', 'success', 
                'pages_accessed', 'processing_time'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in all_results:
                emails_list = result.get('emails_found', [])
                if isinstance(emails_list, str):
                    emails_list = [e.strip() for e in emails_list.split(',') if e.strip()]
                
                emails_str = ', '.join(emails_list) if emails_list else ''
                pages_str = '; '.join(result.get('pages_accessed', [])) if result.get('pages_accessed') else ''
                
                writer.writerow({
                    'name': result.get('name', ''),
                    'domain': result.get('domain', ''),
                    'website': result.get('website', ''),
                    'city': result.get('city', ''),
                    'industry': result.get('industry', ''),
                    'emails_found': emails_str,
                    'email_count': len(emails_list),
                    'discovery_method': result.get('discovery_method', ''),
                    'success': result.get('success', False),
                    'pages_accessed': pages_str,
                    'processing_time': result.get('processing_time', 0)
                })
        
        print(f"📄 Enhanced CSV report saved: {final_filepath}")
        
        # Generate emails-only file with deduplication
        print(f"📧 Generating unique emails file...")
        emails_only_filename = f"FINAL_unique_emails_{total_emails_found}_emails_{timestamp}.txt"
        emails_only_filepath = os.path.join(output_dir, emails_only_filename)
        
        all_unique_emails = set()
        email_sources = defaultdict(list)  # Track which companies provided each email
        
        for result in all_results:
            company_name = result.get('name', 'Unknown')
            emails = result.get('emails_found', [])
            
            if isinstance(emails, str):
                emails = [e.strip() for e in emails.split(',') if e.strip()]
            
            for email in emails:
                email = email.strip().lower()
                if email:
                    all_unique_emails.add(email)
                    email_sources[email].append(company_name)
        
        # Write emails with source information
        with open(emails_only_filepath, 'w', encoding='utf-8') as f:
            f.write(f"# Enhanced Email Discovery Report\n")
            f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total Unique Emails: {len(all_unique_emails)}\n")
            f.write(f"# Source Companies: {total_processed_count}\n")
            f.write(f"# Success Rate: {success_count/total_processed_count*100:.1f}%\n\n")
            
            for email in sorted(all_unique_emails):
                sources = email_sources[email]
                if len(sources) == 1:
                    f.write(f"{email}\n")
                else:
                    f.write(f"{email} # Found in {len(sources)} companies\n")
        
        print(f"📧 Unique emails file saved: {emails_only_filepath}")
        print(f"🔢 Total unique emails: {len(all_unique_emails)}")
        
        # Generate summary statistics file
        summary_filename = f"FINAL_summary_stats_{timestamp}.json"
        summary_filepath = os.path.join(output_dir, summary_filename)
        
        summary_stats = {
            'processing_summary': {
                'total_companies_processed': total_processed_count,
                'companies_with_emails': success_count,
                'success_rate_percent': round(success_rate := (success_count/total_processed_count*100) if total_processed_count > 0 else 0, 2),
                'total_emails_found': total_emails_found,
                'unique_emails_found': len(all_unique_emails),
                'processing_time_minutes': round((time.time() - start_time)/60, 2) if start_time else 0
            },
            'method_breakdown': {
                method: {
                    'companies': method_stats[method],
                    'emails': method_emails[method],
                    'percentage': round((method_stats[method]/total_processed_count*100) if total_processed_count > 0 else 0, 2)
                }
                for method in method_stats.keys()
            },
            'top_email_domains': {},
            'report_generated': datetime.now().isoformat()
        }
        
        # Analyze email domains
        domain_counts = defaultdict(int)
        for email in all_unique_emails:
            if '@' in email:
                domain = email.split('@')[1]
                domain_counts[domain] += 1
        
        # Top 10 email domains
        top_domains = dict(sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)[:10])
        summary_stats['top_email_domains'] = top_domains
        
        with open(summary_filepath, 'w', encoding='utf-8') as f:
            json.dump(summary_stats, f, indent=2, ensure_ascii=False)
        
        print(f"📊 Summary statistics saved: {summary_filepath}")
        
        if start_time:
            total_processing_time = time.time() - start_time
            print(f"⏱️  Total processing time: {total_processing_time/60:.1f} minutes")
            
        print(f"✅ Enhanced report generation completed successfully!")
            
    except Exception as e:
        print(f"❌ Error generating enhanced final report: {e}")
        print(f"⚠️  Processing completed but report generation failed")

def main():
    """Enhanced main function with comprehensive argument parsing."""
    parser = argparse.ArgumentParser(
        description='Enhanced Email Discovery Script - Optimized for Speed and Accuracy',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
🚀 ENHANCED FEATURES:
  • Parallel page processing with intelligent prioritization
  • Advanced obfuscation detection (Base64, ROT13, Unicode)
  • Comprehensive French business email patterns
  • Session pooling and connection reuse
  • Smart timeout and retry strategies
  • Enhanced monitoring and analytics

📋 EXAMPLES:
  python enhanced_email_finder.py companies.ndjson --workers 200 --verbose
  python enhanced_email_finder.py companies.xlsx --workers 150 --monitor
  python enhanced_email_finder.py *.csv --resume --workers 180 --verbose
  python enhanced_email_finder.py companies.ndjson --limit 100 --monitor
  python enhanced_email_finder.py companies.xlsx --max-hours 2 --workers 200

📂 SUPPORTED FORMATS: .ndjson, .xlsx, .xls, .csv
💾 AUTO-SAVE: Progress saved every 300 companies for resume
🔍 TESTING: Use --limit for smaller test runs
📊 MONITORING: Use --monitor for detailed performance analytics
        """
    )
    
    parser.add_argument('files', nargs='+', help='Input files (NDJSON, Excel, or CSV)')
    parser.add_argument('--output', '-o', default='results', help='Output directory (default: results)')
    parser.add_argument('--workers', '-w', type=int, default=60, help='Number of worker threads (default: 60, max: 200)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output with debugging')
    parser.add_argument('--resume', '-r', action='store_true', help='Resume from previous progress')
    parser.add_argument('--limit', '-l', type=int, help='Limit number of companies for testing')
    parser.add_argument('--max-hours', type=float, help='Maximum runtime in hours')
    parser.add_argument('--monitor', action='store_true', help='Enable comprehensive performance monitoring')
    
    args = parser.parse_args()
    
    # Enhanced input validation
    valid_files = []
    for file_path in args.files:
        if os.path.exists(file_path):
            valid_files.append(file_path)
        else:
            glob_files = glob.glob(file_path)
            if glob_files:
                valid_files.extend(glob_files)
            else:
                print(f"⚠️  Warning: File not found: {file_path}")
    
    if not valid_files:
        print("❌ Error: No valid input files found")
        return 1
    
    # Validate worker count
    if args.workers > 200:
        print(f"⚠️  Warning: Worker count capped at 200 (requested: {args.workers})")
        args.workers = 200
    
    # Display enhanced startup information
    print(f"🚀 Enhanced Email Discovery Script v2.0")
    print(f"=" * 60)
    print(f"📂 Input files: {len(valid_files)}")
    print(f"⚡ Workers: {args.workers}")
    print(f"📁 Output directory: {args.output}")
    print(f"🔄 Resume mode: {'✅ ENABLED' if args.resume else '❌ DISABLED'}")
    print(f"🔍 Verbose mode: {'✅ ENABLED' if args.verbose else '❌ DISABLED'}")
    print(f"📊 Monitor mode: {'✅ ENABLED' if args.monitor else '❌ DISABLED'}")
    
    if args.limit:
        print(f"🧪 Test mode: LIMIT {args.limit} companies")
    if args.max_hours:
        print(f"⏰ Max runtime: {args.max_hours} hours")
        
    print(f"=" * 60)
    
    try:
        # Initialize global session manager
        print(f"🔧 Initializing HTTP session pool...")
        
        # Start processing
        process_files(
            input_files=valid_files,
            output_dir=args.output,
            workers=args.workers,
            verbose=args.verbose,
            resume=args.resume,
            limit=args.limit,
            max_hours=args.max_hours,
            monitor=args.monitor
        )
        
        print(f"\n🎉 Enhanced email discovery completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        print(f"\n⏹️  Script interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Critical error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())