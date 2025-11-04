"""
Ultra-Fast Deep Forum Scraper Bot
Optimized for maximum speed, accuracy, and deep crawling
Handles forums and threads with proper deduplication
"""

import asyncio
import aiohttp
import re
import os
import sys
import time
import html
import math
import gc
import logging
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse, urlencode
from selectolax.parser import HTMLParser
from datetime import datetime
from typing import Set, List, Tuple, Optional, Dict
from io import BytesIO
from collections import defaultdict, deque
from pyrogram import Client, filters
from pyrogram.types import Message
from fastapi import FastAPI
import uvicorn
import threading
import aiosqlite
import orjson

# ───────────────────────────────
# ⚙️ CONFIG - ALL IMPORTANT VARIABLES
# ───────────────────────────────

# ========== TELEGRAM BOT CREDENTIALS ==========
API_ID = int(os.getenv("API_ID", 24536446))
API_HASH = os.getenv("API_HASH", "baee9dd189e1fd1daf0fb7239f7ae704")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8305440384:AAEBV2b329KaOhto1JVoPsr3JhKpRpEqsJU")
ALLOWED_CHAT_IDS = {5809601894, 1285451259}

# ========== SCRAPING PERFORMANCE SETTINGS ==========
MAX_CONCURRENT_REQUESTS = 50        # How many requests at same time (increase for faster scraping)
MAX_PAGES_TO_SCRAPE = 50000        # Maximum total pages to scrape
MAX_DEPTH_CRAWL = 500              # Maximum forums/threads to visit (increase for deeper scraping)
BASE_REQUEST_DELAY = 0.1           # Delay between requests in seconds (decrease for faster scraping)
PAGE_DELAY = 0.1                   # Delay between pages in thread (faster pagination)
REQUEST_TIMEOUT = 50               # Timeout for each request in seconds
MAX_REQUEST_RETRIES = 3            # How many times to retry failed requests
DEFAULT_THREADS_COUNT = 10         # Default threads for processing

# ========== HTTP CLIENT SETTINGS ==========
HTTP_CONNECTION_LIMIT = 300        # Maximum total connections (increased)
HTTP_CONNECTION_PER_HOST = 100     # Maximum connections per host (increased)
HTTP_KEEPALIVE_TIMEOUT = 60        # Keep connection alive timeout (increased)
USER_AGENT_STRING = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# ========== DATABASE SETTINGS ==========
TEMP_DB_PATH = "ForumScraping/media.db"
OUTPUT_HTML_FILE = "ForumScraping/forum_gallery.html"
UPLOAD_FILE_PATH = "ForumScraping/forum_gallery.html"

# ========== BATCH PROCESSING SETTINGS ==========
THREAD_BATCH_SIZE = 50             # Process threads in batches of this size
PROGRESS_UPDATE_INTERVAL = 10      # Seconds between progress message updates (minimum 10s)
PROGRESS_PERCENT_THRESHOLD = 5     # Minimum % change to trigger update
PROGRESS_UPDATE_DELAY = 5          # Minimum seconds between any progress update
MAX_PAGES_PER_FORUM = 50           # Maximum pages to scrape per forum
MAX_PAGES_PER_THREAD = 300         # Maximum pages to scrape per thread (for large threads)

# ========== HTML GENERATION SETTINGS ==========
MAX_FILE_SIZE_MB = 100            # Maximum HTML file size in MB
MAX_PAGINATION_RANGE = 100         # Maximum pagination buttons to show

# ========== MEDIA EXTRACTION SETTINGS ==========
VALID_MEDIA_EXTENSIONS = ["jpg", "jpeg", "png", "gif", "webp", "mp4", "mov", "avi", "mkv", "webm"]
EXCLUDE_URL_PATTERNS = ["/data/avatars/", "/data/assets/", "/data/addonflare/"]

# ========== UPLOAD HOSTS CONFIGURATION ==========
UPLOAD_HOSTS = [
    {
        "name": "HTML Hosting",
        "url": "https://html-hosting.tirev71676.workers.dev/api/upload",
        "field": "file"
    },
    {
        "name": "Litterbox",
        "url": "https://litterbox.catbox.moe/resources/internals/api.php",
        "field": "fileToUpload",
        "data": {"reqtype": "fileupload", "time": "72h"}
    },
    {
        "name": "Catbox",
        "url": "https://catbox.moe/user/api.php",
        "field": "fileToUpload",
        "data": {"reqtype": "fileupload"}
    }
]

# ========== HEALTH CHECK SETTINGS ==========
HEALTH_CHECK_PORT = int(os.getenv("PORT", 10000))
HEALTH_CHECK_HOST = '0.0.0.0'

# ========== HEALTH CHECK APP ==========
app = FastAPI()

@app.get('/')
def root():
    return {"status": "OK"}

@app.get('/health')
def health():
    return {"status": "OK"}

def run_fastapi():
    try:
        uvicorn.run(app, host=HEALTH_CHECK_HOST, port=HEALTH_CHECK_PORT, log_level="error")
    except Exception as e:
        logger.error(f"FastAPI error: {e}")

threading.Thread(target=run_fastapi, daemon=True).start()

# ========== FORUM SCRAPING CONFIG ==========
@dataclass
class ForumScraperConfig:
    max_concurrent: int = MAX_CONCURRENT_REQUESTS
    max_pages: int = MAX_PAGES_TO_SCRAPE
    max_depth: int = MAX_DEPTH_CRAWL
    base_delay: float = BASE_REQUEST_DELAY
    timeout: int = REQUEST_TIMEOUT
    max_retries: int = MAX_REQUEST_RETRIES
    threads: int = DEFAULT_THREADS_COUNT
    user_agent: str = USER_AGENT_STRING

forum_config = ForumScraperConfig()

# Performance tracking for forum scraping
@dataclass
class ForumStats:
    start_time: float = 0.0
    pages_processed: int = 0
    threads_found: int = 0
    requests_made: int = 0
    failed_requests: int = 0
    media_extracted: int = 0
    forums_discovered: int = 0
    
    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time if self.start_time > 0 else 0.0
    
    @property
    def pages_per_second(self) -> float:
        return self.pages_processed / self.elapsed if self.elapsed > 0 else 0.0
    
    @property
    def success_rate(self) -> float:
        return ((self.requests_made - self.failed_requests) / self.requests_made * 100) if self.requests_made > 0 else 0.0

# Compiled regex patterns for forum scraping performance
FORUM_PATTERN = re.compile(r"/forums?/", re.IGNORECASE)
THREAD_PATTERN = re.compile(r"/threads?/", re.IGNORECASE)
SKIP_PATTERNS = re.compile(r"/(login|logout|register|search|help|account|conversations|watched|bookmarks|members|find-new)", re.IGNORECASE)
FILE_EXTENSIONS = re.compile(r'\.(jpg|jpeg|png|gif|css|js|pdf|zip|rar|ico|svg)$', re.IGNORECASE)

# Thread URL normalization patterns
THREAD_SUFFIXES = re.compile(r'/(page-\d+|latest|unread|post-\d+|#.*?)/?$', re.IGNORECASE)
QUERY_PARAMS = re.compile(r'\?.*$')

# Media patterns
VALID_EXTS = VALID_MEDIA_EXTENSIONS
EXCLUDE_PATTERNS = EXCLUDE_URL_PATTERNS

# Pagination helper
def get_total_pages(html_str: str) -> int:
    """Extract total pages from pagination"""
    try:
        tree = HTMLParser(html_str)
        nav = tree.css_first("ul.pageNav-main")
        if not nav:
            return 1
        pages = [int(a.text(strip=True)) for a in nav.css("li.pageNav-page a") if a.text(strip=True).isdigit()]
        return max(pages) if pages else 1
    except Exception:
        return 1

def get_page_url(base_url: str, page: int) -> str:
    """Generate paginated URL (matching forumThead.py format)"""
    if page <= 1:
        return base_url
    # Handle different pagination formats
    if '/threads/' in base_url:
        # Thread pagination: /threads/name.123/page-2
        if base_url.endswith('/'):
            return f"{base_url}page-{page}"
        else:
            return f"{base_url}/page-{page}"
    elif '/forums/' in base_url:
        # Forum pagination: /forums/name.84/page-2
        if base_url.endswith('/'):
            return f"{base_url}page-{page}"
        else:
            return f"{base_url}/page-{page}"
    return base_url

# ========== PYROGRAM BOT CONFIG ==========
bot = Client("forum_scraper_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ========== DATABASE CONFIG ==========
TEMP_DB = TEMP_DB_PATH
OUTPUT_FILE = OUTPUT_HTML_FILE
MAX_FILE_SIZE_MB = MAX_FILE_SIZE_MB
MAX_PAGINATION_RANGE = MAX_PAGINATION_RANGE

# Upload Config
UPLOAD_FILE = UPLOAD_FILE_PATH
HOSTS = UPLOAD_HOSTS

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class ScraperError(Exception):
    pass

# ========== FORUM SCRAPING UTILITIES ==========

class AsyncForumHTTPClient:
    """High-performance async HTTP client for forum scraping using aiohttp"""
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(forum_config.max_concurrent)
    
    async def __aenter__(self):
        """Initialize async HTTP client with optimizations"""
        headers = {
            'User-Agent': forum_config.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        timeout = aiohttp.ClientTimeout(total=forum_config.timeout)
        connector = aiohttp.TCPConnector(
            limit=HTTP_CONNECTION_LIMIT,
            limit_per_host=HTTP_CONNECTION_PER_HOST,
            keepalive_timeout=HTTP_KEEPALIVE_TIMEOUT,
            enable_cleanup_closed=True
        )
        
        self.session = aiohttp.ClientSession(
            headers=headers,
            timeout=timeout,
            connector=connector
        )
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up HTTP client"""
        if self.session:
            await self.session.close()
    
    async def fetch_with_retry(self, url: str, stats: ForumStats) -> Tuple[str, int, str]:
        """Fetch URL with retry logic - returns (html, status_code, title)"""
        async with self.semaphore:
            for attempt in range(forum_config.max_retries):
                try:
                    if attempt > 0:
                        await asyncio.sleep(forum_config.base_delay * (2 ** (attempt - 1)))
                    
                    stats.requests_made += 1
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            html_content = await response.text()
                            title = self.extract_title_fast(html_content, url)
                            return html_content, response.status, title
                        elif response.status in [429, 503, 502, 504]:
                            await asyncio.sleep(forum_config.base_delay * 2)
                            continue
                        else:
                            return "", response.status, url
                            
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout for {url}, attempt {attempt + 1}")
                    continue
                except aiohttp.ClientError as e:
                    logger.warning(f"Client error for {url}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error for {url}: {e}")
                    break
            
            stats.failed_requests += 1
            return "", 0, url
    
    def extract_title_fast(self, html_content: str, fallback: str) -> str:
        """Ultra-fast title extraction using selectolax"""
        if not html_content:
            return fallback
        
        try:
            parser = HTMLParser(html_content)
            
            # Try title tag first
            title_tag = parser.css_first('title')
            if title_tag and title_tag.text(strip=True):
                title = title_tag.text(strip=True)
                if len(title) > 3:
                    return re.sub(r'\s+', ' ', title)[:150]
            
            # Try h1 tags
            h1_tags = parser.css('h1')
            for h1 in h1_tags[:2]:
                if h1.text(strip=True):
                    title = h1.text(strip=True)
                    if len(title) > 3:
                        return re.sub(r'\s+', ' ', title)[:150]
            
        except Exception:
            pass
        
        return fallback

def normalize_thread_url(url: str) -> str:
    """Advanced thread URL normalization"""
    try:
        parsed = urlparse(url)
        path = parsed.path
        
        if THREAD_PATTERN.search(path):
            # Remove query parameters first
            clean_url = QUERY_PARAMS.sub('', url)
            
            # Remove thread suffixes
            normalized = THREAD_SUFFIXES.sub('/', clean_url)
            
            # Ensure it ends with /
            if not normalized.endswith('/'):
                normalized += '/'
            
            return normalized
    
    except Exception:
        pass
    
    return url

def is_valid_forum_link(href: str, follow_domains: Set[str]) -> bool:
    """Ultra-fast link validation for forum scraping"""
    if not href or not isinstance(href, str) or len(href) < 2:
        return False
    
    href = href.strip()
    
    # Quick exclusions
    if (href.startswith("#") or href.startswith("javascript:") or 
        href.startswith("mailto:") or href.startswith("tel:")):
        return False
    
    # File extensions check
    if FILE_EXTENSIONS.search(href):
        return False
    
    try:
        parsed = urlparse(href)
        if parsed.netloc and parsed.netloc not in follow_domains:
            return False
    except Exception:
        return False
    
    return True

def extract_forum_links_fast(html_content: str, base_url: str, current_url: str, follow_domains: Set[str]) -> Tuple[List[str], List[str]]:
    """Ultra-fast link extraction using selectolax - Returns: (forum_links, thread_links)"""
    if not html_content:
        return [], []
    
    try:
        parser = HTMLParser(html_content)
    except Exception:
        return [], []
    
    forum_links = []
    thread_links = []
    
    # Get all links with href attributes
    links = parser.css('a[href]')
    
    for link in links:
        try:
            href = link.attributes.get('href', '').strip()
            if not is_valid_forum_link(href, follow_domains):
                continue
            
            full_url = urljoin(base_url, href)
            
            # Skip common patterns
            if SKIP_PATTERNS.search(full_url):
                continue
            
            path = urlparse(full_url).path.lower()
            
            if FORUM_PATTERN.search(path):
                forum_links.append(full_url)
            elif THREAD_PATTERN.search(path):
                normalized = normalize_thread_url(full_url)
                thread_links.append(normalized)
                
        except Exception:
            continue
    
    return forum_links, thread_links

def is_forum_url(url: str) -> bool:
    """Check if a URL is a forum URL"""
    return FORUM_PATTERN.search(url.lower()) is not None

def extract_media_from_html(raw_html: str) -> List[str]:
    """Extract all media URLs from HTML"""
    if not raw_html:
        return []
    
    html_content = html.unescape(raw_html)
    tree = HTMLParser(html_content)
    urls = set()
    
    # Extract from src attributes
    for node in tree.css("*[src]"):
        src = node.attributes.get("src", "").strip()
        if src:
            if "/vh/dli?" in src:
                src = src.replace("/vh/dli?", "/vh/dl?")
            urls.add(src)
    
    # Extract from data-src
    for node in tree.css("*[data-src]"):
        ds = node.attributes.get("data-src", "").strip()
        if ds:
            urls.add(ds)
    
    # Extract from data-video
    for node in tree.css("*[data-video]"):
        dv = node.attributes.get("data-video", "").strip()
        if dv:
            urls.add(dv)
    
    # Extract from video sources
    for node in tree.css("video, video source"):
        src = node.attributes.get("src", "").strip()
        if src:
            urls.add(src)
    
    # Extract from style attributes
    for node in tree.css("*[style]"):
        style = node.attributes.get("style") or ""
        for m in re.findall(r'url\((.*?)\)', style):
            m = m.strip('"\' ')
            if m:
                urls.add(m)
    
    # Extract URLs from text
    for match in re.findall(r'https?://[^\s"\'<>]+', html_content):
        urls.add(match.strip())
    
    # Filter media URLs
    media_urls = []
    for u in urls:
        if u:
            low = u.lower()
            if ("encoded$" in low and ".mp4" in low) or any(f".{ext}" in low for ext in VALID_EXTS):
                # Skip excluded patterns
                if any(bad in u for bad in EXCLUDE_PATTERNS):
                    continue
                media_urls.append(u)
    
    return list(dict.fromkeys(media_urls))

def get_base_url(url: str) -> str:
    """Extract base URL from full URL"""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"

def get_domain(url: str) -> str:
    """Extract domain from URL"""
    try:
        return urlparse(url).netloc
    except:
        return ""

# ========== DEEP FORUM CRAWLER ==========

class DeepForumCrawler:
    """Deep crawler for forums with BFS traversal"""
    
    def __init__(self, start_url: str, stats: ForumStats, db_path: str, main_source_name: str):
        self.start_url = start_url
        self.base_url = get_base_url(start_url)
        self.follow_domains = {get_domain(start_url)}
        self.stats = stats
        self.db_path = db_path
        self.main_source_name = main_source_name  # Single name for all media from this crawler
        
        # Tracking sets
        self.visited_forums: Set[str] = set()
        self.visited_threads: Set[str] = set()
        self.queued_forums: deque = deque()
        self.queued_threads: deque = deque()
        
        # Determine start type
        if is_forum_url(start_url):
            self.queued_forums.append(start_url)
        else:
            normalized = normalize_thread_url(start_url)
            self.queued_threads.append(normalized)
    
    async def crawl(self, http_client: AsyncForumHTTPClient, progress_callback=None):
        """Main crawling loop with BFS"""
        self.stats.start_time = time.time()
        
        # Initialize database
        await self.init_database()
        
        # Process forums first (BFS)
        while self.queued_forums and len(self.visited_forums) < forum_config.max_depth:
            forum_url = self.queued_forums.popleft()
            
            if forum_url in self.visited_forums:
                continue
            
            self.visited_forums.add(forum_url)
            
            # Fetch forum page to get total pages
            html, status, title = await http_client.fetch_with_retry(forum_url, self.stats)
            
            if status == 200 and html:
                self.stats.pages_processed += 1
                
                # Get total pages for this forum
                total_pages = get_total_pages(html)
                logger.info(f"Forum {forum_url} has {total_pages} pages")
                
                # Process all pages of this forum
                for page_num in range(1, min(total_pages + 1, MAX_PAGES_PER_FORUM + 1)):  # Use config variable
                    page_url = get_page_url(forum_url, page_num)
                    
                    # Skip first page as we already have it
                    if page_num == 1:
                        page_html = html
                    else:
                        page_html, page_status, _ = await http_client.fetch_with_retry(page_url, self.stats)
                        if page_status != 200:
                            continue
                        self.stats.pages_processed += 1
                    
                    # Extract links from this page
                    forum_links, thread_links = extract_forum_links_fast(
                        page_html, self.base_url, page_url, self.follow_domains
                    )
                    
                    # Add new forums to queue
                    for link in forum_links:
                        if link not in self.visited_forums:
                            self.queued_forums.append(link)
                            self.stats.forums_discovered += 1
                    
                    # Add threads to queue
                    for link in thread_links:
                        normalized = normalize_thread_url(link)
                        if normalized not in self.visited_threads:
                            self.queued_threads.append(normalized)
                            self.stats.threads_found += 1
                    
                    # Progress callback
                    if progress_callback:
                        await progress_callback()
                    
                    await asyncio.sleep(forum_config.base_delay)
            
            await asyncio.sleep(forum_config.base_delay)
        
        # Process threads
        thread_semaphore = asyncio.Semaphore(forum_config.max_concurrent)
        tasks = []
        
        while self.queued_threads:
            thread_url = self.queued_threads.popleft()
            
            if thread_url in self.visited_threads:
                continue
            
            self.visited_threads.add(thread_url)
            
            task = self.process_thread(http_client, thread_url, thread_semaphore, progress_callback)
            tasks.append(task)
            
            # Process in batches
            if len(tasks) >= THREAD_BATCH_SIZE:
                await asyncio.gather(*tasks)
                tasks.clear()
                gc.collect()
        
        # Process remaining
        if tasks:
            await asyncio.gather(*tasks)
    
    async def process_thread(self, http_client: AsyncForumHTTPClient, thread_url: str, semaphore, progress_callback=None):
        """Process a single thread and extract media from ALL pages and ALL articles (optimized version)"""
        async with semaphore:
            # Fetch first page
            html, status, title = await http_client.fetch_with_retry(thread_url, self.stats)
            
            if status == 200 and html:
                self.stats.pages_processed += 1
                
                # Get total pages for this thread
                total_pages = get_total_pages(html)
                logger.info(f"Thread {thread_url} has {total_pages} pages - processing all pages...")
                
                all_media_urls = []
                seen_in_thread = set()  # Track duplicates within thread
                
                # Process all pages of this thread
                for page_num in range(1, min(total_pages + 1, MAX_PAGES_PER_THREAD + 1)):
                    # Generate page URL
                    if page_num == 1:
                        page_url = thread_url
                        page_html = html
                    else:
                        # Handle different URL formats (like forumThead.py)
                        if thread_url.endswith('/'):
                            page_url = f"{thread_url}page-{page_num}"
                        else:
                            page_url = f"{thread_url}/page-{page_num}"
                        
                        page_html, page_status, _ = await http_client.fetch_with_retry(page_url, self.stats)
                        if page_status != 200:
                            logger.warning(f"Failed to fetch page {page_num}: status {page_status}")
                            continue
                        self.stats.pages_processed += 1
                    
                    # Extract media from ALL articles on this page (matching forumThead.py logic)
                    try:
                        tree = HTMLParser(page_html)
                        articles = tree.css("article.message--post")
                        
                        if not articles:
                            # Fallback: try generic article selector
                            articles = tree.css("article")
                        
                        page_media_count = 0
                        
                        for article in articles:
                            if not article or not article.html:
                                continue
                            
                            # Extract images
                            for img in article.css('img[src]'):
                                src = img.attributes.get('src', '').strip()
                                if not src:
                                    continue
                                
                                # Make absolute URL
                                if src.startswith('/'):
                                    src = urljoin(self.base_url, src)
                                elif not src.startswith(('http://', 'https://')):
                                    continue
                                
                                # Skip excluded patterns
                                if any(bad in src for bad in EXCLUDE_PATTERNS):
                                    continue
                                if src.startswith("data:image") or src.startswith("data:"):
                                    continue
                                if any(kw in src.lower() for kw in ["avatars", "ozzmodz_badges_badge", "premium", "likes", "addonflare"]):
                                    continue
                                
                                # Deduplicate within thread
                                if src not in seen_in_thread:
                                    all_media_urls.append(src)
                                    seen_in_thread.add(src)
                                    page_media_count += 1
                            
                            # Extract videos
                            for video in article.css('video[src], source[src]'):
                                src = video.attributes.get('src', '').strip()
                                if not src:
                                    continue
                                
                                # Make absolute URL
                                if src.startswith('/'):
                                    src = urljoin(self.base_url, src)
                                elif not src.startswith(('http://', 'https://')):
                                    continue
                                
                                # Skip excluded patterns
                                if any(bad in src for bad in EXCLUDE_PATTERNS):
                                    continue
                                if src.startswith("data:"):
                                    continue
                                
                                # Deduplicate within thread
                                if src not in seen_in_thread:
                                    all_media_urls.append(src)
                                    seen_in_thread.add(src)
                                    page_media_count += 1
                        
                        # Log progress every 10 pages or at milestones
                        if page_num % 10 == 0 or page_num == total_pages or page_num <= 5:
                            logger.info(f"Page {page_num}/{total_pages}: Extracted {len(all_media_urls)} media items (+{page_media_count} new)")
                        
                    except Exception as e:
                        logger.error(f"Error processing articles on page {page_num}: {e}")
                        continue
                    
                    # Small delay between pages (optimized)
                    await asyncio.sleep(PAGE_DELAY)
                    
                    # Progress callback every 20 pages
                    if page_num % 20 == 0 and progress_callback:
                        await progress_callback()
                
                logger.info(f"Thread {thread_url}: Total {len(all_media_urls)} unique media items from {total_pages} pages")
                
                # Store all media from all pages (deduplicated in database)
                if all_media_urls:
                    await self.store_media(thread_url, title, all_media_urls)
                    self.stats.media_extracted += len(all_media_urls)
                else:
                    logger.warning(f"No media found in thread {thread_url}")
                
                # Extract more thread links from first page (pagination, related threads)
                _, thread_links = extract_forum_links_fast(
                    html, self.base_url, thread_url, self.follow_domains
                )
                
                for link in thread_links:
                    normalized = normalize_thread_url(link)
                    if normalized not in self.visited_threads:
                        self.queued_threads.append(normalized)
                        self.stats.threads_found += 1
                
                # Progress callback
                if progress_callback:
                    await progress_callback()
            
            await asyncio.sleep(forum_config.base_delay)
    
    async def init_database(self):
        """Initialize SQLite database"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''CREATE TABLE IF NOT EXISTS media (
                id INTEGER PRIMARY KEY,
                thread_url TEXT,
                thread_title TEXT,
                media_url TEXT UNIQUE,
                media_type TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            # Create indexes
            await db.execute('CREATE INDEX IF NOT EXISTS idx_thread ON media(thread_url)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_type ON media(media_type)')
            await db.commit()
    
    async def store_media(self, thread_url: str, thread_title: str, media_urls: List[str]):
        """Store media URLs in database with main source name (optimized batch insert)"""
        if not media_urls:
            return
        
        # Prepare batch data
        batch_data = []
        for url in media_urls:
            # Clean URL
            url = url.strip()
            if url.startswith('%22') and url.endswith('%22'):
                url = url[3:-3]
            if url.startswith('"') and url.endswith('"'):
                url = url[1:-1]
            
            if not url.startswith(('http://', 'https://')):
                # Make absolute URL
                url = urljoin(self.base_url, url)
            
            # Determine type
            if 'vh/dl?url' in url or '.mp4' in url.lower():
                typ = 'videos'
            elif '.gif' in url.lower():
                typ = 'gifs'
            else:
                typ = 'images'
            
            batch_data.append((self.main_source_name, self.main_source_name, url, typ))
        
        # Batch insert for performance
        async with aiosqlite.connect(self.db_path) as db:
            try:
                await db.executemany(
                    'INSERT OR IGNORE INTO media (thread_url, thread_title, media_url, media_type) VALUES (?, ?, ?, ?)',
                    batch_data
                )
                await db.commit()
            except Exception as e:
                logger.warning(f"Failed to store media batch: {e}")

# ========== HTML GENERATOR ==========

def extract_name_from_url(url: str) -> str:
    """Extract readable name from forum/thread URL"""
    try:
        # Remove trailing slash and split
        parts = url.rstrip('/').split('/')
        # Get last part which usually contains the name
        if len(parts) >= 2:
            name_part = parts[-1]
            # Remove numeric suffix if present (e.g., ".84" or ".44794")
            if '.' in name_part:
                name_part = name_part.split('.')[0]
            # Replace hyphens with spaces and title case
            return name_part.replace('-', ' ').replace('_', ' ').title()
        return url
    except:
        return url

async def generate_html_gallery(db_path: str, source_urls: List[str]) -> Optional[str]:
    """Generate HTML gallery from database with ONE button per input URL"""
    logger.info("Generating HTML gallery...")
    
    # Fetch all media from database grouped by main source (input URL)
    media_by_source = defaultdict(lambda: {'images': {}, 'videos': {}, 'gifs': {}})
    total_count = 0
    
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute('SELECT thread_url, thread_title, media_url, media_type, scraped_at FROM media ORDER BY scraped_at DESC')
        async for row in cursor:
            thread_url, thread_title, media_url, media_type, scraped_at = row
            
            # thread_url is actually the main_source_name now
            source_name = thread_url
            
            # Clean URL
            clean_url = media_url.strip()
            if clean_url.startswith('%22') and clean_url.endswith('%22'):
                clean_url = clean_url[3:-3]
            if clean_url.startswith('"') and clean_url.endswith('"'):
                clean_url = clean_url[1:-1]
            
            if not clean_url.startswith(('http://', 'https://')):
                logger.warning(f"Skipping invalid URL: {clean_url}")
                continue
            
            try:
                # Ensure URL is properly escaped for JSON
                safe_src = clean_url.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '').replace('\r', '')
                
                # Get date
                date = scraped_at[:10] if scraped_at else datetime.now().strftime("%Y-%m-%d")
                
                # Store by date for this source
                if date not in media_by_source[source_name][media_type]:
                    media_by_source[source_name][media_type][date] = []
                
                media_by_source[source_name][media_type][date].append(safe_src)
                total_count += 1
            except Exception as e:
                logger.error(f"Failed to process media item: {media_url}, error: {e}")
                continue
    
    if total_count == 0:
        logger.warning("No media found in database")
        return None
    
    logger.info(f"Generating HTML with {total_count} media items from {len(media_by_source)} sources")
    
    # Prepare media data (one button per input URL)
    media_data = {}
    media_counts = {}
    total_type_counts = {'images': 0, 'videos': 0, 'gifs': 0}
    year_counts = {}
    
    for source_name in sorted(media_by_source.keys()):
        media_by_date = media_by_source[source_name]
        media_list = []
        count = 0
        
        for media_type in ['images', 'videos', 'gifs']:
            for date in sorted(media_by_date[media_type].keys(), reverse=True):
                for url in media_by_date[media_type][date]:
                    media_list.append({
                        'type': media_type,
                        'src': url,
                        'date': date
                    })
                    count += 1
                    total_type_counts[media_type] += 1
                    
                    # Year counts
                    year = date.split('-')[0]
                    year_counts[year] = year_counts.get(year, 0) + 1
        
        media_list = sorted(media_list, key=lambda x: x['date'], reverse=True)
        safe_name = source_name.replace(' ', '_')
        media_data[safe_name] = media_list
        media_counts[source_name] = count
    
    source_names_list = sorted(media_by_source.keys())
    
    # Serialize to JSON using orjson
    try:
        media_data_json = orjson.dumps(media_data).decode('utf-8')
        year_counts_json = orjson.dumps(year_counts).decode('utf-8')
        usernames_json = orjson.dumps([name.replace(' ', '_') for name in source_names_list]).decode('utf-8')
        
        json_size_mb = len(media_data_json) / (1024 * 1024)
        logger.info(f"Media data JSON size: {json_size_mb:.2f} MB")
        
        if json_size_mb > MAX_FILE_SIZE_MB:
            logger.error(f"Media data JSON size {json_size_mb:.2f} MB exceeds limit of {MAX_FILE_SIZE_MB} MB")
            return None
    except Exception as e:
        logger.error(f"Failed to serialize media data: {e}")
        return None
    
    # Calculate pagination
    default_items_per_page = max(1, math.ceil(total_count / MAX_PAGINATION_RANGE))
    
    title = "Forum Media Gallery"
    
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{ background-color: #000; font-family: Arial, sans-serif; margin: 0; padding: 20px; color: white; }}
    h1 {{ text-align: center; margin-bottom: 20px; font-size: 24px; }}
    .button-container {{ text-align: center; margin-bottom: 20px; display: flex; flex-wrap: wrap; justify-content: center; gap: 10px; }}
    .filter-button {{ padding: 14px 26px; margin: 6px; font-size: 18px; border-radius: 8px; border: none; background-color: #333; color: white; cursor: pointer; transition: background-color 0.3s; }}
    .filter-button:hover {{ background-color: #555; }}
    .filter-button.active {{ background-color: #007bff; }}
    .number-input {{ padding: 12px; font-size: 18px; width: 80px; border-radius: 8px; border: none; background-color: #333; color: white; }}
    .media-type-select {{ padding: 12px; font-size: 18px; border-radius: 8px; border: none; background-color: #333; color: white; }}
    .pagination {{ text-align: center; margin: 20px 0; }}
    .pagination-button {{ padding: 14px 24px; margin: 0 6px; font-size: 18px; border-radius: 8px; border: none; background-color: #333; color: white; cursor: pointer; transition: background-color 0.3s, transform 0.2s; }}
    .pagination-button:hover {{ background-color: #555; transform: scale(1.05); }}
    .pagination-button.active {{ background-color: #007bff; font-weight: bold; border: 2px solid #0056b3; }}
    .pagination-button:disabled {{ background-color: #555; cursor: not-allowed; opacity: 0.6; }}
    .masonry {{ display: flex; justify-content: center; gap: 10px; min-height: 100px; }}
    .column {{ flex: 1; display: flex; flex-direction: column; gap: 10px; }}
    .column img, .column video {{ width: 100%; border-radius: 5px; display: block; }}
    .column video {{ background-color: #111; }}
    @media (max-width: 768px) {{ 
      .masonry {{ flex-direction: column; }} 
      .filter-button {{ padding: 8px 15px; font-size: 14px; }} 
      .number-input, .media-type-select {{ width: 100px; font-size: 14px; }} 
      .pagination-button {{ padding: 6px 10px; font-size: 12px; }}
    }}
  </style>
</head>
<body>
  <h1>Forum Media Gallery</h1>
  <div class="button-container">
    <select id="mediaType" class="media-type-select">
      <option value="all" selected>All ({total_count})</option>
      <option value="images">Images ({total_type_counts['images']})</option>
      <option value="videos">Videos ({total_type_counts['videos']})</option>
      <option value="gifs">Gifs ({total_type_counts['gifs']})</option>
    </select>
    <select id="yearSelect" class="media-type-select">
      <option value="all" selected>All ({total_count})</option>
    </select>
    <div id="itemsPerUserContainer">
      <input type="number" id="itemsPerUser" class="number-input" min="1" value="2" placeholder="Items per thread">
    </div>
    <input type="number" id="itemsPerPage" class="number-input" min="1" value="{default_items_per_page}" placeholder="Items per page">
    <button class="filter-button active" data-usernames="" data-original-text="All">All ({total_count})</button>
    {"".join(
    f'<button class="filter-button" data-usernames="{html.escape(name.replace(" ", "_"))}" '
    f'data-original-text="{html.escape(name)} ({media_counts[name]})">'
    f'{html.escape(name)} ({media_counts[name]})</button>'
    for name in source_names_list)}
  </div>
  <div class="pagination" id="pagination"></div>
  <div class="masonry" id="masonry"></div>
  <script>
    const mediaData = {media_data_json};
    const usernames = {usernames_json};
    const yearCounts = {year_counts_json};
    const masonry = document.getElementById("masonry");
    const pagination = document.getElementById("pagination");
    const buttons = document.querySelectorAll('.filter-button');
    const mediaTypeSelect = document.getElementById('mediaType');
    const yearSelect = document.getElementById('yearSelect');
    
    // Populate year select
    yearSelect.innerHTML = '<option value="all" selected>All (' + Object.values(yearCounts).reduce((a,b)=>a+b,0) + ')</option>';
    const sortedYears = Object.keys(yearCounts).sort((a,b)=>b-a);
    sortedYears.forEach(year => {{
      const option = document.createElement('option');
      option.value = year;
      option.textContent = year + ' (' + yearCounts[year] + ')';
      yearSelect.appendChild(option);
    }});
    
    const itemsPerUserInput = document.getElementById('itemsPerUser');
    const itemsPerPageInput = document.getElementById('itemsPerPage');
    let selectedUsername = '';
    window.currentPage = 1;

    function updateButtonLabels() {{
      buttons.forEach(button => {{
        const originalText = button.getAttribute('data-original-text');
        button.textContent = originalText;
      }});
    }}

    function getOrderedMedia(mediaType, itemsPerUser, itemsPerPage, page, yearFilter) {{
      try {{
        let allMedia = [];
        if (selectedUsername === '') {{
          let maxRounds = 0;
          const mediaByUser = {{}};
          usernames.forEach(username => {{
            let userMedia = mediaData[username] || [];
            if (mediaType !== 'all') {{
              userMedia = userMedia.filter(item => item.type === mediaType);
            }}
            if (yearFilter !== 'all') {{
              userMedia = userMedia.filter(item => item.date.startsWith(yearFilter));
            }}
            userMedia = userMedia.sort((a, b) => new Date(b.date) - new Date(a.date));
            mediaByUser[username] = userMedia;
            maxRounds = Math.max(maxRounds, Math.ceil(userMedia.length / itemsPerUser));
          }});
          for (let round = 0; round < maxRounds; round++) {{
            usernames.forEach(username => {{
              const start = round * itemsPerUser;
              const end = start + itemsPerUser;
              allMedia = allMedia.concat(mediaByUser[username].slice(start, end));
            }});
          }}
          allMedia = allMedia.filter(item => item);
        }} else {{
          let userMedia = mediaData[selectedUsername] || [];
          if (mediaType !== 'all') {{
            userMedia = userMedia.filter(item => item.type === mediaType);
          }}
          if (yearFilter !== 'all') {{
            userMedia = userMedia.filter(item => item.date.startsWith(yearFilter));
          }}
          allMedia = userMedia.sort((a, b) => new Date(b.date) - new Date(a.date));
        }}
        const start = (page - 1) * itemsPerPage;
        const end = start + itemsPerPage;
        console.log('getOrderedMedia:', {{ mediaType, itemsPerUser, itemsPerPage, page, yearFilter, start, end, total: allMedia.length }});
        return {{ media: allMedia.slice(start, end), total: allMedia.length }};
      }} catch (e) {{
        console.error('Error in getOrderedMedia:', e);
        return {{ media: [], total: 0 }};
      }}
    }}

    function updatePagination(totalItems, itemsPerPage) {{
      try {{
        pagination.innerHTML = '';
        const totalPages = Math.ceil(totalItems / itemsPerPage);
        if (totalPages <= 1) {{
          console.log('updatePagination: Only one page, no pagination needed');
          return;
        }}

        console.log('updatePagination:', {{ totalItems, itemsPerPage, currentPage: window.currentPage, totalPages }});

        const maxButtons = 5;
        let startPage = Math.max(1, window.currentPage - Math.floor(maxButtons / 2));
        let endPage = Math.min(totalPages, startPage + maxButtons - 1);
        if (endPage - startPage + 1 < maxButtons) {{
          startPage = Math.max(1, endPage - maxButtons + 1);
        }}

        const prevButton = document.createElement('button');
        prevButton.className = 'pagination-button';
        prevButton.textContent = 'Previous';
        prevButton.disabled = window.currentPage === 1;
        prevButton.addEventListener('click', () => {{
          if (window.currentPage > 1) {{
            window.currentPage--;
            console.log('Previous button clicked, new currentPage:', window.currentPage);
            renderMedia();
          }}
        }});
        pagination.appendChild(prevButton);

        for (let i = startPage; i <= endPage; i++) {{
          const pageButton = document.createElement('button');
          pageButton.className = 'pagination-button' + (i === window.currentPage ? ' active' : '');
          pageButton.textContent = i;
          pageButton.addEventListener('click', (function(pageNumber) {{
            return function() {{
              window.currentPage = pageNumber;
              console.log('Page button clicked, new currentPage:', window.currentPage);
              renderMedia();
            }};
          }})(i));
          pagination.appendChild(pageButton);
        }}

        const nextButton = document.createElement('button');
        nextButton.className = 'pagination-button';
        nextButton.textContent = 'Next';
        nextButton.disabled = window.currentPage === totalPages;
        nextButton.addEventListener('click', () => {{
          if (window.currentPage < totalPages) {{
            window.currentPage++;
            console.log('Next button clicked, new currentPage:', window.currentPage);
            renderMedia();
          }}
        }});
        pagination.appendChild(nextButton);
      }} catch (e) {{
        console.error('Error in updatePagination:', e);
      }}
    }}

    function renderMedia() {{
      try {{
        masonry.innerHTML = '';
        const mediaType = mediaTypeSelect.value;
        const yearFilter = yearSelect.value;
        const itemsPerUser = parseInt(itemsPerUserInput.value) || 2;
        const itemsPerPage = parseInt(itemsPerPageInput.value) || {default_items_per_page};
        const result = getOrderedMedia(mediaType, itemsPerUser, itemsPerPage, window.currentPage, yearFilter);
        const allMedia = result.media;
        const totalItems = result.total;
        console.log('renderMedia:', {{ mediaType, yearFilter, itemsPerUser, itemsPerPage, currentPage: window.currentPage, mediaCount: allMedia.length, totalItems }});
        updatePagination(totalItems, itemsPerPage);

        const columnsCount = 3;
        const columns = [];
        for (let i = 0; i < columnsCount; i++) {{
          const col = document.createElement("div");
          col.className = "column";
          masonry.appendChild(col);
          columns.push(col);
        }}

        const totalRows = Math.ceil(allMedia.length / columnsCount);
        for (let row = 0; row < totalRows; row++) {{
          for (let col = 0; col < columnsCount; col++) {{
            const actualCol = row % 2 === 0 ? col : columnsCount - 1 - col;
            const index = row * columnsCount + col;
            if (index < allMedia.length) {{
              let element;
              if (allMedia[index].type === "videos") {{
                element = document.createElement("video");
                element.src = allMedia[index].src;
                element.controls = true;
                element.alt = "Video";
                element.loading = "lazy";
                element.preload = "metadata";
                element.playsInline = true;
                element.onerror = () => {{
                  console.error('Failed to load video:', allMedia[index].src);
                  element.remove();
                }};
              }} else {{
                element = document.createElement("img");
                element.src = allMedia[index].src;
                element.alt = allMedia[index].type.charAt(0).toUpperCase() + allMedia[index].type.slice(1);
                element.loading = "lazy";
                element.onerror = () => {{
                  console.error('Failed to load image:', allMedia[index].src);
                  element.remove();
                }};
              }}
              columns[actualCol].appendChild(element);
            }}
          }}
        }}
        window.scrollTo({{ top: 0, behavior: "smooth" }});
      }} catch (e) {{
        console.error('Error in renderMedia:', e);
        masonry.innerHTML = '<p style="color: red;">Error loading media. Please check console for details.</p>';
      }}
    }}

    buttons.forEach(button => {{
      button.addEventListener('click', () => {{
        try {{
          const username = button.getAttribute('data-usernames');
          if (button.classList.contains('active')) {{
            return;
          }}
          buttons.forEach(btn => btn.classList.remove('active'));
          button.classList.add('active');
          selectedUsername = username;
          window.currentPage = 1;
          console.log('Filter button clicked, selectedUsername:', username, 'currentPage:', window.currentPage);
          updateButtonLabels();
          renderMedia();
        }} catch (e) {{
          console.error('Error in button click handler:', e);
        }}
      }});
    }});

    mediaTypeSelect.addEventListener('change', () => {{
      try {{
        window.currentPage = 1;
        console.log('Media type changed, resetting currentPage to 1');
        renderMedia();
      }} catch (e) {{
        console.error('Error in mediaTypeSelect change handler:', e);
      }}
    }});

    yearSelect.addEventListener('change', () => {{
      try {{
        window.currentPage = 1;
        console.log('Year changed, resetting currentPage to 1');
        renderMedia();
      }} catch (e) {{
        console.error('Error in yearSelect change handler:', e);
      }}
    }});

    itemsPerUserInput.addEventListener('input', () => {{
      try {{
        window.currentPage = 1;
        console.log('Items per user changed, resetting currentPage to 1');
        renderMedia();
      }} catch (e) {{
        console.error('Error in itemsPerUserInput input handler:', e);
      }}
    }});

    itemsPerPageInput.addEventListener('input', () => {{
      try {{
        window.currentPage = 1;
        console.log('Items per page changed, resetting currentPage to 1');
        renderMedia();
      }} catch (e) {{
        console.error('Error in itemsPerPageInput input handler:', e);
      }}
    }});

    document.addEventListener('play', function(e) {{
      const videos = document.querySelectorAll("video");
      videos.forEach(video => {{
        if (video !== e.target) {{
          video.pause();
        }}
      }});
    }}, true);

    try {{
      updateButtonLabels();
      renderMedia();
    }} catch (e) {{
      console.error('Initial render failed:', e);
      masonry.innerHTML = '<p style="color: red;">Error loading media. Please check console for details.</p>';
    }}
  </script>
</body>
</html>"""
    
    logger.info(f"HTML generated successfully, size: {len(html_content) / 1024:.2f} KB")
    
    # Clean up memory
    try:
        media_by_source.clear()
        del media_by_source
        media_data.clear()
        del media_data
        gc.collect()
        logger.info("Memory cleaned up after HTML generation")
    except Exception as cleanup_error:
        logger.error(f"Error during cleanup: {cleanup_error}")
        gc.collect()
    
    return html_content

# ========== UPLOAD FUNCTIONS ==========

async def upload_file(client, host, data):
    """Upload file to hosting service"""
    buf = BytesIO(data)
    form_data = aiohttp.FormData()
    
    # Add file
    form_data.add_field(host["field"], buf, filename=UPLOAD_FILE, content_type="text/html")
    
    # Add additional data fields if present
    if "data" in host:
        for key, value in host["data"].items():
            form_data.add_field(key, value)
    
    try:
        async with client.post(host["url"], data=form_data, timeout=30.0) as r:
            if r.status in (200, 201):
                if host["name"] == "HTML Hosting":
                    j = await r.json()
                    if j.get("success") and j.get("url"):
                        return (host["name"], j["url"])
                    else:
                        return (host["name"], f"Error: {j.get('error', 'Unknown')}")
                else:
                    t = await r.text()
                    t = t.strip()
                    if t.startswith("https://"):
                        if host["name"] == "Litterbox" and "files.catbox.moe" in t:
                            t = "https://litterbox.catbox.moe/" + t.split("/")[-1]
                        return (host["name"], t)
                    return (host["name"], f"Invalid response: {t[:100]}")
            return (host["name"], f"HTTP {r.status}")
    except Exception as e:
        return (host["name"], f"Exception: {e}")

# ========== PROGRESS BAR ==========

def generate_bar(percentage):
    """Generate progress bar"""
    filled = int(percentage / 10)
    empty = 10 - filled
    return "●" * filled + "○" * (empty // 2) + "◌" * (empty - empty // 2)

# ========== BOT HANDLER ==========

@bot.on_message(filters.text & filters.private)
async def handle_message(client: Client, message: Message):
    """Handle incoming messages"""
    if message.chat.id not in ALLOWED_CHAT_IDS:
        return
    
    input_text = message.text.strip()
    
    # Parse multiple URLs (comma-separated)
    urls = [url.strip() for url in input_text.split(',') if url.strip()]
    
    # Validate URLs
    if not urls or not all(url.startswith('http') for url in urls):
        await message.reply("❌ Please provide valid forum or thread URL(s)\n\n**Examples:**\n• Single: `https://desifakes.com/forums/actress-ai-videos-and-gif.84/`\n• Multiple: `https://desifakes.com/threads/kiara-advani.44794/, https://desifakes.com/forums/bollywood.2/`")
        return
    
    # Send initial message
    progress_msg = await message.reply(f"🚀 Starting deep forum scraping...\n\n📍 **URLs to process:** {len(urls)}\n⏳ Initializing...")
    
    # Initialize stats
    stats = ForumStats()
    last_update = [time.time()]
    last_progress = [0.0]  # Track last progress percentage
    all_crawlers = []
    
    async def update_progress():
        """Update progress message with smart throttling"""
        now = time.time()
        
        # Calculate current progress
        total_media = sum(len(c.visited_threads) for c in all_crawlers) * 10  # Estimate
        current_progress = min(100, (stats.pages_processed / max(forum_config.max_pages, 1)) * 100)
        
        # Smart throttling: only update if enough time passed AND significant progress
        time_passed = now - last_update[0] >= PROGRESS_UPDATE_DELAY
        progress_changed = abs(current_progress - last_progress[0]) >= PROGRESS_PERCENT_THRESHOLD
        min_interval_passed = now - last_update[0] >= PROGRESS_UPDATE_INTERVAL
        
        if not (min_interval_passed or (time_passed and progress_changed)):
            return
        
        last_update[0] = now
        last_progress[0] = current_progress
        
        total_forums = sum(len(c.visited_forums) for c in all_crawlers)
        total_threads = sum(len(c.visited_threads) for c in all_crawlers)
        queued_forums = sum(len(c.queued_forums) for c in all_crawlers)
        queued_threads = sum(len(c.queued_threads) for c in all_crawlers)
        
        bar = generate_bar(current_progress)
        
        msg = f"""🔍 **Deep Forum Scraping**

{bar} {current_progress:.1f}%

📊 **Statistics:**
• URLs: {len(all_crawlers)} processing
• Forums: {total_forums} visited, {queued_forums} queued
• Threads: {total_threads} processed, {queued_threads} queued
• Pages: {stats.pages_processed:,} processed
• Media: {stats.media_extracted:,} extracted
• Requests: {stats.requests_made:,} ({stats.success_rate:.1f}% success)
• Speed: {stats.pages_per_second:.2f} pages/sec
• Time: {stats.elapsed:.1f}s

⏱️ Last updated: {datetime.now().strftime('%H:%M:%S')}"""
        
        try:
            await progress_msg.edit(msg)
        except Exception:
            pass
    
    # Start scraping
    try:
        async with AsyncForumHTTPClient() as http_client:
            # Process all URLs
            for url in urls:
                # Extract name for this main source
                main_source_name = extract_name_from_url(url)
                crawler = DeepForumCrawler(url, stats, TEMP_DB, main_source_name)
                all_crawlers.append(crawler)
                await crawler.crawl(http_client, update_progress)
        
        # Final progress
        total_forums = sum(len(c.visited_forums) for c in all_crawlers)
        total_threads = sum(len(c.visited_threads) for c in all_crawlers)
        
        await progress_msg.edit(f"""✅ **Scraping Complete!**

📊 **Final Statistics:**
• URLs: {len(urls)} processed
• Forums: {total_forums} visited
• Threads: {total_threads} processed
• Media: {stats.media_extracted:,} extracted (deduplicated)
• Pages: {stats.pages_processed:,} processed
• Requests: {stats.requests_made:,} ({stats.success_rate:.1f}% success)
• Speed: {stats.pages_per_second:.2f} pages/sec
• Time: {stats.elapsed:.1f}s

🎨 Generating HTML gallery...""")
        
        # Generate HTML
        html_content = await generate_html_gallery(TEMP_DB, urls)
        
        if not html_content:
            await message.reply("❌ No media found or HTML generation failed")
            return
        
        # Save HTML
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML saved to {OUTPUT_FILE}")
        
        # Upload to hosts
        await progress_msg.edit("📤 Uploading to hosting services...")
        
        with open(OUTPUT_FILE, 'rb') as f:
            data = f.read()
        
        async with aiohttp.ClientSession() as session:
            tasks = [upload_file(session, h, data) for h in HOSTS]
            results = await asyncio.gather(*tasks)
        
        links = []
        for name, res in results:
            status = "✅" if res.startswith("https://") else "❌"
            links.append(f"{status} {name}: {res}")
        
        caption = f"""✅ **Forum Scraping Complete!**

📊 **Statistics:**
• Total Media: {stats.media_extracted}
• URLs: {len(urls)}
• Forums: {total_forums}
• Threads: {total_threads}
• Time: {stats.elapsed:.1f}s

📤 **Upload Links:**
{chr(10).join(links)}"""
        
        # Send file
        await message.reply_document(OUTPUT_FILE, caption=caption)
        await progress_msg.delete()
        
        # Cleanup
        try:
            if os.path.exists(TEMP_DB):
                os.remove(TEMP_DB)
            if os.path.exists("ForumScraping"):
                import shutil
                shutil.rmtree("ForumScraping")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
    
    except Exception as e:
        logger.error(f"Scraping error: {e}")
        import traceback
        traceback.print_exc()
        await message.reply(f"❌ Error: {str(e)}\n\nCheck logs for details.")

# ========== MAIN ==========

if __name__ == "__main__":
    logger.info("Starting Forum Scraper Bot...")
    bot.run()
