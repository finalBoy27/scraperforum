import asyncio
import httpx
import re
import os
import json
import time
import html
import gc
import logging
import aioshutil
import random
from urllib.parse import urlencode, urljoin
from selectolax.parser import HTMLParser
from datetime import datetime
from pathlib import Path
from io import BytesIO
from pyrogram import Client, filters
from pyrogram.types import Update, Message, InputMediaPhoto
from pyrogram.raw.functions.channels import CreateForumTopic
from pyrogram.errors import FloodWait
import aiosqlite
from fastapi import FastAPI
import uvicorn
import threading
from PIL import Image
import imageio.v3 as iio

# Health check app
app = FastAPI()

@app.get('/')
def root():
    return {"status": "OK"}

@app.get('/health')
def health():
    return {"status": "OK"}

def run_fastapi():
    uvicorn.run(app, host='0.0.0.0', port=int(os.getenv("PORT", 10000)))

# Disable FastAPI logs if needed, but for now keep
logging.getLogger('uvicorn').disabled = True  # optional

threading.Thread(target=run_fastapi, daemon=True).start()

# ───────────────────────────────
# ⚙️ CONFIG - ALL IMPORTANT VARIABLES
# ───────────────────────────────

# === TELEGRAM CREDENTIALS ===
API_ID = int(os.getenv("API_ID", 24536446))
API_HASH = os.getenv("API_HASH", "baee9dd189e1fd1daf0fb7239f7ae704")
BOT_TOKEN = os.getenv("BOT_TOKEN", "7841933095:AAEz5SLNiGzWanheul1bwZL4HJbQBOBROqw")

# === DOWNLOAD SETTINGS ===
TIMEOUT = [12.0, 15.0, 18.0, 21.0]   # HTTP request timeout per attempt (seconds) - grows dynamically
MAX_DOWNLOAD_RETRIES = 4             # How many times to retry a failed download
RETRY_DELAY = [0.5, 0.7, 0.9, 1.0]   # Wait time between retries per attempt (seconds) - exponential backoff
DELAY_BETWEEN_REQUESTS = 0.3         # Delay between concurrent download requests
MAX_CONCURRENT_WORKERS = 10          # Maximum concurrent downloads
BATCH_DOWNLOAD_SIZE = 10             # Download this many URLs at once

# === SEND SETTINGS ===
BATCH_SEND_SIZE = 10                 # Must accumulate 10 images before sending (Telegram media group limit)
SEND_DELAY = 1.5                     # Delay between sending media groups (seconds) to avoid rate limits
SEND_SEMAPHORE = asyncio.Semaphore(1)  # Limit concurrent sends to prevent rate limits
MIN_IMAGE_SIZE = 100                 # Minimum image size in bytes (filter out tiny images)

# === MEDIA CONVERSION SETTINGS ===
ENABLE_GIF_CONVERSION = True         # Convert GIFs to static thumbnails
ENABLE_VIDEO_CONVERSION = True       # Convert videos to thumbnails
VIDEO_DOMAIN_PREFIX = "https://video.desifakes.net/vh/dli?"  # EXACT prefix for downloadable videos
EXCLUDED_VIDEO_PREFIXES = ["https://video.desifakes.net/vh/dl?"]  # Bad video URLs to exclude
VIDEO_EXTS = ["mp4", "avi", "mov", "webm", "mkv", "flv", "wmv"]  # Video file extensions
GIF_EXTS = ["gif"]                   # GIF extensions
PRESERVE_ORIGINAL_QUALITY = True     # Keep original quality without compression
CONVERT_TO_EXTENSION = ".jpg"        # Convert all media to this extension for consistency

# === FILTERING SETTINGS ===
EXCLUDED_DOMAINS = ["pornbb.xyz"]    # Domains to exclude from download
VALID_IMAGE_EXTS = ["jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff", "svg", "ico", "avif", "jfif"]

# === PROGRESS & UI SETTINGS ===
PROGRESS_UPDATE_INTERVAL = 20        # Seconds between progress message updates
PROGRESS_PERCENT_THRESHOLD = 10      # Minimum % change to trigger update
PROGRESS_UPDATE_DELAY = 5            # Minimum seconds between any progress update

# === STORAGE SETTINGS ===
TEMP_DIR = "temp_images"             # Temporary directory for downloaded images
TEMP_DB = "Scraping/tempImages.db"   # Database for tracking (not used currently)

# Initialize Pyrogram client
bot = Client("image_downloader_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ───────────────────────────────
# 🧩 LOGGING SETUP
# ───────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress Pyrogram connection logs
logging.getLogger('pyrogram').setLevel(logging.WARNING)

def log_memory():
    try:
        import psutil
        mem = psutil.Process().memory_info().rss / 1024 / 1024
        logger.info(f"Memory usage: {mem:.2f} MB")
    except ImportError:
        logger.info("psutil not available for memory tracking")

def generate_bar(percentage):
    filled = int(percentage / 10)
    empty = 10 - filled
    return "●" * filled + "○" * (empty // 2) + "◌" * (empty - empty // 2)

# ───────────────────────────────
# 📹 VIDEO & GIF CONVERSION
# ───────────────────────────────

def is_gif_url(url):
    """Check if URL is a GIF that should be converted to thumbnail"""
    url_lower = url.lower()
    for ext in GIF_EXTS:
        if f'.{ext}' in url_lower:
            logger.debug(f"🎬 GIF URL detected: {url}")
            return True
    return False

def is_video_url(url):
    """Check if URL is a video that should be converted to thumbnail"""
    # First, check excluded video prefixes (URLs that look like videos but don't work)
    for excluded_prefix in EXCLUDED_VIDEO_PREFIXES:
        if url.startswith(excluded_prefix):
            logger.debug(f"⚠️ Excluding video URL (matches excluded prefix): {url}")
            return False
    
    # Check if URL starts with EXACT special video domain prefix
    if url.startswith(VIDEO_DOMAIN_PREFIX):
        logger.debug(f"✅ Video URL detected (matches VIDEO_DOMAIN_PREFIX): {url}")
        return True
    
    # Check for video extensions in URL
    url_lower = url.lower()
    for ext in VIDEO_EXTS:
        if f'.{ext}' in url_lower:
            logger.debug(f"🎥 Video URL detected (extension): {url}")
            return True
    
    return False

def convert_gif_to_thumbnail(filepath):
    """Convert GIF to static thumbnail (first frame) as JPEG - preserving original quality"""
    try:
        logger.info(f"🎬 Converting GIF to thumbnail: {filepath}")
        with Image.open(filepath) as gif:
            # Get first frame
            gif.seek(0)
            frame = gif.convert("RGB")
            
            # Create new filepath with correct extension
            new_filepath = filepath.rsplit('.', 1)[0] + CONVERT_TO_EXTENSION
            
            # Save with maximum quality - no compression or optimization
            if PRESERVE_ORIGINAL_QUALITY:
                frame.save(new_filepath, 'JPEG', quality=100, subsampling=0)
            else:
                frame.save(new_filepath, 'JPEG', quality=95, optimize=True)
            
        # Remove original GIF file immediately
        if os.path.exists(filepath):
            os.remove(filepath)
        
        # Force garbage collection to free memory immediately
        del frame
        gc.collect()
        
        new_size = os.path.getsize(new_filepath)
        logger.info(f"✅ GIF converted to thumbnail: {new_size} bytes → {new_filepath}")
        return new_filepath
    except Exception as e:
        logger.error(f"❌ GIF conversion failed for {filepath}: {str(e)}")
        # Clean up on error
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except:
            pass
        return None

def convert_video_to_thumbnail(filepath):
    """Convert video to thumbnail (first frame) as JPG - preserving original quality"""
    try:
        logger.info(f"🎥 Converting video to thumbnail: {filepath}")
        
        # Read first frame from video file
        frame = iio.imread(filepath, index=0)
        
        # Create new filepath with correct extension
        new_filepath = filepath.rsplit('.', 1)[0] + CONVERT_TO_EXTENSION
        
        # Save with maximum quality - no compression
        if PRESERVE_ORIGINAL_QUALITY:
            iio.imwrite(new_filepath, frame, quality=100)
        else:
            iio.imwrite(new_filepath, frame, quality=95)
        
        # Clear frame from memory
        del frame
        
        # Remove original video file immediately
        if os.path.exists(filepath):
            os.remove(filepath)
        
        # Force garbage collection
        gc.collect()
        
        new_size = os.path.getsize(new_filepath)
        logger.info(f"✅ Video converted to thumbnail: {new_size} bytes → {new_filepath}")
        return new_filepath
    except Exception as e:
        logger.error(f"❌ Video conversion failed for {filepath}: {str(e)}")
        # Clean up on error
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except:
            pass
        return None

def normalize_image_extension(filepath):
    """Convert any image to consistent extension (.jpg) - preserving original quality"""
    try:
        if filepath.lower().endswith(CONVERT_TO_EXTENSION):
            return filepath  # Already correct extension
        
        logger.info(f"🔄 Normalizing image extension: {filepath}")
        with Image.open(filepath) as img:
            # Convert to RGB if necessary
            if img.mode in ('RGBA', 'LA', 'P'):
                img = img.convert('RGB')
            
            # Create new filepath with correct extension
            new_filepath = filepath.rsplit('.', 1)[0] + CONVERT_TO_EXTENSION
            
            # Save with maximum quality - no compression or optimization
            if PRESERVE_ORIGINAL_QUALITY:
                img.save(new_filepath, 'JPEG', quality=100, subsampling=0)
            else:
                img.save(new_filepath, 'JPEG', quality=95, optimize=True)
        
        # Remove original file
        if os.path.exists(filepath) and filepath != new_filepath:
            os.remove(filepath)
        
        gc.collect()
        logger.info(f"✅ Extension normalized: {new_filepath}")
        return new_filepath
    except Exception as e:
        logger.error(f"❌ Extension normalization failed for {filepath}: {str(e)}")
        return filepath  # Return original on error

# ───────────────────────────────
# 🧩 UTILITIES
# ───────────────────────────────
async def fetch_html(url: str):
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(url, follow_redirects=True, timeout=TIMEOUT)
            return r.text if r.status_code == 200 else ""
    except Exception as e:
        logger.error(f"Fetch error for {url}: {e}")
        return ""

def extract_media_data_from_html(html_str: str):
    """Extract mediaData, usernames, yearCounts from HTML"""
    try:
        tree = HTMLParser(html_str)
        script_tags = tree.css("script")
        media_data = {}
        usernames = []
        year_counts = {}

        for script in script_tags:
            script_text = script.text()
            if "const mediaData =" in script_text:
                # Extract mediaData JSON
                match = re.search(r'const mediaData = (\{.*?\});', script_text, re.DOTALL)
                if match:
                    media_data = json.loads(match.group(1))
            if "const usernames =" in script_text:
                # Extract usernames JSON
                match = re.search(r'const usernames = (\[.*?\]);', script_text, re.DOTALL)
                if match:
                    usernames = json.loads(match.group(1))
            if "const yearCounts =" in script_text:
                # Extract yearCounts JSON
                match = re.search(r'const yearCounts = (\{.*?\});', script_text, re.DOTALL)
                if match:
                    year_counts = json.loads(match.group(1))

        return media_data, usernames, year_counts
    except Exception as e:
        logger.error(f"Error extracting media data: {str(e)}")
        return {}, [], {}

def create_username_images(media_data, usernames):
    """Create username_images dict from mediaData"""
    username_images = {}
    for username in usernames:
        safe_username = username.replace(' ', '_')
        if safe_username in media_data:
            urls = [item['src'] for item in media_data[safe_username]]
            username_images[username] = urls
    return username_images

def filter_and_deduplicate_urls(username_images):
    """Filter URLs, exclude domains, remove duplicates, categorize media types"""
    all_urls = []
    seen_urls = set()
    filtered_username_images = {}
    media_stats = {'images': 0, 'gifs': 0, 'videos': 0, 'excluded': 0}

    for username, urls in username_images.items():
        filtered_urls = []
        for url in urls:
            if not url or not url.startswith(('http://', 'https://')):
                continue
            
            # Skip duplicates
            if url in seen_urls:
                continue
            
            # Exclude specific domains
            if any(domain in url.lower() for domain in EXCLUDED_DOMAINS):
                media_stats['excluded'] += 1
                continue
            
            # Check if it's a video
            if is_video_url(url):
                if ENABLE_VIDEO_CONVERSION:
                    seen_urls.add(url)
                    all_urls.append(url)
                    filtered_urls.append(url)
                    media_stats['videos'] += 1
                else:
                    media_stats['excluded'] += 1
                continue
            
            # Check if it's a GIF
            if is_gif_url(url):
                if ENABLE_GIF_CONVERSION:
                    seen_urls.add(url)
                    all_urls.append(url)
                    filtered_urls.append(url)
                    media_stats['gifs'] += 1
                else:
                    media_stats['excluded'] += 1
                continue
            
            # Check if it's a valid image
            url_lower = url.lower()
            has_image_ext = any(f".{ext}" in url_lower for ext in VALID_IMAGE_EXTS)
            
            if has_image_ext:
                seen_urls.add(url)
                all_urls.append(url)
                filtered_urls.append(url)
                media_stats['images'] += 1
        
        if filtered_urls:
            filtered_username_images[username] = filtered_urls
    
    logger.info(f"📊 Media filtered - Images: {media_stats['images']}, GIFs: {media_stats['gifs']}, "
                f"Videos: {media_stats['videos']}, Excluded: {media_stats['excluded']}")

    return filtered_username_images, all_urls

async def download_image(url, temp_dir, semaphore, url_metadata=None):
    """Download single image/video/gif with retries and conversion"""
    async with semaphore:
        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
        
        for attempt in range(MAX_DOWNLOAD_RETRIES):
            current_timeout = TIMEOUT[min(attempt, len(TIMEOUT) - 1)]
            current_retry_delay = RETRY_DELAY[min(attempt, len(RETRY_DELAY) - 1)]
            
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.get(url, timeout=current_timeout, follow_redirects=True)
                    
                    if r.status_code == 200:
                        content = r.content
                        if len(content) < MIN_IMAGE_SIZE:
                            logger.warning(f"⚠️ Content too small ({len(content)} bytes): {url}")
                            return None
                        
                        # Determine file extension from URL or content-type
                        file_ext = '.jpg'  # default
                        if is_video_url(url):
                            # Detect video extension
                            for ext in VIDEO_EXTS:
                                if f'.{ext}' in url.lower():
                                    file_ext = f'.{ext}'
                                    break
                        elif is_gif_url(url):
                            file_ext = '.gif'
                        else:
                            # Try to get extension from URL
                            for ext in VALID_IMAGE_EXTS:
                                if f'.{ext}' in url.lower():
                                    file_ext = f'.{ext}'
                                    break
                        
                        # Save to temp file
                        filename = f"temp_{int(time.time() * 1000000)}_{len(content)}{file_ext}"
                        filepath = os.path.join(temp_dir, filename)
                        
                        with open(filepath, 'wb') as f:
                            f.write(content)
                        
                        # Convert if needed
                        if is_video_url(url) and ENABLE_VIDEO_CONVERSION:
                            converted_path = convert_video_to_thumbnail(filepath)
                            if converted_path:
                                filepath = converted_path
                            else:
                                return None  # Conversion failed
                        elif is_gif_url(url) and ENABLE_GIF_CONVERSION:
                            converted_path = convert_gif_to_thumbnail(filepath)
                            if converted_path:
                                filepath = converted_path
                            else:
                                return None  # Conversion failed
                        else:
                            # Normalize extension for regular images
                            filepath = normalize_image_extension(filepath)
                        
                        # Verify file exists and has content
                        if not os.path.exists(filepath):
                            logger.error(f"❌ File disappeared after processing: {filepath}")
                            return None
                        
                        final_size = os.path.getsize(filepath)
                        logger.info(f"✅ Downloaded & processed ({final_size} bytes): {url}")
                        
                        return {
                            'url': url,
                            'path': filepath,
                            'size': final_size,
                            'username': url_metadata.get('username') if url_metadata else None
                        }
                    
                    elif r.status_code == 404:
                        logger.info(f"ℹ️ 404 Not Found: {url}")
                        return None  # Not retryable
                    else:
                        logger.warning(f"⚠️ HTTP {r.status_code} for {url} (attempt {attempt + 1}/{MAX_DOWNLOAD_RETRIES})")
                
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ Timeout for {url} (attempt {attempt + 1}/{MAX_DOWNLOAD_RETRIES}, timeout={current_timeout}s)")
            except Exception as e:
                logger.warning(f"⚠️ Download error for {url} (attempt {attempt + 1}/{MAX_DOWNLOAD_RETRIES}): {str(e)}")
            
            # Wait before retry (except on last attempt)
            if attempt < MAX_DOWNLOAD_RETRIES - 1:
                await asyncio.sleep(current_retry_delay)
        
        logger.error(f"❌ Failed after {MAX_DOWNLOAD_RETRIES} attempts: {url}")
        return None

async def download_batch(urls, temp_dir, username_map=None):
    """Download batch of URLs concurrently with metadata"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_WORKERS)
    
    # Create metadata for each URL
    tasks = []
    for url in urls:
        metadata = {'username': username_map.get(url) if username_map else None}
        tasks.append(download_image(url, temp_dir, semaphore, url_metadata=metadata))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    successful = []
    failed = []
    
    for url, result in zip(urls, results):
        if isinstance(result, Exception):
            logger.error(f"❌ Exception during download of {url}: {str(result)}")
            failed.append(url)
        elif result is not None:
            successful.append(result)
        else:
            failed.append(url)
    
    return successful, failed

async def send_image_batch_pyrogram(images, username, chat_id, topic_id=None, batch_num=1):
    """Send exactly 10 images using Pyrogram with proper error handling"""
    if not images or len(images) != BATCH_SEND_SIZE:
        logger.warning(f"⚠️ Attempted to send {len(images)} images, expected {BATCH_SEND_SIZE}")
        return False
    
    async with SEND_SEMAPHORE:
        await asyncio.sleep(SEND_DELAY)  # Rate limit protection
        
        max_send_retries = 3
        for attempt in range(max_send_retries):
            try:
                media = []
                for i, img in enumerate(images):
                    if i == 0:
                        caption = f"{username.replace('_', ' ')} - {batch_num}"
                        media.append(InputMediaPhoto(img['path'], caption=caption))
                    else:
                        media.append(InputMediaPhoto(img['path']))
                
                if topic_id:
                    await bot.send_media_group(chat_id, media, reply_to_message_id=topic_id)
                else:
                    await bot.send_media_group(chat_id, media)
                
                logger.info(f"✅ Sent batch {batch_num} for {username} ({len(images)} images)")
                return True
                
            except FloodWait as e:
                wait_time = e.value + 2  # Add buffer
                logger.warning(f"⏳ FloodWait: waiting {wait_time}s for {username} batch {batch_num}")
                await asyncio.sleep(wait_time)
                # Retry after wait
                continue
                
            except Exception as e:
                logger.error(f"❌ Send error for {username} batch {batch_num} (attempt {attempt + 1}/{max_send_retries}): {str(e)}")
                if attempt < max_send_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                else:
                    return False
        
        return False

def cleanup_images(images):
    """Remove temp image files and clear from memory"""
    if not images:
        return
    
    for img in images:
        try:
            if isinstance(img, dict) and 'path' in img:
                if os.path.exists(img['path']):
                    os.remove(img['path'])
        except Exception as e:
            logger.warning(f"⚠️ Cleanup error for {img.get('path', 'unknown')}: {str(e)}")
    
    # Clear references
    images.clear()
    gc.collect()

async def process_batches(username_images, chat_id, topic_id=None, user_topic_ids=None, progress_msg=None):
    """
    Process all URLs with improved batching logic:
    - Download in batches of BATCH_DOWNLOAD_SIZE
    - Accumulate BATCH_SEND_SIZE (10) before sending
    - Track pending sends accurately
    - Handle failed URLs separately without infinite loops
    - Proper cleanup and memory management
    """
    
    # Create URL to username mapping
    url_to_username = {}
    all_urls = []
    for username, urls in username_images.items():
        for url in urls:
            url_to_username[url] = username
            all_urls.append(url)
    
    total_urls = len(all_urls)
    logger.info(f"📊 Processing {total_urls} total URLs for {len(username_images)} users")
    
    # Stats tracking
    stats = {
        'total_urls': total_urls,
        'downloaded': 0,
        'sent': 0,
        'failed': 0,
        'batch_num': 0,
        'round': 1,
        'pending_send': 0,
        'retry_queue': 0
    }
    
    # Storage for pending images per username
    pending_images_by_user = {username: [] for username in username_images.keys()}
    user_batch_nums = {username: 1 for username in username_images.keys()}
    
    # Failed URL tracking
    failed_urls = []
    failed_urls_seen = set()  # Prevent infinite retry loops
    
    temp_dir = TEMP_DIR
    os.makedirs(temp_dir, exist_ok=True)
    
    # Progress tracking
    last_progress_update = [0]
    last_progress_percent = [0]
    
    async def update_progress():
        """Update progress message with current stats"""
        now = time.time()
        progress_percent = int((stats['downloaded'] + stats['failed']) / total_urls * 100) if total_urls else 100
        
        # Check if enough time passed and percentage changed enough
        time_ok = (now - last_progress_update[0]) >= PROGRESS_UPDATE_DELAY
        percent_ok = (progress_percent - last_progress_percent[0]) >= PROGRESS_PERCENT_THRESHOLD
        
        if time_ok and percent_ok and progress_msg:
            bar = generate_bar(progress_percent)
            
            # Count total users being processed
            total_users = len(username_images)
            current_user_idx = min(stats['batch_num'] // 10 + 1, total_users)
            
            progress_text = (
                f"👤 User: Processing ({current_user_idx}/{total_users})\n"
                f"{bar} {progress_percent}%\n"
                f"📦 Batch: {stats['batch_num']} | Round: {stats['round']}\n"
                f"📥 Downloaded: {stats['downloaded']}\n"
                f"📤 Sent: {stats['sent']}\n"
                f"💾 Pending Send: {stats['pending_send']}\n"
                f"❌ Failed: {stats['failed']}\n"
                f"🔄 Retry Queue: {stats['retry_queue']}"
            )
            
            try:
                await progress_msg.edit(progress_text)
                last_progress_update[0] = now
                last_progress_percent[0] = progress_percent
            except FloodWait as e:
                logger.warning(f"⏳ FloodWait on progress update: {e.value}s")
                await asyncio.sleep(e.value)
                try:
                    await progress_msg.edit(progress_text)
                    last_progress_update[0] = now
                    last_progress_percent[0] = progress_percent
                except:
                    pass
            except Exception as e:
                logger.warning(f"⚠️ Progress update error: {str(e)}")
    
    async def send_accumulated_batches():
        """Send all accumulated batches of 10 images"""
        sent_count = 0
        for username, images in list(pending_images_by_user.items()):
            while len(images) >= BATCH_SEND_SIZE:
                # Take exactly 10 images
                batch_to_send = images[:BATCH_SEND_SIZE]
                images[:BATCH_SEND_SIZE] = []  # Remove from pending
                
                # Get topic for this user
                user_topic = user_topic_ids.get(username) if user_topic_ids else topic_id
                batch_num = user_batch_nums[username]
                
                # Send the batch
                success = await send_image_batch_pyrogram(
                    batch_to_send, username, chat_id, user_topic, batch_num
                )
                
                if success:
                    stats['sent'] += len(batch_to_send)
                    sent_count += len(batch_to_send)
                    user_batch_nums[username] += 1
                    
                    # Cleanup sent images
                    cleanup_images(batch_to_send)
                else:
                    logger.error(f"❌ Failed to send batch {batch_num} for {username}")
                    # Don't re-add to pending, just cleanup
                    cleanup_images(batch_to_send)
                
                # Update pending count
                stats['pending_send'] = sum(len(imgs) for imgs in pending_images_by_user.values())
        
        return sent_count
    
    # ═══════════════════════════════════════════
    # PHASE 1: Process all original URLs
    # ═══════════════════════════════════════════
    logger.info("🚀 Phase 1: Processing original URLs")
    
    url_index = 0
    while url_index < len(all_urls):
        # Download batch
        batch_urls = all_urls[url_index:url_index + BATCH_DOWNLOAD_SIZE]
        url_index += BATCH_DOWNLOAD_SIZE
        stats['batch_num'] += 1
        
        logger.info(f"📦 Downloading batch {stats['batch_num']} ({len(batch_urls)} URLs)")
        successful, failed = await download_batch(batch_urls, temp_dir, url_to_username)
        
        # Track failed URLs for retry (only if not seen before)
        for failed_url in failed:
            if failed_url not in failed_urls_seen:
                failed_urls.append(failed_url)
                failed_urls_seen.add(failed_url)
        
        stats['failed'] += len(failed)
        stats['retry_queue'] = len(failed_urls)
        
        # Add successful downloads to pending by username
        for img_data in successful:
            username = img_data.get('username') or url_to_username.get(img_data['url'], 'Unknown')
            if username in pending_images_by_user:
                pending_images_by_user[username].append(img_data)
        
        stats['downloaded'] += len(successful)
        stats['pending_send'] = sum(len(imgs) for imgs in pending_images_by_user.values())
        
        # Send accumulated batches if we have enough
        await send_accumulated_batches()
        stats['pending_send'] = sum(len(imgs) for imgs in pending_images_by_user.values())
        
        # Update progress
        await update_progress()
        
        # Memory cleanup
        del successful, failed, batch_urls
        gc.collect()
    
    # ═══════════════════════════════════════════
    # PHASE 2: Retry failed URLs ONCE
    # ═══════════════════════════════════════════
    if failed_urls:
        logger.info(f"🔄 Phase 2: Retrying {len(failed_urls)} failed URLs")
        stats['round'] = 2
        
        retry_index = 0
        retry_failed = []  # Don't retry these again
        
        while retry_index < len(failed_urls):
            batch_urls = failed_urls[retry_index:retry_index + BATCH_DOWNLOAD_SIZE]
            retry_index += BATCH_DOWNLOAD_SIZE
            stats['batch_num'] += 1
            
            logger.info(f"🔁 Retry batch {stats['batch_num']} ({len(batch_urls)} URLs)")
            successful, failed = await download_batch(batch_urls, temp_dir, url_to_username)
            
            # DO NOT retry failed URLs again (avoid infinite loop)
            retry_failed.extend(failed)
            
            # Add successful to pending
            for img_data in successful:
                username = img_data.get('username') or url_to_username.get(img_data['url'], 'Unknown')
                if username in pending_images_by_user:
                    pending_images_by_user[username].append(img_data)
            
            stats['downloaded'] += len(successful)
            stats['failed'] = len(failed_urls_seen) - stats['downloaded']  # Accurate failed count
            stats['retry_queue'] = len(failed_urls) - retry_index
            stats['pending_send'] = sum(len(imgs) for imgs in pending_images_by_user.values())
            
            # Send accumulated batches
            await send_accumulated_batches()
            stats['pending_send'] = sum(len(imgs) for imgs in pending_images_by_user.values())
            
            await update_progress()
            
            del successful, failed, batch_urls
            gc.collect()
        
        # Final failed count
        stats['failed'] = len(retry_failed)
        stats['retry_queue'] = 0
        logger.info(f"📊 Retry complete - {len(retry_failed)} URLs permanently failed")
    
    # ═══════════════════════════════════════════
    # PHASE 3: Send remaining images (< 10)
    # ═══════════════════════════════════════════
    logger.info("📤 Phase 3: Sending remaining images")
    
    for username, images in pending_images_by_user.items():
        if images:
            logger.info(f"📨 Sending final {len(images)} images for {username}")
            
            # Send in groups of 10 if possible
            while len(images) >= BATCH_SEND_SIZE:
                batch_to_send = images[:BATCH_SEND_SIZE]
                images[:BATCH_SEND_SIZE] = []
                
                user_topic = user_topic_ids.get(username) if user_topic_ids else topic_id
                batch_num = user_batch_nums[username]
                
                success = await send_image_batch_pyrogram(
                    batch_to_send, username, chat_id, user_topic, batch_num
                )
                
                if success:
                    stats['sent'] += len(batch_to_send)
                    user_batch_nums[username] += 1
                
                cleanup_images(batch_to_send)
            
            # Send remaining (< 10) at the end
            if images:
                logger.info(f"📨 Sending final incomplete batch for {username}: {len(images)} images")
                user_topic = user_topic_ids.get(username) if user_topic_ids else topic_id
                
                # Pad to 10 or send as-is (Telegram allows 2-10 in media group)
                if len(images) >= 2:
                    try:
                        async with SEND_SEMAPHORE:
                            await asyncio.sleep(SEND_DELAY)
                            media = []
                            batch_num = user_batch_nums[username]
                            for i, img in enumerate(images):
                                if i == 0:
                                    caption = f"{username.replace('_', ' ')} - {batch_num} (final)"
                                    media.append(InputMediaPhoto(img['path'], caption=caption))
                                else:
                                    media.append(InputMediaPhoto(img['path']))
                            
                            if user_topic:
                                await bot.send_media_group(chat_id, media, reply_to_message_id=user_topic)
                            else:
                                await bot.send_media_group(chat_id, media)
                            
                            stats['sent'] += len(images)
                            logger.info(f"✅ Sent final batch for {username}")
                    except Exception as e:
                        logger.error(f"❌ Failed to send final batch for {username}: {str(e)}")
                
                cleanup_images(images)
    
    stats['pending_send'] = 0
    
    # ═══════════════════════════════════════════
    # CLEANUP
    # ═══════════════════════════════════════════
    logger.info("🧹 Cleaning up temporary directory")
    try:
        if os.path.exists(temp_dir):
            await aioshutil.rmtree(temp_dir)
    except Exception as e:
        logger.warning(f"⚠️ Cleanup error: {str(e)}")
    
    gc.collect()
    log_memory()
    
    return stats['downloaded'], stats['sent'], total_urls

# ───────────────────────────────
# ✅ FIXED TOPIC CREATION FUNCTION
# ───────────────────────────────
async def create_forum_topic(client: Client, chat_id: int, topic_name: str):
    """Create a forum topic and return its ID"""
    try:
        # Verify bot can access the chat
        try:
            chat = await client.get_chat(chat_id)
            logger.info(f"📣 Connected to chat: {chat.title}")
        except Exception:
            logger.info("ℹ️ Chat not found in cache. Sending handshake message...")
            await client.send_message(chat_id, "👋 Bot connected successfully!")
            chat = await client.get_chat(chat_id)
        
        # Create the forum topic
        peer = await client.resolve_peer(chat_id)
        random_id = random.randint(100000, 999999999)
        
        result = await client.invoke(
            CreateForumTopic(
                channel=peer,
                title=topic_name,
                random_id=random_id,
                icon_color=0xFFD700  # optional: gold color
            )
        )
        
        # Extract topic_id
        topic_id = None
        for update in result.updates:
            if hasattr(update, "message") and hasattr(update.message, "id"):
                topic_id = update.message.id
                break
        
        if not topic_id:
            logger.error("⚠️ Could not detect topic_id. Check permissions.")
            return None
        
        logger.info(f"🆕 Topic created: {topic_name} (ID: {topic_id})")
        return topic_id
        
    except Exception as e:
        logger.error(f"❌ Error creating topic '{topic_name}': {str(e)}")
        return None

# ───────────────────────────────
# 🔍 HELPER: GET CHAT ID
# ───────────────────────────────
@bot.on_message(filters.command("getid"))
async def get_chat_id(client: Client, message: Message):
    """Get the chat ID of current chat or forwarded message"""
    if message.forward_from_chat:
        chat = message.forward_from_chat
        await message.reply(
            f"**Forwarded Chat Info:**\n"
            f"• Title: {chat.title}\n"
            f"• ID: `{chat.id}`\n"
            f"• Type: {chat.type}\n"
            f"• Is Forum: {getattr(chat, 'is_forum', False)}"
        )
    else:
        chat = message.chat
        await message.reply(
            f"**Current Chat Info:**\n"
            f"• Title: {getattr(chat, 'title', 'Private Chat')}\n"
            f"• ID: `{chat.id}`\n"
            f"• Type: {chat.type}\n"
            f"• Is Forum: {getattr(chat, 'is_forum', False)}"
        )

# ───────────────────────────────
# BOT HANDLER
# ───────────────────────────────
@bot.on_message(filters.command("down") & filters.private)
async def handle_down(client: Client, message: Message):
    """
    Main bot handler for /down command
    
    Usage:
        /down <url> [-g <chat_id>] [-t <topic_id>] [-ct <topic_name>] [-u]
        
    Options:
        -g: Target group/channel chat ID
        -t: Existing topic ID to reply to
        -ct: Create new topic with this name
        -u: Create separate topics per user
    """
    try:
        text = message.text.strip()
        args = text.split()[1:] if len(text.split()) > 1 else []

        # Parse arguments
        url = None
        target_chat_id = message.chat.id
        target_topic_id = None
        create_topic_name = None
        create_topics_per_user = False

        i = 0
        while i < len(args):
            if args[i] == '-g' and i + 1 < len(args):
                try:
                    target_chat_id = int(args[i + 1])
                except ValueError:
                    await message.reply(f"❌ Invalid chat ID: {args[i + 1]}")
                    return
                i += 2
            elif args[i] == '-t' and i + 1 < len(args):
                try:
                    target_topic_id = int(args[i + 1])
                except ValueError:
                    await message.reply(f"❌ Invalid topic ID: {args[i + 1]}")
                    return
                i += 2
            elif args[i] == '-ct' and i + 1 < len(args):
                create_topic_name = args[i + 1]
                i += 2
            elif args[i] == '-u':
                create_topics_per_user = True
                i += 1
            else:
                if not url:
                    url = args[i]
                i += 1

        # Get HTML content
        html_content = ""
        if message.reply_to_message:
            if message.reply_to_message.document:
                # Download document
                try:
                    file_path = await message.reply_to_message.download()
                    with open(file_path, 'r', encoding='utf-8') as f:
                        html_content = f.read()
                    os.remove(file_path)
                    logger.info("✅ HTML document downloaded and parsed")
                except Exception as e:
                    await message.reply(f"❌ Error reading document: {str(e)}")
                    return
            elif message.reply_to_message.text:
                # Assume URLs in text
                urls = re.findall(r'https?://[^\s]+', message.reply_to_message.text)
                if urls:
                    # Fetch first URL as HTML
                    html_content = await fetch_html(urls[0])
                    if not html_content:
                        await message.reply(f"❌ Failed to fetch HTML from: {urls[0]}")
                        return
        elif url:
            html_content = await fetch_html(url)
            if not html_content:
                await message.reply(f"❌ Failed to fetch HTML from: {url}")
                return

        if not html_content:
            await message.reply("❌ No valid HTML content found. Please provide a URL or reply to an HTML document.")
            return

        # Extract data
        media_data, usernames, year_counts = extract_media_data_from_html(html_content)
        if not media_data:
            await message.reply("❌ Failed to extract media data from HTML. Check if the HTML contains 'const mediaData = {...}'")
            return

        username_images = create_username_images(media_data, usernames)
        username_images, all_urls = filter_and_deduplicate_urls(username_images)

        total_media = sum(len(urls) for urls in username_images.values())
        total_images = len(all_urls)

        if total_images == 0:
            await message.reply("❌ No valid images/media found after filtering.")
            return

        logger.info(f"📊 Found {total_images} media items for {len(username_images)} users")

        # Handle topic creation with improved logic
        user_topic_ids = {}
        if create_topics_per_user:
            logger.info(f"🆕 Creating {len(username_images)} topics for users...")
            topics_created = 0
            for username in username_images.keys():
                topic_name = f"{username.replace('_', ' ')}"
                topic_id = await create_forum_topic(client, target_chat_id, topic_name)
                if topic_id:
                    user_topic_ids[username] = topic_id
                    topics_created += 1
                await asyncio.sleep(0.5)  # Small delay between topic creations
            logger.info(f"✅ Created {topics_created}/{len(username_images)} topics")
        elif create_topic_name:
            logger.info(f"🆕 Creating single topic: {create_topic_name}")
            target_topic_id = await create_forum_topic(client, target_chat_id, create_topic_name)
            if not target_topic_id:
                await message.reply(f"❌ Failed to create topic '{create_topic_name}'. Check bot permissions.")
                return

        # Send initial progress
        progress_msg = await message.reply("🚀 Starting download process...")

        # Process batches
        total_downloaded, total_sent, total_filtered = await process_batches(
            username_images, target_chat_id, target_topic_id, user_topic_ids, progress_msg
        )

        # Final stats
        failed_count = total_filtered - total_downloaded
        success_rate = int((total_downloaded / total_filtered * 100)) if total_filtered > 0 else 0
        
        stats = f"""✅ **Download Complete!**

📊 **Final Statistics:**
━━━━━━━━━━━━━━━━━━━━
• 📁 Total Media Found: {total_media}
• 🔍 Filtered & Valid: {total_filtered}
• ✅ Successfully Downloaded: {total_downloaded}
• 📤 Successfully Sent: {total_sent}
• ❌ Failed: {failed_count}
• 📈 Success Rate: {success_rate}%
━━━━━━━━━━━━━━━━━━━━
• 👥 Users Processed: {len(username_images)}
• 🗂️ Topics Created: {len(user_topic_ids) if user_topic_ids else (1 if target_topic_id else 0)}"""

        try:
            await progress_msg.edit(stats)
        except Exception as e:
            logger.error(f"❌ Failed to update final stats: {str(e)}")
    
    except Exception as e:
        logger.error(f"❌ Critical error in handle_down: {str(e)}", exc_info=True)
        try:
            await message.reply(f"❌ An error occurred: {str(e)}")
        except:
            pass

# ───────────────────────────────
# MAIN
# ───────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=run_fastapi, daemon=True).start()
    bot.run()
