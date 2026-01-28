from Settings import getSettings
from pathlib import Path
from itertools import islice
import mutagen
from mutagen import easymp4, mp3, mp4, flac, wave
import webbrowser
import time
import random
import requests
from bs4 import BeautifulSoup
import logging
import pyperclip
import subprocess
import shutil
import xml.etree.ElementTree as ET
import os
import psutil
import platform
import urllib.parse
import re
import json
from BookStatus import skipBook, failBook, checkOutputExists

# Selenium imports - optional, used for auto-fetch when DuckDuckGo blocks requests
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# Global Selenium browser instance (persists across searches)
_selenium_driver = None

# URL cache for reducing API calls and handling rate limits
_url_cache = {}
_cache_file = Path(__file__).parent / ".url_cache.json"
_cache_ttl = 3600 * 24  # 24 hour cache TTL

def _load_cache():
    """Load cache from disk."""
    global _url_cache
    try:
        if _cache_file.exists():
            with open(_cache_file, 'r', encoding='utf-8') as f:
                _url_cache = json.load(f)
    except Exception:
        _url_cache = {}

def _save_cache():
    """Save cache to disk."""
    try:
        with open(_cache_file, 'w', encoding='utf-8') as f:
            json.dump(_url_cache, f)
    except Exception:
        pass

def _get_cached(url):
    """Get cached response for URL if valid."""
    if url in _url_cache:
        entry = _url_cache[url]
        if time.time() - entry.get('time', 0) < _cache_ttl:
            return entry.get('content')
    return None

def _set_cached(url, content):
    """Cache response for URL."""
    _url_cache[url] = {'content': content, 'time': time.time()}
    _save_cache()

# Load cache on module import
_load_cache()

log = logging.getLogger(__name__)
settings = None
conversions = []

def _write_to_log_file(message):
    """Write directly to file handler only, bypassing console output."""
    import logging
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.stream.write(message + "\n")
            handler.stream.flush()

def console_print(message):
    """Print to console and also write to log file if enabled."""
    print(message, flush=True)
    _write_to_log_file(message)

def console_input(prompt):
    """Prompt for input and log the response."""
    response = input(prompt)
    _write_to_log_file(f"{prompt}{response}")
    return response

def open_url_cross_platform(url):
    """Robustly open a URL in the user's default browser, with fallbacks for all major OSes."""
    try:
        system = platform.system()
        # On Linux, prefer xdg-open in a fully detached subprocess FIRST to ensure persistence
        if system == "Linux":
            try:
                log.debug("Linux detected; launching via xdg-open (detached)")
                subprocess.Popen(
                    ['xdg-open', url],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
                return
            except Exception:
                log.debug("xdg-open failed; attempting Python webbrowser as fallback")
                try:
                    if webbrowser.open(url, new=2):
                        return
                except Exception:
                    pass

            log.debug("Default browser open failed; attempting additional platform-specific fallbacks")
            # As a last resort on Linux, try known controllers (still may be tied to parent)
            for browser in ['firefox', 'google-chrome', 'chromium', 'brave-browser']:
                try:
                    webbrowser.get(browser).open(url, new=2)
                    return
                except Exception:
                    continue

            log.error("Could not open a web browser. Please open this URL manually: " + url)
            return

        # Non-Linux platforms
        # 1) Honor $BROWSER if set
        browser_env = os.environ.get('BROWSER')
        if browser_env:
            try:
                log.debug(f"Using BROWSER controller: {browser_env}")
                webbrowser.get(browser_env).open(url, new=2)
                return
            except Exception:
                pass

        # 2) Use Python's default (respects system defaults)
        try:
            if webbrowser.open(url, new=2):
                log.debug("Opened URL via Python webbrowser default")
                return
        except Exception:
            pass

        # 3) Minimal platform-specific fallbacks
        log.debug("Default browser open failed; attempting platform-specific fallback")

        if system == "Windows":
            try:
                os.startfile(url)  # type: ignore[attr-defined]
                return
            except Exception:
                pass
        elif system == "Darwin":
            try:
                subprocess.Popen(['open', url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
                return
            except Exception:
                pass
        log.error("Could not open a web browser. Please open this URL manually: " + url)
    except Exception as e:
        log.error(f"Failed to open browser: {e}. Please open this URL manually: {url}")

def loadSettings():
    global settings
    settings = getSettings()

def cleanAuthorForPath(author):
    """
    Clean author name for use in file/folder paths.
    - Extracts the first actual author (skipping credit-only entries like "Foreword by Name")
    - Strips credits like '- foreword', '- contributor', etc.
    - Removes invalid path characters

    For folder structure, we only use the primary author. The full author list
    is preserved in the audiobook metadata.
    """
    if not author:
        return author

    # Credit prefixes that indicate someone is NOT the main author
    credit_prefixes = r'^(foreword|forward|introduction|preface|afterword|epilogue|read|narrated|translated|edited)\s+by\s+'
    # Credit suffixes that indicate someone is NOT the main author (e.g., "Name - foreword")
    credit_suffixes = r'\s+-\s*(foreword|forward|contributor|editor|introduction|afterword|translator|narrator|preface|epilogue)\s*$'

    # Split by comma, semicolon, slash, or ampersand to get individual author entries
    # This handles: "Author1, Author2", "Author1; Author2", "Author1/Author2", "Author1 & Author2"
    segments = re.split(r'\s*[,;/&]\s*', author)

    # Find the first segment that is NOT a credit attribution
    cleaned = None
    for segment in segments:
        seg = segment.strip()
        # Skip entries that are credit attributions like "Foreword by John Smith"
        if re.match(credit_prefixes, seg, re.IGNORECASE):
            continue
        # Skip entries with credit suffixes like "Sheryl Sandberg - foreword"
        if re.search(credit_suffixes, seg, re.IGNORECASE):
            continue
        cleaned = seg
        break

    # If all segments were credits, use the first one anyway (edge case)
    if cleaned is None and segments:
        cleaned = segments[0].strip()

    if not cleaned:
        return author

    # Strip any credit suffix from this author: "Name - foreword" -> "Name"
    credit_roles = r'(foreword|forward|contributor|editor|introduction|afterword|translator|narrator|preface)'
    cleaned = re.sub(r'\s+-\s+' + credit_roles + r'\s*$', '', cleaned, flags=re.IGNORECASE)

    # Strip credential suffixes to normalize author names (M.D. vs MD, Ph.D. vs PhD, etc.)
    # This prevents "Daniel J. Siegel M.D." and "Daniel J. Siegel MD" from being different folders
    # Also strips professional licenses/certifications like MA, MFT, LPC, etc.
    credentials = r',?\s*(M\.?D\.?|Ph\.?D\.?|D\.?O\.?|J\.?D\.?|Ed\.?D\.?|Psy\.?D\.?|D\.?Min\.?|D\.?D\.?S\.?|R\.?N\.?|L\.?M\.?F\.?T\.?|L\.?C\.?S\.?W\.?|M\.?F\.?T\.?|L\.?P\.?C\.?|L\.?M\.?H\.?C\.?|L\.?P\.?C\.?C\.?|M\.?A\.?|M\.?S\.?|M\.?B\.?A\.?|M\.?S\.?W\.?|B\.?A\.?|B\.?S\.?|C\.?P\.?A\.?|Jr\.?|Sr\.?|III|II|IV)\s*$'
    # Apply multiple times to strip multiple credentials (e.g., "Name MA MFT")
    prev_cleaned = None
    while prev_cleaned != cleaned:
        prev_cleaned = cleaned
        cleaned = re.sub(credentials, '', cleaned, flags=re.IGNORECASE).strip()

    # Remove invalid path characters (Windows: < > : " / \ | ? *)
    # Keep apostrophes, commas, hyphens, and periods - they're valid in paths
    cleaned = re.sub(r'[<>"|?:*]', '', cleaned)

    # Clean up any double spaces or trailing/leading whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    return cleaned

def cleanTitleForPath(title):
    """
    Clean title for use in file/folder paths.
    - Removes invalid path characters (Windows: < > : " / \ | ? *)
    - Removes control characters (tabs, newlines)
    - Preserves apostrophes, commas, hyphens, and periods
    """
    if not title:
        return title

    # Remove invalid path characters and control characters
    cleaned = re.sub(r'[<>"|?:*\t\n\r]', '', title)

    # Clean up any double spaces or trailing/leading whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    return cleaned

class Metadata:
    def __init__(self):
        self.author = ""
        self.authors = []
        self.title = ""
        self.summary = ""
        self.subtitle = ""
        self.narrator = ""
        self.narrators = []
        self.publisher = ""
        self.publishYear = ""
        self.genres = []
        self.isbn = ""
        self.asin = ""
        self.series = ""
        self.seriesMulti = []
        self.volumeNumber = ""
        self.bookPath = ""
        self.coverUrl = ""  # URL to cover image (from Audible)

def findCoverImage(folder):
    """
    Search for cover image in a folder using common naming patterns.
    Returns the Path to the cover image if found, None otherwise.

    Search priority:
    1. cover.jpg / cover.png
    2. folder.jpg / folder.png
    3. *-Cover.jpg / *-Cover.png (case insensitive)
    4. *_cover.jpg / *_cover.png (case insensitive)
    5. If only one image file exists, use it
    """
    folder = Path(folder)
    if not folder.exists():
        return None

    # Priority 1: cover.jpg/png
    for ext in ['.jpg', '.jpeg', '.png']:
        coverPath = folder / f"cover{ext}"
        if coverPath.exists():
            log.debug(f"Found cover image: {coverPath}")
            return coverPath

    # Priority 2: folder.jpg/png
    for ext in ['.jpg', '.jpeg', '.png']:
        folderPath = folder / f"folder{ext}"
        if folderPath.exists():
            log.debug(f"Found folder image: {folderPath}")
            return folderPath

    # Priority 3 & 4: *-Cover.* or *_cover.* (case insensitive)
    for pattern in ['*-Cover.*', '*_cover.*', '*-cover.*', '*_Cover.*']:
        matches = list(folder.glob(pattern))
        for match in matches:
            if match.suffix.lower() in ['.jpg', '.jpeg', '.png']:
                log.debug(f"Found cover image by pattern: {match}")
                return match

    # Priority 5: If only one image file exists, use it
    imageFiles = []
    for ext in ['*.jpg', '*.jpeg', '*.png']:
        imageFiles.extend(folder.glob(ext))

    if len(imageFiles) == 1:
        log.debug(f"Found single image file: {imageFiles[0]}")
        return imageFiles[0]

    log.debug(f"No cover image found in: {folder}")
    return None


def copyCoverImage(sourceFolder, destFolder):
    """
    Find and copy cover image from source to destination folder.
    Renames to cover.jpg or cover.png for consistency.

    Returns the path to the copied cover, or None if no cover found.
    """
    coverPath = findCoverImage(sourceFolder)
    if not coverPath:
        return None

    destFolder = Path(destFolder)
    destFolder.mkdir(parents=True, exist_ok=True)

    # Determine destination filename based on extension
    ext = coverPath.suffix.lower()
    if ext in ['.jpg', '.jpeg']:
        destName = 'cover.jpg'
    elif ext == '.png':
        destName = 'cover.png'
    else:
        destName = f'cover{ext}'

    destPath = destFolder / destName

    try:
        shutil.copy(coverPath, destPath)
        log.info(f"Copied cover image to: {destPath}")
        return destPath
    except Exception as e:
        log.warning(f"Failed to copy cover image: {e}")
        return None


class Conversion:
    def __init__(self, file, track, type, md, sourceFolderPath=None):
        self.file = file
        self.track = track
        self.type = type
        self.md = md
        self.sourceFolderPath = sourceFolderPath


def getTitle(track):
    log.debug("Extracting title from track")

    # For audiobooks, prefer album (book title) over title (track/chapter name)
    if isinstance(track, mp3.EasyMP3) or isinstance(track, easymp4.EasyMP4):
        if 'album' in track and track['album'] != "":
            return track['album'][0]
        elif 'title' in track and track['title'] != "":
            return track['title'][0]
        else:
            log.debug("No title found. Returning empty string")
            return ""
    elif isinstance(track, mp3.MP3):
        if 'TALB' in track and track['TALB'] != "":
            return track['TALB']
        elif 'TIT2' in track and track['TIT2'] != "":
            return track['TIT2']
        else:
            log.debug("No title found. Returning empty string")
            return ""
    elif isinstance(track, mp4.MP4):
        if '\xa9alb' in track and track['\xa9alb'] != "":
            return track['\xa9alb']
        elif '\xa9nam' in track and track['\xa9nam'] != "":
            return track['\xa9nam']
        else:
            log.debug("No title found. Returning empty string")
            return ""
    elif isinstance(track, flac.FLAC):
        # FLAC uses Vorbis comments (case-insensitive keys stored uppercase)
        if 'album' in track and track['album']:
            return track['album'][0]
        elif 'title' in track and track['title']:
            return track['title'][0]
        else:
            log.debug("No title found in FLAC. Returning empty string")
            return ""
    elif isinstance(track, wave.WAVE):
        # WAVE can have ID3 tags
        if track.tags:
            if 'TIT2' in track.tags:
                return str(track.tags['TIT2'])
            elif 'TALB' in track.tags:
                return str(track.tags['TALB'])
        log.debug("No title found in WAVE. Returning empty string")
        return ""

    else:
        filename = getattr(track, 'filename', 'unknown')
        log.error(f"Unable to get title - unsupported format {type(track).__name__}: {filename}")
        return ""


def getAuthor(track):
    log.debug("Extracting author from track")

    if isinstance(track, mp3.EasyMP3) or isinstance(track, easymp4.EasyMP4):
        # Check albumartist first (author), then artist (may have narrator in old files)
        if 'albumartist' in track and track['albumartist'] != "":
            return track['albumartist'][0]
        elif 'artist' in track and track['artist'] != '':
            return track['artist'][0]
        elif 'composer' in track and track['composer'] != "":
            return track['composer'][0]
        elif 'lyricist' in track and track['lyricist'] != "":
            return track['lyricist'][0]
        else:
            log.debug("No author found. Returning empty string")
            return ""
    elif isinstance(track, mp3.MP3):
        if 'TPE1' in track and track['TPE1'] != "":
            return track['TPE1']
        elif 'TCOM' in track and track['TCOM'] != "":
            return track['TCOM']
        elif 'TPE2' in track and track['TPE2'] != "":
            return track['TPE2']
        elif 'TEXT' in track and track['TEXT'] != "":
            return track['TEXT']
        else:
            log.debug("No author found. Returning empty string")
            return ""
    elif isinstance(track, mp4.MP4):
        if '\xa9ART' in track and track['\xa9ART'] != "":
            return track['\xa9ART']
        elif 'soco' in track and track['soco'] != "":
            return track['soco']
        elif 'aART' in track and track['aART'] != "":
            return track['aART']
        else:
            log.debug("No author found. Returning empty string")
            return ""
    elif isinstance(track, flac.FLAC):
        # FLAC uses Vorbis comments
        if 'albumartist' in track and track['albumartist']:
            return track['albumartist'][0]
        elif 'artist' in track and track['artist']:
            return track['artist'][0]
        elif 'composer' in track and track['composer']:
            return track['composer'][0]
        else:
            log.debug("No author found in FLAC. Returning empty string")
            return ""
    elif isinstance(track, wave.WAVE):
        # WAVE can have ID3 tags
        if track.tags:
            if 'TPE2' in track.tags:
                return str(track.tags['TPE2'])
            elif 'TPE1' in track.tags:
                return str(track.tags['TPE1'])
            elif 'TCOM' in track.tags:
                return str(track.tags['TCOM'])
        log.debug("No author found in WAVE. Returning empty string")
        return ""
    else:
        filename = getattr(track, 'filename', 'unknown')
        log.error(f"Unable to get author - unsupported format {type(track).__name__}: {filename}")
        return ""


def getNarrator(track):
    """Extract narrator from track metadata."""
    log.debug("Extracting narrator from track")

    if isinstance(track, mp3.EasyMP3) or isinstance(track, easymp4.EasyMP4):
        # EasyMP3/EasyMP4 don't have standard narrator field, check if custom registered
        try:
            if 'narrator' in track and track['narrator']:
                return track['narrator'][0]
        except:
            pass
        return ""
    elif isinstance(track, mp3.MP3):
        # Check TXXX frames for narrator
        for key in track.keys():
            if key.startswith('TXXX:') and 'narrator' in key.lower():
                return str(track[key])
        return ""
    elif isinstance(track, mp4.MP4):
        # Check for narrator in MP4 tags
        if '\xa9nrt' in track and track['\xa9nrt']:
            return track['\xa9nrt'][0] if isinstance(track['\xa9nrt'], list) else str(track['\xa9nrt'])
        # Check freeform tags
        for key in track.keys():
            if 'narrator' in key.lower():
                val = track[key]
                if isinstance(val, list) and val:
                    return val[0].decode('utf-8') if isinstance(val[0], bytes) else str(val[0])
                return str(val)
        return ""
    else:
        return ""


def getSeries(track):
    """Extract series name from track metadata."""
    log.debug("Extracting series from track")

    if isinstance(track, mp3.EasyMP3) or isinstance(track, easymp4.EasyMP4):
        try:
            if 'series' in track and track['series']:
                return track['series'][0]
        except:
            pass
        return ""
    elif isinstance(track, mp3.MP3):
        # Check TXXX frames for series
        for key in track.keys():
            if key.startswith('TXXX:') and 'series' in key.lower():
                return str(track[key])
        return ""
    elif isinstance(track, mp4.MP4):
        # Check freeform tags for series
        for key in track.keys():
            if 'series' in key.lower() and 'index' not in key.lower():
                val = track[key]
                if isinstance(val, list) and val:
                    return val[0].decode('utf-8') if isinstance(val[0], bytes) else str(val[0])
                return str(val)
        return ""
    else:
        return ""


def getDescription(track):
    """Extract description/summary from track metadata."""
    log.debug("Extracting description from track")

    if isinstance(track, mp3.EasyMP3):
        # EasyMP3 doesn't expose TXXX frames, need to access underlying ID3
        try:
            from mutagen.id3 import ID3
            id3_tags = ID3(track.filename)
            for key in id3_tags.keys():
                if key.startswith('TXXX:') and 'description' in key.lower():
                    return str(id3_tags[key].text[0]) if id3_tags[key].text else ""
            # Also check COMM (comment) frames
            for key in id3_tags.keys():
                if key.startswith('COMM'):
                    return str(id3_tags[key].text[0]) if id3_tags[key].text else ""
        except:
            pass
        return ""
    elif isinstance(track, easymp4.EasyMP4):
        try:
            if 'description' in track and track['description']:
                return track['description'][0]
        except:
            pass
        return ""
    elif isinstance(track, mp3.MP3):
        # Check TXXX frames for description
        for key in track.keys():
            if key.startswith('TXXX:') and 'description' in key.lower():
                return str(track[key])
        # Also check COMM (comment) frames
        for key in track.keys():
            if key.startswith('COMM'):
                return str(track[key])
        return ""
    elif isinstance(track, mp4.MP4):
        # Check for description in MP4 tags
        if '\xa9des' in track and track['\xa9des']:
            return track['\xa9des'][0] if isinstance(track['\xa9des'], list) else str(track['\xa9des'])
        # Check freeform tags
        for key in track.keys():
            if 'description' in key.lower() or 'summary' in key.lower():
                val = track[key]
                if isinstance(val, list) and val:
                    return val[0].decode('utf-8') if isinstance(val[0], bytes) else str(val[0])
                return str(val)
        return ""
    else:
        return ""


def getYear(track):
    """Extract release year from track metadata."""
    log.debug("Extracting year from track")

    if isinstance(track, mp3.EasyMP3) or isinstance(track, easymp4.EasyMP4):
        try:
            if 'date' in track and track['date']:
                # May be full date or just year
                return track['date'][0][:4] if len(track['date'][0]) >= 4 else track['date'][0]
        except:
            pass
        return ""
    elif isinstance(track, mp3.MP3):
        # Check TYER (year) or TDRC (recording date)
        if 'TYER' in track:
            return str(track['TYER'])[:4]
        if 'TDRC' in track:
            return str(track['TDRC'])[:4]
        return ""
    elif isinstance(track, mp4.MP4):
        # Check for date in MP4 tags
        if '\xa9day' in track and track['\xa9day']:
            val = track['\xa9day'][0] if isinstance(track['\xa9day'], list) else str(track['\xa9day'])
            return val[:4] if len(val) >= 4 else val
        return ""
    else:
        return ""


def assessMetadata(track):
    """
    Check what metadata fields are missing from the track.
    Returns a dict with:
      - 'complete': True if all required fields are present
      - 'missing': list of missing field names

    For fetchUpdate mode, we only fetch metadata if complete=False.
    Required: author, title, cover
    Optional (tracked but not required): description, year
    """
    missing = []
    optional_missing = []

    author = getAuthor(track)
    title = getTitle(track)
    description = getDescription(track)
    year = getYear(track)

    # Required fields
    if not author or author.strip() == "":
        missing.append('author')
    if not title or title.strip() == "":
        missing.append('title')

    # Optional fields (tracked for info but don't block completion)
    if not description or description.strip() == "":
        optional_missing.append('description')
    if not year or year.strip() == "":
        optional_missing.append('year')

    # Check for cover art
    has_cover = False
    try:
        if isinstance(track, easymp4.EasyMP4):
            from mutagen.mp4 import MP4
            mp4_raw = MP4(track.filename)
            has_cover = 'covr' in mp4_raw and len(mp4_raw['covr']) > 0
        elif isinstance(track, (mp3.EasyMP3, mp3.MP3)):
            from mutagen.id3 import ID3
            try:
                id3_raw = ID3(track.filename)
                has_cover = any(key.startswith('APIC') for key in id3_raw.keys())
            except:
                has_cover = False
        elif isinstance(track, mp4.MP4):
            has_cover = 'covr' in track and len(track['covr']) > 0
    except Exception as e:
        log.debug(f"Error checking cover art: {e}")
        has_cover = False

    if not has_cover:
        missing.append('cover')

    return {
        'complete': len(missing) == 0,
        'missing': missing + optional_missing,  # Report all missing for logging
        'required_missing': missing,  # Only required fields
        'optional_missing': optional_missing,  # Optional fields
        'author': author,
        'title': title,
        'description': description,
        'year': year,
        'has_cover': has_cover
    }


class CachedResponse:
    """Mock response object for cached content."""
    def __init__(self, content, status_code=200):
        self.text = content
        self.content = content.encode('utf-8') if isinstance(content, str) else content
        self.status_code = status_code
        self.ok = status_code == 200
        self.encoding = 'utf-8'

    def json(self):
        return json.loads(self.text)

    def raise_for_status(self):
        if not self.ok:
            raise Exception(f"Status code: {self.status_code}")

def GETpage(url, use_cache=True):
    # Check cache first
    if use_cache:
        cached = _get_cached(url)
        if cached is not None:
            log.info(f"GET page (cached): {url}")
            return CachedResponse(cached)

    log.info("GET page: " + url)
    timer = 2
    page = None
    while True:
        try:
            page = requests.get(url)
            break
        except Exception as e:
            if timer == 2:
                #loading
                time.sleep(timer)
                timer *= 1.5
            elif timer >= 10:
                log.error("metadata shows failed, aborting GET")
                return None

    if page is None:
        return None

    if page.status_code != requests.codes.ok:
        log.error("Status code not OK, aborting GET")
        return None

    try:
        page.raise_for_status()
        # Cache successful response
        if use_cache:
            _set_cached(url, page.text)
        return page
    except Exception as e:
        log.error("Raise for status failed, aborting GET")
        return None
    
def parseAudibleMd(info, md):
    log.debug("Parsing audible metadata")

    try: #authors (multiple supported)
        md.authors = []
        authors_raw = info.get('authors', [])
        if isinstance(authors_raw, list) and len(authors_raw) > 0:
            for author in authors_raw:
                name = None
                if isinstance(author, dict):
                    name = author.get('name') or author.get('display_name')
                elif isinstance(author, str):
                    name = author
                if name:
                    md.authors.append(name)
            if len(md.authors) > 0:
                md.author = md.authors[0]
        else:
            log.debug("No authors found in audible JSON")
    except Exception as e:
        log.debug("Exeption parsing author in audible JSON")

    try: #title
        md.title = info['title']
    except Exception as e:
        log.debug("Exeption parsing title in audible JSON")


    try: #summary
        rawSummary = BeautifulSoup(info['publisher_summary'], 'html.parser')
        md.summary = rawSummary.getText()
    except Exception as e:
        log.debug("Exeption parsing summary in audible JSON")


    try: #subtitle
        md.subtitle = info['subtitle']
    except Exception as e:
        log.debug("Exeption parsing subtitle in audible JSON")


    try: #narrators
        if len(info['narrators']) == 0:
            log.debug("No narrators found")
        else:
            md.narrator = info['narrators'][0]['name']

            for n in info['narrators']:
                md.narrators.append(n['name'])

        md.narrator = info['narrators'][0]['name']

    except Exception as e:
        log.debug("Exeption parsing narrator in audible JSON")


    try: #publisher
        md.publisher = info['publisher_name']
    except Exception as e:
        log.debug("Exeption parsing publisher in audible JSON")


    try: #publish year
        md.publishYear = info['release_date'][:4]
    except Exception as e:
        log.debug("Exeption parsing release year in audible JSON")


    try: #genres (multiple supported)
        genres: list[str] = []
        # Common fields where genres may appear in Audible product JSON
        # 1) thesaurus_subject_keywords: ["Fantasy", "Epic" ...]
        tsk = info.get('thesaurus_subject_keywords')
        if isinstance(tsk, list):
            genres.extend([g for g in tsk if isinstance(g, str) and g])

        # 2) genres: can be ["Fantasy", ...] or [{"name": "Fantasy"}, ...]
        if not genres and 'genres' in info:
            g = info.get('genres')
            if isinstance(g, list):
                for item in g:
                    if isinstance(item, str) and item:
                        genres.append(item)
                    elif isinstance(item, dict):
                        name = item.get('name') or item.get('title') or item.get('display_name')
                        if name:
                            genres.append(name)

        # 3) category_ladders: [[{"name": "Fiction"}, {"name": "Fantasy"}], ...]
        if not genres and 'category_ladders' in info:
            ladders = info.get('category_ladders')
            if isinstance(ladders, list):
                for ladder in ladders:
                    if isinstance(ladder, list):
                        for node in ladder:
                            if isinstance(node, dict):
                                name = node.get('name') or node.get('display_name')
                                if name:
                                    genres.append(name)

        # Deduplicate while preserving order
        seen = set()
        unique_genres = []
        for g in genres:
            if g not in seen:
                seen.add(g)
                unique_genres.append(g)
        md.genres = unique_genres
    except Exception as e:
        log.debug("Exeption parsing genres in audible JSON")


    try: #series
        md.series = info['series'][0]['title']
    except Exception as e:
        log.debug("Exeption parsing series in audible JSON")


    try: #volume num
        md.volumeNumber = info['series'][0]['sequence']
    except Exception as e:
        log.debug("Exeption parsing volume number in audible JSON")

    try: #asin
        md.asin = info['asin']
    except Exception as e:
        log.debug("Exeption parsing ASIN in audible JSON")

    try: #cover image URL
        # Audible API returns product_images with various sizes
        # Try to get the highest quality image (500 or larger)
        product_images = info.get('product_images')
        if product_images:
            # product_images is a dict like {"500": "url", "1024": "url", ...}
            # Get the largest available size
            sizes = sorted([int(s) for s in product_images.keys() if s.isdigit()], reverse=True)
            if sizes:
                md.coverUrl = product_images[str(sizes[0])]
                log.debug(f"Found cover URL at size {sizes[0]}: {md.coverUrl}")
    except Exception as e:
        log.debug(f"Exception parsing cover URL in audible JSON: {e}")


def parseGoodreadsMd(soup, md):
    log.debug("Parsing goodreads metadata")
    try:
        md.title = soup.find('h1', class_="Text Text__title1").text.strip()
    except Exception as e:
        log.debug("Exeption parsing title from goodreads")

    # Authors (multiple)
    try:
        md.authors = []
        author_spans = soup.find_all('span', class_="ContributorLink__name")
        for span in author_spans:
            name = span.get_text(strip=True)
            if name:
                md.authors.append(name)
        if len(md.authors) > 0:
            md.author = md.authors[0]
        else:
            # Fallback: try older structure
            a_links = soup.select('a.ContributorLink__name, a.authorName')
            for a in a_links:
                name = a.get_text(strip=True)
                if name:
                    md.authors.append(name)
            if len(md.authors) > 0:
                md.author = md.authors[0]
    except Exception as e:
        log.debug("Exeption parsing authors from goodreads")

    try:    #if multiple classes, use wrapper div instead
        md.summary = soup.find('span', class_="Formatted").text.strip()
    except Exception as e:
        log.debug("Exeption parsing summary from goodreads")
    
    # Publisher, Publish Year, ISBN
    try:
        details_section = soup.select_one('[data-testid="bookDetails"]') or soup.find('div', id='bookDataBox')
        details_text = details_section.get_text(" ", strip=True) if details_section else soup.get_text(" ", strip=True)

        # Publish year (First published ... YYYY) or (Published ... YYYY)
        m_year = re.search(r'(?:First\s+published|Published)[^\d]*(\d{4})', details_text, re.IGNORECASE)
        if m_year:
            md.publishYear = m_year.group(1)

        # Publisher (after 'by ')
        m_pub = re.search(r'Published.*?by\s+([^\d,]+)', details_text, re.IGNORECASE)
        if m_pub:
            md.publisher = m_pub.group(1).strip()

        # ISBN (10 or 13, possibly with hyphens)
        m_isbn = re.search(r'ISBN(?:-13)?:?\s*([0-9Xx\-]{10,17})', details_text)
        if m_isbn:
            candidate = m_isbn.group(1).replace('-', '').strip()
            if 10 <= len(candidate) <= 13:
                md.isbn = candidate
    except Exception as e:
        log.debug("Exeption parsing publisher/publish year/ISBN from goodreads")


    # Genres (multiple)
    try:
        genres: list[str] = []
        # New Goodreads layout often lists genres as tag buttons
        # Strategy 1: look for data-testid containers and anchors
        containers = soup.select('[data-testid="genresList"], [data-testid="bookMeta"]')
        for cont in containers:
            for a in cont.find_all('a'):
                text = a.get_text(strip=True)
                if text and len(text) < 60:  # avoid long non-genre texts
                    genres.append(text)
        # Strategy 2: look for tag buttons
        if not genres:
            for a in soup.select('a.Button--tag, a.ActionLink--genre, a[href*="/genres/"]'):
                text = a.get_text(strip=True)
                if text:
                    genres.append(text)
        # Deduplicate while preserving order
        seen = set()
        unique_genres = []
        for g in genres:
            if g not in seen:
                seen.add(g)
                unique_genres.append(g)
        md.genres = unique_genres
    except Exception as e:
        log.debug("Exeption parsing genres from goodreads")


        
    try:
        temp = soup.find("div", class_="BookPageTitleSection__title").find_next().text
        md.series = temp[ : temp.find('#') - 1]
    except Exception as e:
        log.debug("Exeption parsing series from goodreads")


    try:
        temp = soup.find("div", class_="BookPageTitleSection__title").find_next().text
        md.volumeNumber = temp[temp.find('#') + 1: ]
    except Exception as e:
        log.debug("Exeption parsing volume number from goodreads")


def parseSpotifyMd(soup, md):
    """
    Parse metadata from Spotify pages (albums, audiobooks, shows/podcasts).
    Spotify embeds JSON-LD with @type: "MusicAlbum", "Audiobook", or "PodcastSeries".
    """
    log.debug("Parsing Spotify metadata")

    # Try to find JSON-LD data first (most reliable)
    try:
        ld_script = soup.find('script', type='application/ld+json')
        if ld_script:
            ld_data = json.loads(ld_script.string)
            ld_type = ld_data.get('@type', '')
            log.debug(f"Spotify JSON-LD type: {ld_type}")

            # Get title (name field in JSON-LD)
            if 'name' in ld_data:
                md.title = ld_data['name']
                log.debug(f"Spotify title from JSON-LD: {md.title}")

            # Get author - different fields based on type
            # Audiobooks have 'author', albums have 'byArtist'
            if 'author' in ld_data:
                # Audiobook format - author can be object or list
                author_data = ld_data['author']
                if isinstance(author_data, list):
                    md.author = author_data[0].get('name', '') if author_data else ''
                elif isinstance(author_data, dict):
                    md.author = author_data.get('name', '')
                else:
                    md.author = str(author_data)
                log.debug(f"Spotify author from JSON-LD: {md.author}")

            # Get narrator for audiobooks (stored in readBy)
            if 'readBy' in ld_data:
                narrator_data = ld_data['readBy']
                if isinstance(narrator_data, list):
                    md.narrator = narrator_data[0].get('name', '') if narrator_data else ''
                elif isinstance(narrator_data, dict):
                    md.narrator = narrator_data.get('name', '')
                log.debug(f"Spotify narrator from JSON-LD: {md.narrator}")

            # Get release date
            if 'datePublished' in ld_data:
                md.publishYear = ld_data['datePublished'][:4]  # Get year from YYYY-MM-DD
                log.debug(f"Spotify publish year: {md.publishYear}")

            # Get description
            if 'description' in ld_data:
                md.summary = ld_data['description']
                log.debug(f"Spotify description found ({len(md.summary)} chars)")
    except Exception as e:
        log.debug(f"Exception parsing JSON-LD from Spotify: {e}")

    # Parse artist from meta description or page title (fallback for albums)
    # Format: "Listen to {title} on Spotify · album · {artist} · {year} · {tracks} songs"
    # Or title: "{title} - Album by {artist} | Spotify"
    # Or audiobook: "{title} - Audiobook by {author} | Spotify"
    if not md.author:
        try:
            # Try og:title or page title first
            og_title = soup.find('meta', property='og:title')
            if og_title and og_title.get('content'):
                title_content = og_title['content']
                # Pattern: "Title - Album by Artist" or "Title - Audiobook by Author"
                match = re.search(r'(?:Album|album|Audiobook|audiobook)\s+by\s+(.+?)(?:\s*\||\s*$)', title_content)
                if match:
                    md.author = match.group(1).strip()
                    log.debug(f"Spotify author from og:title: {md.author}")

            # Fallback to meta description
            if not md.author:
                meta_desc = soup.find('meta', attrs={'name': 'description'}) or soup.find('meta', property='og:description')
                if meta_desc and meta_desc.get('content'):
                    desc = meta_desc['content']
                    # Pattern: "Listen to X on Spotify · album · Artist Name · YYYY"
                    match = re.search(r'·\s*(?:album|Album)\s*·\s*([^·]+)', desc)
                    if match:
                        md.author = match.group(1).strip()
                        log.debug(f"Spotify author from description: {md.author}")
        except Exception as e:
            log.debug(f"Exception parsing author from Spotify: {e}")

    # Get title from og:title if not already set
    if not md.title:
        try:
            og_title = soup.find('meta', property='og:title')
            if og_title and og_title.get('content'):
                title_content = og_title['content']
                # Extract title before " - Album by" or " - Audiobook by" or " | Spotify"
                title_match = re.match(r'^(.+?)\s*(?:-\s*(?:Album|Audiobook)\s+by|·|\|)', title_content)
                if title_match:
                    md.title = title_match.group(1).strip()
                else:
                    md.title = title_content.replace(' | Spotify', '').strip()
                log.debug(f"Spotify title from og:title: {md.title}")
        except Exception as e:
            log.debug(f"Exception parsing title from Spotify: {e}")

    # Get cover image from og:image
    try:
        og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            md.coverUrl = og_image['content']
            log.debug(f"Spotify cover URL: {md.coverUrl}")
    except Exception as e:
        log.debug(f"Exception parsing cover from Spotify: {e}")


def fetchSpotifyWithSelenium(spotifyUrl, md):
    """
    Fetch Spotify metadata using Selenium to render JavaScript.
    Updates the provided Metadata object with title, author, cover, etc.

    Args:
        spotifyUrl: The Spotify URL (album, show, or audiobook)
        md: Metadata object to populate

    Returns:
        bool: True if successfully fetched metadata, False otherwise
    """
    if not SELENIUM_AVAILABLE:
        log.debug("Selenium not available for Spotify fetch")
        return False

    driver = getSeleniumDriver()
    if driver is None:
        return False

    try:
        log.info(f"Fetching Spotify metadata with Selenium: {spotifyUrl}")
        driver.get(spotifyUrl)

        # Wait for page to load (Spotify uses React, needs JS rendering)
        time.sleep(4)

        # Try to get page source and parse
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        # Spotify renders author in a span with data-testid="entityAuthor"
        if not md.author:
            try:
                author_elem = soup.find('span', attrs={'data-testid': 'entityAuthor'})
                if author_elem:
                    md.author = author_elem.get_text(strip=True)
                    log.debug(f"Spotify author from data-testid (Selenium): {md.author}")
            except Exception as e:
                log.debug(f"Error finding entityAuthor: {e}")

        # Also try data-testid="creator-link" for albums
        if not md.author:
            try:
                creator_elem = soup.find(attrs={'data-testid': 'creator-link'})
                if creator_elem:
                    md.author = creator_elem.get_text(strip=True)
                    log.debug(f"Spotify author from creator-link (Selenium): {md.author}")
            except Exception as e:
                log.debug(f"Error finding creator-link: {e}")

        # Try JSON-LD
        try:
            ld_script = soup.find('script', type='application/ld+json')
            if ld_script:
                ld_data = json.loads(ld_script.string)
                ld_type = ld_data.get('@type', '')
                log.debug(f"Spotify JSON-LD type (Selenium): {ld_type}")

                if 'name' in ld_data and not md.title:
                    md.title = ld_data['name']
                    log.debug(f"Spotify title from JSON-LD (Selenium): {md.title}")

                if 'author' in ld_data and not md.author:
                    author_data = ld_data['author']
                    if isinstance(author_data, list):
                        md.author = author_data[0].get('name', '') if author_data else ''
                    elif isinstance(author_data, dict):
                        md.author = author_data.get('name', '')
                    else:
                        md.author = str(author_data)
                    log.debug(f"Spotify author from JSON-LD (Selenium): {md.author}")

                if 'readBy' in ld_data and not md.narrator:
                    narrator_data = ld_data['readBy']
                    if isinstance(narrator_data, list):
                        md.narrator = narrator_data[0].get('name', '') if narrator_data else ''
                    elif isinstance(narrator_data, dict):
                        md.narrator = narrator_data.get('name', '')
                    log.debug(f"Spotify narrator from JSON-LD (Selenium): {md.narrator}")

                if 'datePublished' in ld_data and not md.publishYear:
                    md.publishYear = ld_data['datePublished'][:4]
                    log.debug(f"Spotify publish year (Selenium): {md.publishYear}")

                if 'description' in ld_data and not md.summary:
                    md.summary = ld_data['description']
        except Exception as e:
            log.debug(f"Error parsing JSON-LD from Selenium: {e}")

        # Try og: meta tags for title
        if not md.title:
            try:
                og_title = soup.find('meta', property='og:title')
                if og_title and og_title.get('content'):
                    title_content = og_title['content']
                    # Parse "Title - Album by Artist | Spotify" format
                    title_match = re.match(r'^(.+?)\s*(?:-\s*(?:Album|Audiobook)\s+by|·|\|)', title_content)
                    if title_match:
                        md.title = title_match.group(1).strip()
                    else:
                        md.title = title_content.replace(' | Spotify', '').strip()
                    log.debug(f"Spotify title from og:title (Selenium): {md.title}")

                    # Extract author from og:title if available
                    if not md.author:
                        match = re.search(r'(?:Album|album|Audiobook|audiobook)\s+by\s+(.+?)(?:\s*\||\s*$)', title_content)
                        if match:
                            md.author = match.group(1).strip()
                            log.debug(f"Spotify author from og:title (Selenium): {md.author}")
            except Exception as e:
                log.debug(f"Error parsing og:title from Selenium: {e}")

        # Get cover image
        if not md.coverUrl:
            try:
                og_image = soup.find('meta', property='og:image')
                if og_image and og_image.get('content'):
                    md.coverUrl = og_image['content']
                    log.debug(f"Spotify cover URL (Selenium): {md.coverUrl}")
            except Exception as e:
                log.debug(f"Error parsing og:image from Selenium: {e}")

        return bool(md.title and md.author)

    except Exception as e:
        log.error(f"Selenium Spotify fetch error: {e}")
        return False


def normalizeForComparison(text):
    """Normalize text for fuzzy comparison - lowercase, remove punctuation, collapse whitespace."""
    if not text:
        return ""
    # Lowercase
    text = text.lower()
    # Remove common subtitle indicators and everything after
    for sep in [': ', ' - ', ' – ']:
        if sep in text:
            text = text.split(sep)[0]
    # Remove punctuation and extra whitespace
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def calculateMatchConfidence(fileAuthor, fileTitle, audibleAuthor, audibleTitle):
    """
    Calculate confidence score (0-100) that the Audible result matches the file.
    Returns tuple: (confidence_score, match_details)
    """
    fileAuthorNorm = normalizeForComparison(fileAuthor)
    fileTitleNorm = normalizeForComparison(fileTitle)
    audibleAuthorNorm = normalizeForComparison(audibleAuthor)
    audibleTitleNorm = normalizeForComparison(audibleTitle)

    # Title matching (weighted higher)
    titleScore = 0
    if fileTitleNorm and audibleTitleNorm:
        # Exact match
        if fileTitleNorm == audibleTitleNorm:
            titleScore = 100
        # One contains the other
        elif fileTitleNorm in audibleTitleNorm or audibleTitleNorm in fileTitleNorm:
            titleScore = 85
        # Check word overlap
        else:
            fileWords = set(fileTitleNorm.split())
            audibleWords = set(audibleTitleNorm.split())
            if fileWords and audibleWords:
                overlap = len(fileWords & audibleWords)
                total = len(fileWords | audibleWords)
                titleScore = int((overlap / total) * 100) if total > 0 else 0

    # Author matching
    authorScore = 0
    if fileAuthorNorm and audibleAuthorNorm:
        # Exact match
        if fileAuthorNorm == audibleAuthorNorm:
            authorScore = 100
        # One contains the other (handles "John Smith" vs "John Smith PhD")
        elif fileAuthorNorm in audibleAuthorNorm or audibleAuthorNorm in fileAuthorNorm:
            authorScore = 90
        # Check if last name matches (common for author matching)
        else:
            fileLastName = fileAuthorNorm.split()[-1] if fileAuthorNorm else ""
            audibleLastName = audibleAuthorNorm.split()[-1] if audibleAuthorNorm else ""
            if fileLastName and audibleLastName and fileLastName == audibleLastName:
                authorScore = 70
            else:
                # Word overlap for multiple authors
                fileWords = set(fileAuthorNorm.split())
                audibleWords = set(audibleAuthorNorm.split())
                if fileWords and audibleWords:
                    overlap = len(fileWords & audibleWords)
                    total = len(fileWords | audibleWords)
                    authorScore = int((overlap / total) * 100) if total > 0 else 0

    # Combined score: title is more important (60% title, 40% author)
    # If we have no file metadata to compare, be less confident
    if not fileAuthorNorm and not fileTitleNorm:
        combinedScore = 0
    elif not fileAuthorNorm:
        combinedScore = titleScore
    elif not fileTitleNorm:
        combinedScore = authorScore
    else:
        combinedScore = int(titleScore * 0.6 + authorScore * 0.4)

    details = f"title:{titleScore}% author:{authorScore}%"
    return (combinedScore, details)


def tryAutoFetchAudible(searchText, fileAuthor, fileTitle, confidenceThreshold=80):
    """
    Try to automatically fetch Audible metadata by scraping Google search results.

    Args:
        searchText: The search query to use
        fileAuthor: Author from the file's existing metadata
        fileTitle: Title from the file's existing metadata
        confidenceThreshold: Minimum confidence score to auto-accept (0-100)

    Returns:
        tuple: (Metadata or None, confidence_score, match_details)
        - If confidence >= threshold, returns populated Metadata
        - If confidence < threshold or error, returns None
    """
    try:
        # Search DuckDuckGo for Audible results (Google blocks scraping)
        searchQuery = f"site:audible.com/pd/ {searchText}"
        encodedQuery = urllib.parse.quote(searchQuery)
        searchURL = f"https://html.duckduckgo.com/html/?q={encodedQuery}"

        log.info(f"Auto-fetch: searching DuckDuckGo for '{searchText}'...")

        # Check cache first
        cached = _get_cached(searchURL)
        if cached is not None:
            log.info("Auto-fetch: using cached DuckDuckGo results")
            response = CachedResponse(cached)
        else:
            # User agent - use a recent Chrome version to look like a real browser
            # Note: Don't send Accept-Encoding to avoid compression issues with response.text
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }

            response = requests.get(searchURL, headers=headers, timeout=10)

            # Cache successful responses
            if response.status_code == 200:
                _set_cached(searchURL, response.text)

        # Save HTML for debugging
        cache_path = Path(__file__).parent / "cache.html"
        # Log response info for debugging
        log.debug(f"Auto-fetch: response status={response.status_code}, encoding={response.encoding}, content-type={response.headers.get('content-type', 'unknown')}")

        # Force UTF-8 encoding if not detected properly
        if response.encoding is None or response.encoding == 'ISO-8859-1':
            response.encoding = 'utf-8'

        try:
            cache_path.write_text(response.text, encoding='utf-8')
            log.debug(f"Auto-fetch: saved response HTML to {cache_path}")
        except Exception as e:
            # If text decoding fails, save raw bytes
            cache_path.write_bytes(response.content)
            log.debug(f"Auto-fetch: saved raw response bytes to {cache_path} (text decode failed: {e})")

        # Pause after search to avoid rate limiting / CAPTCHA triggers
        # Random delay between 5-15 seconds to increase randomness
        time.sleep(random.uniform(4, 9))

        if not response.ok:
            log.info(f"Auto-fetch: DuckDuckGo search failed with status {response.status_code}")
            return (None, 0, "DuckDuckGo search failed")

        # Check for CAPTCHA page - be specific to avoid false positives
        # DuckDuckGo CAPTCHA page contains "Unfortunately, bots use DuckDuckGo too"
        # Also check raw bytes in case encoding detection failed
        response_text = response.text
        is_captcha = 'Unfortunately, bots use DuckDuckGo' in response_text
        if not is_captcha:
            # Try checking raw bytes (handles encoding issues)
            try:
                is_captcha = b'Unfortunately, bots use DuckDuckGo' in response.content
            except:
                pass
        # Also detect if response looks like binary garbage (compression not decoded)
        if not is_captcha and len(response_text) > 100:
            # If first 100 chars have lots of non-printable characters, likely compressed
            non_printable = sum(1 for c in response_text[:100] if ord(c) < 32 or ord(c) > 126)
            if non_printable > 20:
                log.info(f"Auto-fetch: Response appears to be compressed/binary ({non_printable} non-printable chars)")
                is_captcha = True  # Assume it's CAPTCHA if we can't decode

        if is_captcha:
            log.info("Auto-fetch: DuckDuckGo rate-limited, skipping auto-fetch (browser will show results)")
            # Don't try to retry - the browser will show results even though requests is blocked
            # Just return and let the manual flow handle it
            return (None, 0, "DuckDuckGo rate-limited")

        # Parse HTML to find first Audible link
        soup = BeautifulSoup(response.text, 'html.parser')


        # DuckDuckGo wraps all links in redirects like //duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.audible.com%2Fpd%2F...
        # We need to extract the actual URL from the uddg parameter
        audibleLink = None
        for link in soup.find_all('a', href=True):
            href = link['href']

            # Check if this is a DuckDuckGo redirect link
            if 'duckduckgo.com/l/' in href and 'uddg=' in href:
                # Extract the actual URL from the uddg parameter
                try:
                    # Parse the redirect URL
                    parsed_redirect = urllib.parse.urlparse(href)
                    query_params = urllib.parse.parse_qs(parsed_redirect.query)
                    if 'uddg' in query_params:
                        actual_url = query_params['uddg'][0]
                        # Check if this is an Audible product link
                        if 'audible.com/pd/' in actual_url:
                            audibleLink = actual_url
                            break
                except Exception as e:
                    log.debug(f"Auto-fetch: error parsing redirect URL: {e}")
                    continue
            # Also check for direct links (in case format changes)
            elif href.startswith('http') and 'audible.com/pd/' in href:
                audibleLink = href
                break

        if not audibleLink:
            log.info("Auto-fetch: no Audible link found in search results")
            return (None, 0, "No Audible link in results")

        log.info(f"Auto-fetch: found Audible link: {audibleLink}")

        # Extract ASIN from the URL
        parsed = urllib.parse.urlparse(audibleLink)
        path_parts = [p for p in parsed.path.split('/') if p]
        log.debug(f"Auto-fetch: URL path parts: {path_parts}")
        asin = None
        for part in reversed(path_parts):
            m = re.match(r'^[0-9A-Z]{10}$', part, re.IGNORECASE)
            if m:
                asin = m.group(0).upper()
                break

        if not asin:
            log.info(f"Auto-fetch: could not extract ASIN from URL: {audibleLink}")
            return (None, 0, "Could not extract ASIN")

        log.debug(f"Auto-fetch: extracted ASIN: {asin}")

        # Fetch from Audible API
        paramRequest = "?response_groups=contributors,product_attrs,product_desc,product_extended_attrs,series,media"
        targetUrl = f"https://api.audible.com/1.0/catalog/products/{asin}" + paramRequest
        page = GETpage(targetUrl)

        if page is None or not getattr(page, "ok", False):
            log.info(f"Auto-fetch: Audible API request failed for ASIN {asin}")
            return (None, 0, "Audible API failed")

        data = page.json()
        product = data.get('product')
        if not product:
            log.info("Auto-fetch: no product in Audible response")
            return (None, 0, "No product in response")

        # Parse metadata
        md = Metadata()
        md.asin = asin
        parseAudibleMd(product, md)

        if not md.title or not md.author:
            log.info(f"Auto-fetch: Audible result missing title or author (title={md.title}, author={md.author})")
            return (None, 0, "Missing title/author")

        # Calculate confidence
        confidence, details = calculateMatchConfidence(fileAuthor, fileTitle, md.author, md.title)
        log.info(f"Auto-fetch: '{md.title}' by {md.author} (confidence: {confidence}%, {details})")

        if confidence >= confidenceThreshold:
            return (md, confidence, details)
        else:
            log.info(f"Auto-fetch: confidence {confidence}% below threshold {confidenceThreshold}%, will prompt user")
            # Return metadata even with low confidence so caller can check if output exists
            return (md, confidence, details)

    except Exception as e:
        log.info(f"Auto-fetch error: {e}")
        return (None, 0, f"Error: {e}")


def getSeleniumDriver():
    """Get or create a persistent Selenium browser instance."""
    global _selenium_driver

    if not SELENIUM_AVAILABLE:
        log.info("Selenium not available - install with 'pip install selenium'")
        return None

    if _selenium_driver is not None:
        try:
            # Check if driver is still valid
            _selenium_driver.title
            return _selenium_driver
        except:
            # Driver died, need to recreate
            _selenium_driver = None

    try:
        log.info("Starting Selenium browser for auto-fetch (minimized)...")
        # Use Chrome with options to prevent detection
        from selenium.webdriver.chrome.options import Options
        chrome_options = Options()
        # Don't run headless - user needs to solve CAPTCHAs
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        # Set a normal user agent
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        # Start window off-screen - will be moved on-screen when CAPTCHA is detected
        chrome_options.add_argument("--window-position=-2000,-2000")

        _selenium_driver = webdriver.Chrome(options=chrome_options)
        # Remove webdriver flag
        _selenium_driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        log.info("Selenium browser started successfully")
        return _selenium_driver
    except Exception as e:
        log.info(f"Failed to start Selenium browser: {e}")
        return None


def closeSeleniumDriver():
    """Close the Selenium browser instance."""
    global _selenium_driver
    if _selenium_driver is not None:
        try:
            _selenium_driver.quit()
        except:
            pass
        _selenium_driver = None


def tryAutoFetchAudibleSelenium(searchText, fileAuthor, fileTitle, confidenceThreshold=80):
    """
    Try to automatically fetch Audible metadata using Selenium browser.
    This allows user to solve CAPTCHAs in the same browser session.

    Args:
        searchText: The search query to use
        fileAuthor: Author from the file's existing metadata
        fileTitle: Title from the file's existing metadata
        confidenceThreshold: Minimum confidence score to auto-accept (0-100)

    Returns:
        tuple: (Metadata or None, confidence_score, match_details)
    """
    try:
        driver = getSeleniumDriver()
        if driver is None:
            return (None, 0, "Selenium not available")

        # Search DuckDuckGo for Audible results
        searchQuery = f"site:audible.com/pd/ {searchText}"
        encodedQuery = urllib.parse.quote(searchQuery)
        searchURL = f"https://html.duckduckgo.com/html/?q={encodedQuery}"

        # Check cache first - avoid Selenium if we have cached results
        cached = _get_cached(searchURL)
        if cached:
            log.info(f"Auto-fetch (Selenium): using cached DuckDuckGo results for '{searchText}'")
            page_source = cached
            captcha_was_needed = False
            from_cache = True
        else:
            from_cache = False
            log.info(f"Auto-fetch (Selenium): searching DuckDuckGo for '{searchText}'...")
            log.info("Auto-fetch (Selenium): browser window opened - loading search results...")
            driver.get(searchURL)

            # Wait for results to load - check for either results or CAPTCHA
            # Random delay between 5-15 seconds to increase randomness
            wait_time = random.uniform(4, 9)
            log.info(f"Auto-fetch (Selenium): waiting {wait_time:.1f}s for page to load...")
            time.sleep(wait_time)

            # Check if CAPTCHA appeared
            captcha_was_needed = False
            page_source = driver.page_source
            if 'Unfortunately, bots use DuckDuckGo' in page_source:
                captcha_was_needed = True
                # Move window on-screen and bring to foreground for user to solve CAPTCHA
                log.info("Auto-fetch (Selenium): CAPTCHA detected - bringing browser to foreground...")
                try:
                    driver.set_window_position(100, 100)
                    driver.maximize_window()
                except:
                    pass
                log.info("Auto-fetch (Selenium): CAPTCHA detected - please solve it in the browser window")
                console_print("CAPTCHA detected! Please solve it in the browser window...")
                console_print("Waiting up to 60 seconds for you to solve the CAPTCHA...")

                # Wait for user to solve CAPTCHA (up to 60 seconds)
                for i in range(60):
                    time.sleep(1)
                    if i % 10 == 9:  # Log every 10 seconds
                        log.info(f"Auto-fetch (Selenium): still waiting for CAPTCHA... ({60 - i - 1}s remaining)")
                    page_source = driver.page_source
                    if 'Unfortunately, bots use DuckDuckGo' not in page_source:
                        log.info("Auto-fetch (Selenium): CAPTCHA solved, hiding window...")
                        try:
                            driver.set_window_position(-2000, -2000)
                        except:
                            pass
                        break
                else:
                    log.info("Auto-fetch (Selenium): CAPTCHA timeout - skipping auto-fetch")
                    return (None, 0, "CAPTCHA timeout")
            else:
                log.info("Auto-fetch (Selenium): no CAPTCHA detected, proceeding automatically...")

            # Cache the successful search results
            _set_cached(searchURL, page_source)

        # Parse the page for Audible links
        soup = BeautifulSoup(page_source, 'html.parser')

        # Log link counts for debugging
        all_links = [link['href'] for link in soup.find_all('a', href=True)]
        audible_related = [l for l in all_links if 'audible' in l.lower() and 'duckduckgo.com' not in l]
        log.info(f"Auto-fetch (Selenium): found {len(all_links)} total links, {len(audible_related)} audible-related")

        # DuckDuckGo wraps all links in redirects like //duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.audible.com%2Fpd%2F...
        # We need to extract the actual URL from the uddg parameter
        audibleLink = None
        for link in soup.find_all('a', href=True):
            href = link['href']

            # Check if this is a DuckDuckGo redirect link
            if 'duckduckgo.com/l/' in href and 'uddg=' in href:
                # Extract the actual URL from the uddg parameter
                try:
                    # Parse the redirect URL
                    parsed_redirect = urllib.parse.urlparse(href)
                    query_params = urllib.parse.parse_qs(parsed_redirect.query)
                    if 'uddg' in query_params:
                        actual_url = query_params['uddg'][0]
                        # Check if this is an Audible product link
                        if 'audible.com/pd/' in actual_url:
                            audibleLink = actual_url
                            break
                except Exception as e:
                    log.debug(f"Auto-fetch (Selenium): error parsing redirect URL: {e}")
                    continue
            # Also check for direct links (in case format changes)
            elif href.startswith('http') and 'audible.com/pd/' in href:
                audibleLink = href
                break

        if not audibleLink:
            log.info("Auto-fetch (Selenium): no Audible link found in search results")
            if not captcha_was_needed:
                closeSeleniumDriver()
            return (None, 0, "No Audible link in results")

        log.info(f"Auto-fetch (Selenium): found Audible link: {audibleLink}")

        # Extract ASIN from the URL
        parsed = urllib.parse.urlparse(audibleLink)
        path_parts = [p for p in parsed.path.split('/') if p]
        asin = None
        for part in reversed(path_parts):
            m = re.match(r'^[0-9A-Z]{10}$', part, re.IGNORECASE)
            if m:
                asin = m.group(0).upper()
                break

        if not asin:
            log.info(f"Auto-fetch (Selenium): could not extract ASIN from URL: {audibleLink}")
            if not captcha_was_needed:
                closeSeleniumDriver()
            return (None, 0, "Could not extract ASIN")

        log.debug(f"Auto-fetch (Selenium): extracted ASIN: {asin}")

        # Fetch from Audible API
        paramRequest = "?response_groups=contributors,product_attrs,product_desc,product_extended_attrs,series,media"
        targetUrl = f"https://api.audible.com/1.0/catalog/products/{asin}" + paramRequest
        page = GETpage(targetUrl)

        if page is None or not getattr(page, "ok", False):
            log.info(f"Auto-fetch (Selenium): Audible API request failed for ASIN {asin}")
            if not captcha_was_needed:
                closeSeleniumDriver()
            return (None, 0, "Audible API failed")

        data = page.json()
        product = data.get('product')
        if not product:
            log.info("Auto-fetch (Selenium): no product in Audible response")
            if not captcha_was_needed:
                closeSeleniumDriver()
            return (None, 0, "No product in response")

        # Parse metadata
        md = Metadata()
        md.asin = asin
        parseAudibleMd(product, md)

        if not md.title or not md.author:
            log.info(f"Auto-fetch (Selenium): Audible result missing title or author")
            if not captcha_was_needed:
                closeSeleniumDriver()
            return (None, 0, "Missing title/author")

        # Calculate confidence
        confidence, details = calculateMatchConfidence(fileAuthor, fileTitle, md.author, md.title)
        log.info(f"Auto-fetch (Selenium): '{md.title}' by {md.author} (confidence: {confidence}%, {details})")

        # Close browser if no CAPTCHA was needed (user didn't have to interact)
        if not captcha_was_needed:
            log.info("Auto-fetch (Selenium): closing browser (no user interaction was needed)")
            closeSeleniumDriver()

        if confidence >= confidenceThreshold:
            return (md, confidence, details)
        else:
            log.info(f"Auto-fetch (Selenium): confidence {confidence}% below threshold {confidenceThreshold}%, will prompt user")
            # Return metadata even with low confidence so caller can check if output exists
            return (md, confidence, details)

    except Exception as e:
        log.info(f"Auto-fetch (Selenium) error: {e}")
        return (None, 0, f"Error: {e}")


# Sentinel value to indicate metadata fetch was deferred (needs user interaction)
METADATA_DEFERRED = "DEFERRED"

def fetchMetadata(file, track, autoOnly=False) -> Metadata:
    """
    Fetch metadata for an audio file.

    Args:
        file: Path to the audio file
        track: Mutagen track object
        autoOnly: If True, only try auto-fetch. If that fails, return DEFERRED instead of prompting.

    Returns:
        Metadata object if successful, None if skipped/failed, METADATA_DEFERRED if needs interaction and autoOnly=True
    """
    # Check for skip marker file in the book's directory or any parent folder
    checkDir = file.parent
    while checkDir and checkDir != checkDir.parent:  # Stop at root
        skipMarkerPath = checkDir / "ultimate-audio-skip.txt"
        if skipMarkerPath.exists():
            log.info(f"Skip marker found, skipping: {skipMarkerPath}")
            skipBook(file, "Directory marked with ultimate-audio-skip.txt")
            return None
        checkDir = checkDir.parent

    log.info("Fetching metadata")
    md = Metadata()
    md.title = getTitle(track)
    md.author = getAuthor(track)

    # If tags are missing, try to get author/title from folder structure
    # Expected structure: .../Author/Title/file.mp3
    if md.title == "" or md.author == "":
        # Try to extract from directory structure
        parentDir = file.parent.name  # e.g., "Modern Romance"
        grandparentDir = file.parent.parent.name if file.parent.parent else ""  # e.g., "Aziz Ansari"

        # Root folder names to exclude from being used as title/author
        rootFolders = ["AudioBooks", "AudioBooks2", "Audio Books", "ServerFolders", "Audiobooks", "audiobooks"]

        if md.title == "" and parentDir and parentDir not in rootFolders:
            md.title = parentDir
            log.info(f"No title tag, using folder name: {parentDir}")

        if md.author == "" and grandparentDir and grandparentDir not in rootFolders:
            md.author = grandparentDir
            log.info(f"No author tag, using parent folder name: {grandparentDir}")

    if md.title != "" and md.author != "":
        searchText = md.title + " - " + md.author
    elif md.title != "":
        searchText = md.title
    elif md.author != "":
        searchText = md.author
    else:
        searchText = file.stem

    # Try auto-fetch for Audible sources first
    lowConfidenceMd = None  # Store low-confidence result for output existence check
    if settings.fetch in ["audible", "all"]:
        log.info("Attempting auto-fetch from Audible...")
        autoMd, confidence, details = tryAutoFetchAudible(searchText, md.author, md.title)
        if autoMd is not None and confidence >= 80:
            log.info(f"Auto-accepted: '{autoMd.title}' by {autoMd.author} (confidence: {confidence}%)")
            return autoMd
        elif autoMd is not None and confidence > 0:
            log.info(f"Auto-fetch found result but confidence too low ({confidence}%), will check if output exists...")
            lowConfidenceMd = autoMd
        elif "rate-limited" in details.lower():
            # DuckDuckGo blocked requests - try Selenium instead (unless autoOnly mode)
            if autoOnly:
                log.info("Requests blocked by DuckDuckGo, deferring (autoOnly mode, skipping Selenium)")
            else:
                log.info("Requests blocked by DuckDuckGo, trying Selenium browser...")
                console_print("Opening Selenium browser (you may need to solve a CAPTCHA if one appears)...")
                autoMd, confidence, details = tryAutoFetchAudibleSelenium(searchText, md.author, md.title)
                if autoMd is not None and confidence >= 80:
                    log.info(f"Auto-accepted (Selenium): '{autoMd.title}' by {autoMd.author} (confidence: {confidence}%)")
                    return autoMd
                elif autoMd is not None and confidence > 0:
                    log.info(f"Auto-fetch (Selenium) found result but confidence too low ({confidence}%), will check if output exists...")
                    lowConfidenceMd = autoMd

    # If we have a low-confidence result, check if output already exists before prompting
    # If -CV mode, only .m4b counts as existing output
    if lowConfidenceMd is not None and lowConfidenceMd.author and lowConfidenceMd.title:
        cleanAuthor = cleanAuthorForPath(lowConfidenceMd.author)
        cleanTitle = cleanTitleForPath(lowConfidenceMd.title)
        potentialOutputPath = settings.output + f"/{cleanAuthor}/{cleanTitle}"
        existingFile = checkOutputExists(potentialOutputPath, lowConfidenceMd.title, requireM4B=settings.convert)
        if existingFile:
            log.info(f"Output already exists (from low-confidence auto-fetch): {existingFile.name}")
            skipBook(file, f"Output already exists: {existingFile.name}")
            return None

    # If autoOnly mode, don't prompt - return DEFERRED for later processing
    if autoOnly:
        log.info(f"Auto-fetch failed or low confidence - deferring for later: {file.name}")
        return METADATA_DEFERRED

    # Don't clear clipboard - it's annoying and can cause skip to not work

    # Construct search query with parentheses around site restrictions
    if settings.fetch == "audible":
        searchQuery = f"audible.com/pd/ {searchText}"
    elif settings.fetch == "goodreads":
        searchQuery = f"goodreads.com {searchText}"
    elif settings.fetch == "spotify":
        searchQuery = f"open.spotify.com {searchText}"
    elif settings.fetch == "all":
        searchQuery = f"(audible.com/pd/ OR goodreads.com OR open.spotify.com) {searchText}"

    # URL-encode the query
    encodedQuery = urllib.parse.quote(searchQuery)

    # Use a generic search URL that browsers may route to their default search engine
    # Many browsers intercept search URLs and use their configured default search engine
    # If the browser doesn't intercept, it will still perform the search on Google
    searchURL = f"https://www.google.com/search?q={encodedQuery}"

    # Clear clipboard before opening browser to prevent stale values (like old 'skip') from persisting
    pyperclip.copy("")

    open_url_cross_platform(searchURL)

    # Track what we've already seen to detect new copies
    seenClipboards = set()
    initialClip = pyperclip.paste()
    log.info(f"[DEBUG] Initial clipboard: {repr(initialClip[:100] if initialClip else 'empty')}")

    # Check if clipboard already has skip/skipalways or a valid URL - process immediately
    clipUpper = initialClip.strip().upper()
    if clipUpper == "SKIP":
        log.info("Clipboard already contains 'skip' - skipping book")
        pyperclip.copy("")  # Clear clipboard so skip doesn't apply to next book
        skipBook(file, "User skipped during metadata fetch")
        return None
    elif clipUpper == "SKIPALWAYS":
        log.info("Clipboard contains 'skipalways' - skipping book and marking directory")
        pyperclip.copy("")  # Clear clipboard
        # Write skip marker file to the book's directory
        skipMarkerPath = file.parent / "ultimate-audio-skip.txt"
        skipMarkerPath.write_text("This directory was marked to always skip during metadata fetch.\n")
        log.info(f"Created skip marker: {skipMarkerPath}")
        skipBook(file, "User skipped always during metadata fetch")
        return None

    # Add initial clipboard to seen set so we only process NEW copies
    # This prevents stale URLs from previous books being auto-processed
    seenClipboards.add(initialClip)

    # Show more path context: great-grandparent/grandparent/parent/file (Author/Book/Subfolder/file)
    great_grandparent = file.parent.parent.parent.name if file.parent.parent and file.parent.parent.parent else ""
    grandparent = file.parent.parent.name if file.parent.parent else ""
    if great_grandparent:
        log.info(f"Source file: {great_grandparent}/{grandparent}/{file.parent.name}/{file.name}")
    elif grandparent:
        log.info(f"Source file: {grandparent}/{file.parent.name}/{file.name}")
    else:
        log.info(f"Source file: {file.parent.name}/{file.name}")
    log.info("Search opened, copy the Audible/Goodreads/Spotify URL, 'skip' to skip once, or 'skipalways' to permanently skip this directory...")
    while True:
        time.sleep(1)
        currClipboard = pyperclip.paste()

        # Skip if we've already processed this clipboard content
        if currClipboard in seenClipboards:
            continue

        # Log what we got
        log.info(f"[DEBUG] New clipboard ({len(currClipboard)} chars): {repr(currClipboard[:100])}")

        # Mark as seen
        seenClipboards.add(currClipboard)

        # Check for skip/skipalways command (case-insensitive, with whitespace trimming)
        clipUpper = currClipboard.strip().upper()
        if clipUpper == "SKIP":
            pyperclip.copy("")  # Clear clipboard so skip doesn't apply to next book
            skipBook(file, "User skipped during metadata fetch")
            return None
        elif clipUpper == "SKIPALWAYS":
            pyperclip.copy("")  # Clear clipboard
            # Write skip marker file to the book's directory
            skipMarkerPath = file.parent / "ultimate-audio-skip.txt"
            skipMarkerPath.write_text("This directory was marked to always skip during metadata fetch.\n")
            log.info(f"Created skip marker: {skipMarkerPath}")
            skipBook(file, "User skipped always during metadata fetch")
            return None
        elif "audible.com" in currClipboard:
            log.debug("Audible URL captured: " + currClipboard)
            # Handle DuckDuckGo redirect URLs (e.g., https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.audible.com%2F...)
            workingUrl = currClipboard.strip()
            if "duckduckgo.com" in workingUrl and "uddg=" in workingUrl:
                try:
                    ddgParsed = urllib.parse.urlparse(workingUrl)
                    ddgQuery = urllib.parse.parse_qs(ddgParsed.query)
                    if 'uddg' in ddgQuery:
                        workingUrl = ddgQuery['uddg'][0]
                        log.debug(f"Extracted URL from DuckDuckGo redirect: {workingUrl}")
                except Exception as e:
                    log.warning(f"Failed to parse DuckDuckGo redirect: {e}")
            # Robustly extract ASIN from path or query, ignoring extra query params
            try:
                parsed = urllib.parse.urlparse(workingUrl)
                path_parts = [p for p in parsed.path.split('/') if p]
                asin_match = None
                # Search path segments from the end for a valid ASIN (10-char starting with 'B')
                for part in reversed(path_parts):
                    m = re.match(r'^[0-9A-Z]{10}$', part, re.IGNORECASE)
                    if m:
                        asin_match = m.group(0).upper()
                        break
                # Fallback to query parameter 'asin' if present
                if not asin_match:
                    qs = urllib.parse.parse_qs(parsed.query)
                    candidate = qs.get('asin', [None])[0]
                    if candidate and re.match(r'^[0-9A-Z]{10}$', candidate, re.IGNORECASE):
                        asin_match = candidate.upper()
                if not asin_match:
                    log.error("Unable to extract ASIN from Audible URL. Please copy a book page link and try again, or copy 'skip' to skip this book.")
                    # Don't overwrite clipboard - seenClipboards handles duplicates
                    log.info("Waiting for URL...")
                    continue
                md.asin = asin_match
            except Exception:
                log.exception("Error parsing Audible URL")
                # Don't overwrite clipboard - seenClipboards handles duplicates
                log.info("Waiting for URL...")
                continue

            paramRequest = "?response_groups=contributors,product_attrs,product_desc,product_extended_attrs,series,media"
            targetUrl = f"https://api.audible.com/1.0/catalog/products/{md.asin}" + paramRequest
            page = GETpage(targetUrl)
            if page is None or not getattr(page, "ok", False):
                log.error("Audible API request failed. Please copy a valid book page link, or copy 'skip' to skip.")
                # Don't overwrite clipboard - seenClipboards handles duplicates
                log.info("Waiting for URL...")
                continue

            try:
                data = page.json()
                product = data.get('product')
                if not product:
                    raise KeyError("No 'product' in response")
                parseAudibleMd(product, md)

                if not md.title or not md.author:
                    log.error("Audible link did not yield both title and author. Please copy a valid book page link, or copy 'skip' to skip.")
                    # Don't overwrite clipboard - seenClipboards handles duplicates
                    log.info("Waiting for URL...")
                    continue
                # Clear clipboard so this URL doesn't get picked up by the next book
                pyperclip.copy("")
                break
            except (json.JSONDecodeError, KeyError): #TODO this randomly started letting me copy the link for he who fights with monsters series. Did they change their API to send valid JSON for series? If so, maybe check the URL for /series instead of /p or whatever they use?
                log.error("Error reading Audible API. Perhaps this is a series/podcast or invalid link? Copy a book page link, or 'skip'.")
                # Don't overwrite clipboard - seenClipboards handles duplicates
                log.info("Waiting for URL...")
                continue



        elif "goodreads.com" in currClipboard:
            log.debug("Goodreads URL captured: " + currClipboard)
            page = GETpage(currClipboard)
            if page is None:
                log.error("Goodreads page request failed. Please copy a valid book page link, or copy 'skip' to skip.")
                # Don't overwrite clipboard - seenClipboards handles duplicates
                log.info("Waiting for URL...")
                continue
            soup = BeautifulSoup(page.text, 'html.parser')
            parseGoodreadsMd(soup, md)
            # Safety net: ensure required fields present
            if not md.title or not md.author:
                log.error("Goodreads link did not yield both title and author. Please copy a valid book page link, or copy 'skip' to skip.")
                # Don't overwrite clipboard - seenClipboards handles duplicates
                log.info("Waiting for URL...")
                continue
            # Clear clipboard so this URL doesn't get picked up by the next book
            pyperclip.copy("")
            break

        elif "spotify.com" in currClipboard:
            log.debug("Spotify URL captured: " + currClipboard)
            spotifyUrl = currClipboard.strip()

            # Use Spotify's oEmbed API for reliable title and cover (doesn't require JS)
            oembedUrl = f"https://open.spotify.com/oembed?url={spotifyUrl}"
            oembedResp = GETpage(oembedUrl)
            if oembedResp and oembedResp.ok:
                try:
                    oembedData = oembedResp.json()
                    if 'title' in oembedData:
                        md.title = oembedData['title']
                        log.debug(f"Spotify title from oEmbed: {md.title}")
                    if 'thumbnail_url' in oembedData:
                        md.coverUrl = oembedData['thumbnail_url']
                        log.debug(f"Spotify cover from oEmbed: {md.coverUrl}")
                except json.JSONDecodeError:
                    log.error("Failed to parse Spotify oEmbed response")

            # Try Selenium to get author, description, year (Spotify needs JS rendering)
            # Call Selenium if any of these fields are missing
            if SELENIUM_AVAILABLE and (not md.author or not md.summary or not md.publishYear):
                log.info("Trying Selenium for Spotify metadata (author/description/year)...")
                fetchSpotifyWithSelenium(spotifyUrl, md)

            # Fallback: prompt user for author if still missing
            if md.title and not md.author:
                log.warning(f"Spotify provided title '{md.title}' but no author")
                log.info("Copy the author name to clipboard, or 'skip' to skip:")
                while True:
                    time.sleep(1)
                    authorClip = pyperclip.paste()
                    if authorClip != currClipboard and authorClip.strip():
                        authorInput = authorClip.strip()
                        if authorInput.upper() == "SKIP":
                            pyperclip.copy("")
                            skipBook(file, "User skipped during Spotify metadata fetch")
                            return None
                        md.author = authorInput
                        log.info(f"Author set to: {md.author}")
                        break

            # Safety net: ensure required fields present
            if not md.title or not md.author:
                log.error("Spotify link did not yield both title and author. Please copy a valid album/track link, or copy 'skip' to skip.")
                log.info("Waiting for URL...")
                continue
            # Clear clipboard so this URL doesn't get picked up by the next book
            pyperclip.copy("")
            break

    return md



def getAudioFiles(folderPath, batch = -1, recurse = False, offset = 0):
    """
    Get audio files from a folder.

    Args:
        folderPath: Path to search
        batch: Number of files to return (-1 for all)
        recurse: Whether to search subdirectories
        offset: Number of files to skip (for batch continuation)

    Returns:
        List of file paths, or -1 if no files found
    """
    files = []

    if recurse:
        files.extend(list(folderPath.rglob("*.m4*")))  #.m4a, .m4b
        files.extend(list(folderPath.rglob("*.mp*")))  #.mp3, .mp4
        files.extend(list(folderPath.rglob("*.flac")))  #flac
        files.extend(list(folderPath.rglob("*.wav")))  #wav
    else:
        files.extend(list(folderPath.glob("*.m4*")))  #.m4a, .m4b
        files.extend(list(folderPath.glob("*.mp*")))  #.mp3, .mp4
        files.extend(list(folderPath.glob("*.flac")))  #flac
        files.extend(list(folderPath.glob("*.wav")))  #wav

    # Sort files for consistent ordering across batches
    files.sort(key=lambda f: str(f).lower())

    if len(files) == 0:
        return -1

    # Apply offset first
    if offset > 0:
        files = files[offset:]

    if batch == -1 or len(files) <= batch:
        return files
    else:
        return files[:batch]


#TODO .m4a is broken
def convertToM4B(file, type, md, settings, sourceFolderPath=None): #This is run parallel through ProcessPoolExecutor, which limits access to globals
    #When copying we create the new file in destination, otherwise the new file will be copied and there will be an extra original
    #When moving we convert in place and allow the move to be handled in EOF processing
    file = Path(file)  # Ensure file is a Path object (may be string after ProcessPoolExecutor pickling)
    originalTempFile = file  # Save original file path for cleanup (temp files in "Ultimate temp")
    log.info("Converting " + file.name + " to M4B")

    #apparently ffmpeg can't process special characters on input, but has no problem outputting them? So setting newPath with specials here works just fine.
    if md.title:
        newPath = Path(md.bookPath + "/" + cleanTitleForPath(md.title) + ".mp4")
    else:
        newPath = Path(md.bookPath + "/" + cleanTitleForPath(file.stem) + ".mp4")

    tempPath = newPath
    newPath = getUniquePath(newPath.with_suffix(".m4b").name, newPath.parent)

    if settings.move:
        file = sanitizeFile(file)
    else:
        folder, filename = os.path.split(str(file))
        copyFile = shutil.copy(str(file), os.path.join(folder, f"COPY{filename}"))
        file = sanitizeFile(copyFile)

    cmd = ['ffmpeg',
           '-i', str(file),  #input file (convert Path to string for subprocess)
           '-codec', 'copy', #copy audio streams instead of re-encoding
           '-vn',   #disable video
           # '-hide_banner', #suppress verbose progress output. Changes to the log level may make this redundant.
           # '-loglevel', 'error',
           '-loglevel', 'warning',
           '-stats',    #adds back the progress bar loglevel hides
           str(tempPath)]  #convert Path to string for subprocess
    
    
    if type == '.mp3':
        log.debug("Converting MP3 to M4B")
        try:
            # Read metadata from source mp3 before conversion
            sourceMp3 = mutagen.File(file, easy=True)

            subprocess.run(cmd, check=True)

            # Copy metadata from source mp3 to converted m4b
            log.debug("Copying metadata from source mp3 to converted m4b")
            try:
                convertedFile = mutagen.File(tempPath, easy=True)
                if convertedFile and sourceMp3:
                    # Copy common tags
                    for tag in ['artist', 'albumartist', 'album', 'title', 'date', 'genre']:
                        if tag in sourceMp3:
                            convertedFile[tag] = sourceMp3[tag]
                    convertedFile.save()
                    log.debug("Metadata copied to m4b successfully")

                    # Check for cover image and embed it
                    # Search in source folder first, then file's parent folder
                    coverPath = None
                    if sourceFolderPath:
                        coverPath = findCoverImage(sourceFolderPath)
                    if not coverPath:
                        coverPath = findCoverImage(Path(file).parent)

                    if coverPath:
                        try:
                            log.info(f"Embedding cover image: {coverPath}")
                            with open(coverPath, 'rb') as f:
                                coverData = f.read()

                            # tempPath is an mp4 file (m4b), so use MP4 tags
                            from mutagen.mp4 import MP4, MP4Cover
                            mp4File = MP4(tempPath)
                            # Determine format based on extension
                            if coverPath.suffix.lower() == '.png':
                                mp4File['covr'] = [MP4Cover(coverData, imageformat=MP4Cover.FORMAT_PNG)]
                            else:
                                mp4File['covr'] = [MP4Cover(coverData, imageformat=MP4Cover.FORMAT_JPEG)]
                            mp4File.save()
                            log.info("Cover image embedded in converted m4b")
                        except Exception as e:
                            log.warning(f"Failed to embed cover image: {e}")
                    else:
                        log.debug(f"No cover image found for: {file.name}")

            except Exception as e:
                log.warning(f"Failed to copy metadata to m4b file: {e}")

            file.unlink() #if not settings.move, a copy is created which this deletes. Nondestructive.
            # Delete original temp file if it exists and is different from working file
            if not settings.move and originalTempFile.exists() and originalTempFile != file:
                originalTempFile.unlink()
                log.debug(f"Deleted original temp file: {originalTempFile.name}")
            return tempPath.rename(newPath)

        except subprocess.CalledProcessError as e:
            failBook(file, "Conversion failed")
            return file

    elif type == '.mp4':
        log.debug("Converting MP4 to M4B")
        result = file.rename(newPath.with_suffix('.m4b')) #if not settings.move, a copy is created which this moves. Nondestructive.
        # Delete original temp file if it exists and is different from working file
        if not settings.move and originalTempFile.exists() and originalTempFile != file:
            originalTempFile.unlink()
            log.debug(f"Deleted original temp file: {originalTempFile.name}")
        return result

    elif type in ['.flac', '.wav', '.wave']:
        log.debug(f"Converting {type.upper()} to M4B (requires transcoding to AAC)")
        try:
            # Read metadata from source file before conversion
            sourceFile = mutagen.File(file, easy=True)

            # FLAC/WAV can't be stream-copied to M4B - must transcode to AAC
            transcodeCmd = ['ffmpeg',
                '-i', str(file),
                '-c:a', 'aac',           # Transcode to AAC
                '-b:a', '128k',          # 128kbps bitrate (good for audiobooks)
                '-ar', '44100',          # 44.1kHz sample rate
                '-ac', '2',              # Stereo
                '-vn',                   # Disable video
                '-loglevel', 'warning',
                '-stats',
                str(tempPath)]

            subprocess.run(transcodeCmd, check=True)

            # Copy metadata from source to converted m4b
            log.debug(f"Copying metadata from source {type} to converted m4b")
            try:
                convertedFile = mutagen.File(tempPath, easy=True)
                if convertedFile and sourceFile:
                    for tag in ['artist', 'albumartist', 'album', 'title', 'date', 'genre']:
                        if tag in sourceFile:
                            convertedFile[tag] = sourceFile[tag]
                    convertedFile.save()
                    log.debug("Metadata copied to m4b successfully")

                    # Check for cover image and embed it
                    coverPath = None
                    if sourceFolderPath:
                        coverPath = findCoverImage(sourceFolderPath)
                    if not coverPath:
                        coverPath = findCoverImage(Path(file).parent)

                    if coverPath:
                        try:
                            log.info(f"Embedding cover image: {coverPath}")
                            with open(coverPath, 'rb') as f:
                                coverData = f.read()

                            from mutagen.mp4 import MP4, MP4Cover
                            mp4File = MP4(tempPath)
                            if coverPath.suffix.lower() == '.png':
                                mp4File['covr'] = [MP4Cover(coverData, imageformat=MP4Cover.FORMAT_PNG)]
                            else:
                                mp4File['covr'] = [MP4Cover(coverData, imageformat=MP4Cover.FORMAT_JPEG)]
                            mp4File.save()
                            log.info("Cover image embedded in converted m4b")
                        except Exception as e:
                            log.warning(f"Failed to embed cover image: {e}")
                    else:
                        log.debug(f"No cover image found for: {file.name}")

            except Exception as e:
                log.warning(f"Failed to copy metadata to m4b file: {e}")

            file.unlink()
            if not settings.move and originalTempFile.exists() and originalTempFile != file:
                originalTempFile.unlink()
                log.debug(f"Deleted original temp file: {originalTempFile.name}")
            return tempPath.rename(newPath)

        except subprocess.CalledProcessError as e:
            failBook(file, "FLAC/WAV conversion failed")
            return file


def fixAuthorSeparators(directory):
    """
    Scan all audio files in directory and fix comma-separated authors to semicolons.
    This is for fixing existing files that were tagged before we switched to semicolon separators.
    """
    from mutagen.easyid3 import EasyID3
    from mutagen.easymp4 import EasyMP4
    from mutagen.mp4 import MP4

    directory = Path(directory)
    fixed_count = 0
    scanned_count = 0

    # Find all audio files
    audio_extensions = ['*.m4b', '*.m4a', '*.mp3', '*.mp4']
    audio_files = []
    for ext in audio_extensions:
        audio_files.extend(directory.rglob(ext))

    log.info(f"Scanning {len(audio_files)} audio files for comma-separated authors...")

    for audio_file in audio_files:
        scanned_count += 1
        try:
            modified = False
            ext = audio_file.suffix.lower()

            if ext == '.mp3':
                # Handle MP3 files with EasyID3
                try:
                    track = EasyID3(audio_file)
                except Exception:
                    continue

                for tag in ['artist', 'albumartist', 'composer']:
                    if tag in track:
                        value = track[tag]
                        if isinstance(value, list):
                            value = value[0] if value else ''
                        if ',' in value and ';' not in value:
                            # Contains commas but no semicolons - fix it
                            new_value = value.replace(', ', '; ').replace(',', '; ')
                            track[tag] = new_value
                            modified = True
                            log.debug(f"Fixed {tag}: '{value}' -> '{new_value}'")

                if modified:
                    track.save()
                    fixed_count += 1
                    log.info(f"Fixed author separators in: {audio_file.name}")

            elif ext in ['.m4b', '.m4a', '.mp4']:
                # Handle M4A/M4B files
                try:
                    track = MP4(audio_file)
                except Exception:
                    continue

                # MP4 tags for artist/author
                mp4_tags = ['\xa9ART', 'aART', '\xa9aut', '\xa9wrt']

                for tag in mp4_tags:
                    if tag in track:
                        value = track[tag]
                        if isinstance(value, list):
                            value = value[0] if value else ''
                        if isinstance(value, bytes):
                            value = value.decode('utf-8', errors='ignore')
                        if ',' in str(value) and ';' not in str(value):
                            # Contains commas but no semicolons - fix it
                            new_value = str(value).replace(', ', '; ').replace(',', '; ')
                            track[tag] = [new_value]
                            modified = True
                            log.debug(f"Fixed {tag}: '{value}' -> '{new_value}'")

                if modified:
                    track.save()
                    fixed_count += 1
                    log.info(f"Fixed author separators in: {audio_file.name}")

        except Exception as e:
            log.warning(f"Error processing {audio_file.name}: {e}")
            continue

    log.info(f"Scan complete. Fixed {fixed_count} of {scanned_count} files.")
    return fixed_count


def cleanMetadata(track, md):
    log.info("Cleaning file metadata")
    if isinstance(track, mp3.EasyMP3):
        log.debug("Cleaning easymp3 metadata")
        from mutagen.id3 import TXXX, ID3, APIC

        # Preserve existing cover art (APIC frame) before delete
        existing_apic = None
        try:
            id3_raw = ID3(track.filename)
            for key in id3_raw.keys():
                if key.startswith('APIC'):
                    existing_apic = id3_raw[key]
                    log.debug(f"Preserving existing cover art (APIC)")
                    break
        except Exception as e:
            log.debug(f"Could not read existing cover art: {e}")

        track.delete()
        track['title'] = md.title
        track['album'] = md.title  # Also write to album for getTitle() compatibility
        track['date'] = md.publishYear
        # Authors - write to artist, albumartist (semicolon-separated for Plex/Audiobookshelf)
        if hasattr(md, 'authors') and md.authors:
            authors_str = '; '.join(md.authors)
            track['artist'] = authors_str
            track['albumartist'] = authors_str
        else:
            # Convert comma-separated authors to semicolons
            authors_str = md.author.replace(', ', '; ') if md.author else md.author
            track['artist'] = authors_str
            track['albumartist'] = authors_str
        # Narrator - write to composer (semicolon-separated)
        if hasattr(md, 'narrators') and md.narrators:
            track['composer'] = '; '.join(md.narrators)
        elif md.narrator:
            track['composer'] = md.narrator
        # Genres (support multiple)
        try:
            if hasattr(md, 'genres') and md.genres:
                track['genre'] = md.genres
        except Exception:
            pass
        track['asin'] = md.asin

        # Save easy tags first, then add custom TXXX tags via raw ID3
        track.save()

        # Now open with raw ID3 to add custom TXXX frames
        id3_tags = ID3(track.filename)
        # Series index (volume number in series) - use custom TXXX tag
        if md.volumeNumber:
            id3_tags.add(TXXX(encoding=3, desc='series_index', text=md.volumeNumber))
        # Write custom TXXX tags for description, subtitle, isbn, publisher
        if md.summary:
            id3_tags.add(TXXX(encoding=3, desc='description', text=md.summary))
        if md.subtitle:
            id3_tags.add(TXXX(encoding=3, desc='subtitle', text=md.subtitle))
        if md.isbn:
            id3_tags.add(TXXX(encoding=3, desc='isbn', text=md.isbn))
        if md.publisher:
            id3_tags.add(TXXX(encoding=3, desc='publisher', text=md.publisher))

        # Restore or download cover art for EasyMP3
        cover_added = False

        # First, try to download cover from Audible if we have a URL
        if hasattr(md, 'coverUrl') and md.coverUrl:
            try:
                log.info(f"Downloading cover from Audible: {md.coverUrl}")
                cover_response = requests.get(md.coverUrl, timeout=10)
                if cover_response.ok:
                    cover_data = cover_response.content
                    # Detect image format
                    if md.coverUrl.lower().endswith('.png') or cover_data[:8] == b'\x89PNG\r\n\x1a\n':
                        mime_type = 'image/png'
                    else:
                        mime_type = 'image/jpeg'
                    id3_tags.add(APIC(encoding=3, mime=mime_type, type=3, desc='Cover', data=cover_data))
                    log.info("Cover art downloaded and embedded from Audible")
                    cover_added = True
            except Exception as e:
                log.debug(f"Failed to download cover from Audible: {e}")

        # If no Audible cover, restore existing cover
        if not cover_added and existing_apic:
            try:
                id3_tags.add(existing_apic)
                log.debug("Restored existing cover art")
                cover_added = True
            except Exception as e:
                log.debug(f"Failed to restore cover art: {e}")

        if not cover_added:
            log.debug("No cover art available to embed")

        id3_tags.save()
        log.debug("Metadata cleaned")
        return  # Already saved, don't call track.save() again

    elif isinstance(track, easymp4.EasyMP4):
        log.debug("Cleaning easymp4 metadata")
        track.RegisterTextKey('narrator', '@nrt')
        track.RegisterTextKey('author', '@aut')
        track.RegisterTextKey('composer', '\xa9wrt')  # Standard MP4 composer/writer tag
        # track.MP4Tags.RegisterFreeformKey('publisher', "----:com.thovin.publisher")
        track.MP4Tags.RegisterFreeformKey('publisher', "publisher", 'com.UltimateAudiobooks')
        # track.MP4Tags.RegisterFreeformKey('isbn', "----:com.thovin.isbn")
        track.MP4Tags.RegisterFreeformKey('isbn', "isbn", 'com.UltimateAudiobooks')
        # track.MP4Tags.RegisterFreeformKey('asin', "----:com.thovin.asin")
        track.MP4Tags.RegisterFreeformKey('asin', "asin", 'com.UltimateAudiobooks')
        # track.MP4Tags.RegisterFreeformKey('series', "----:com.thovin.series")
        track.MP4Tags.RegisterFreeformKey('series', "series", 'com.UltimateAudiobooks')
        # track.MP4Tags.RegisterFreeformKey('series_index', "----:com.thovin.series_index")
        track.MP4Tags.RegisterFreeformKey('series_index', "series_index", "com.UltimateAudiobooks")

        # Preserve existing cover art before delete
        existing_cover = None
        from mutagen.mp4 import MP4, MP4Cover
        try:
            mp4_raw = MP4(track.filename)
            if 'covr' in mp4_raw:
                existing_cover = mp4_raw['covr']
                log.debug(f"Preserving existing cover art ({len(existing_cover)} image(s))")
        except Exception as e:
            log.debug(f"Could not read existing cover art: {e}")

        track.delete()
        track['title'] = md.title
        track['album'] = md.title  # Also write to album for getTitle() compatibility
        # Narrators (semicolon-separated for Plex/Audiobookshelf)
        if hasattr(md, 'narrators') and md.narrators:
            track['narrator'] = '; '.join(md.narrators)
        else:
            track['narrator'] = md.narrator
        track['date'] = md.publishYear
        track['description'] = md.summary
        # Authors (semicolon-separated) - write to author, artist, and albumartist tags
        if hasattr(md, 'authors') and md.authors:
            authors_str = '; '.join(md.authors)
            track['author'] = authors_str
            track['artist'] = authors_str
            track['albumartist'] = authors_str
        else:
            # Convert comma-separated authors to semicolons
            authors_str = md.author.replace(', ', '; ') if md.author else md.author
            track['author'] = authors_str
            track['artist'] = authors_str
            track['albumartist'] = authors_str
        # Narrator - also write to composer (semicolon-separated)
        if hasattr(md, 'narrators') and md.narrators:
            track['composer'] = '; '.join(md.narrators)
        elif md.narrator:
            track['composer'] = md.narrator
        # Genres (support multiple)
        if hasattr(md, 'genres') and md.genres:
            track['genre'] = md.genres
        track['publisher'] = md.publisher
        track['isbn'] = md.isbn
        track['asin'] = md.asin
        track['series'] = md.series
        # Series index (volume number in series) - use custom freeform key
        # Note: discnumber is reserved for actual multi-disc audiobooks (used by FileMerger for chapter ordering)
        if md.volumeNumber:
            track['series_index'] = md.volumeNumber

        # Restore or download cover art
        # Save current metadata first so we can add cover via raw MP4
        track.save()

        # Now add cover art via raw MP4 interface
        mp4_raw = MP4(track.filename)
        cover_added = False

        # First, try to download cover from Audible if we have a URL
        if hasattr(md, 'coverUrl') and md.coverUrl:
            try:
                log.info(f"Downloading cover from Audible: {md.coverUrl}")
                cover_response = requests.get(md.coverUrl, timeout=10)
                if cover_response.ok:
                    cover_data = cover_response.content
                    # Detect image format
                    if md.coverUrl.lower().endswith('.png') or cover_data[:8] == b'\x89PNG\r\n\x1a\n':
                        mp4_raw['covr'] = [MP4Cover(cover_data, imageformat=MP4Cover.FORMAT_PNG)]
                    else:
                        mp4_raw['covr'] = [MP4Cover(cover_data, imageformat=MP4Cover.FORMAT_JPEG)]
                    mp4_raw.save()
                    log.info("Cover art downloaded and embedded from Audible")
                    cover_added = True
            except Exception as e:
                log.debug(f"Failed to download cover from Audible: {e}")

        # If no Audible cover, restore existing cover
        if not cover_added and existing_cover:
            try:
                mp4_raw['covr'] = existing_cover
                mp4_raw.save()
                log.debug("Restored existing cover art")
                cover_added = True
            except Exception as e:
                log.debug(f"Failed to restore cover art: {e}")

        if not cover_added:
            log.debug("No cover art available to embed")

        return  # Already saved

    elif isinstance(track, mp3.MP3):
        log.debug("Cleaning mp3 metadata")

        # Preserve existing cover art (APIC frame) before delete
        existing_apic = None
        try:
            for key in track.keys():
                if key.startswith('APIC'):
                    existing_apic = track[key]
                    log.debug(f"Preserving existing cover art (APIC)")
                    break
        except Exception as e:
            log.debug(f"Could not read existing cover art: {e}")

        track.delete()
        track.add(mutagen.TIT2(encoding = 3, text = md.title))
        # Narrators (ID3 TPE1) supports multiple
        tpe1_text = md.narrators if hasattr(md, 'narrators') and md.narrators else md.narrator
        track.add(mutagen.TPE1(encoding = 3, text = tpe1_text))
        track.add(mutagen.TALB(encoding = 3, text = md.series))
        track.add(mutagen.TYER(encoding = 3, text = md.publishYear))
        # Series index: TPOS is commonly repurposed for series position, but also add custom TXXX for clarity
        if md.volumeNumber:
            track.add(mutagen.TPOS(encoding = 3, text = md.volumeNumber))
            track.add(mutagen.TXXX(encoding = 3, desc='SERIES_INDEX', text = md.volumeNumber))
        # Authors (ID3 TCOM) - semicolon-separated for Plex/Audiobookshelf
        if hasattr(md, 'authors') and md.authors:
            track.add(mutagen.TCOM(encoding = 3, text = '; '.join(md.authors)))
        else:
            track.add(mutagen.TCOM(encoding = 3, text = md.author))
        # Genres (ID3 TCON) supports multiple values
        if hasattr(md, 'genres') and md.genres:
            track.add(mutagen.TCON(encoding = 3, text = md.genres))
        track.add(mutagen.TPUB(encoding = 3, text = md.publisher))
        track.add(mutagen.TXXX(encoding = 3, desc='description', text = md.summary))
        track.add(mutagen.TXXX(encoding = 3, desc='subtitle', text = md.subtitle))
        track.add(mutagen.TXXX(encoding = 3, desc='isbn', text = md.isbn))
        track.add(mutagen.TXXX(encoding = 3, desc='asin', text = md.asin))
        track.add(mutagen.TXXX(encoding = 3, desc='publisher', text = md.publisher))

        # Restore or download cover art for MP3
        cover_added = False

        # First, try to download cover from Audible if we have a URL
        if hasattr(md, 'coverUrl') and md.coverUrl:
            try:
                from mutagen.id3 import APIC
                log.info(f"Downloading cover from Audible: {md.coverUrl}")
                cover_response = requests.get(md.coverUrl, timeout=10)
                if cover_response.ok:
                    cover_data = cover_response.content
                    # Detect image format
                    if md.coverUrl.lower().endswith('.png') or cover_data[:8] == b'\x89PNG\r\n\x1a\n':
                        mime_type = 'image/png'
                    else:
                        mime_type = 'image/jpeg'
                    track.add(APIC(encoding=3, mime=mime_type, type=3, desc='Cover', data=cover_data))
                    log.info("Cover art downloaded and embedded from Audible")
                    cover_added = True
            except Exception as e:
                log.debug(f"Failed to download cover from Audible: {e}")

        # If no Audible cover, restore existing cover
        if not cover_added and existing_apic:
            try:
                track.add(existing_apic)
                log.debug("Restored existing cover art")
                cover_added = True
            except Exception as e:
                log.debug(f"Failed to restore cover art: {e}")

        if not cover_added:
            log.debug("No cover art available to embed")

    elif isinstance(track, mp4.MP4):
        log.debug("Cleaning mp4/m4b metadata")
        
        track['\xa9nam'] = md.title
        track['\xa9day'] = md.publishYear
        # Series index (volume number in series) - use custom freeform key
        # Note: trkn is for track numbers within an album, not series position
        if md.volumeNumber:
            track['----:com.thovin:series_index'] = mutagen.mp4.MP4FreeForm(str(md.volumeNumber).encode('utf-8'))
        # Authors (MP4) - semicolon-separated for Plex/Audiobookshelf
        if hasattr(md, 'authors') and md.authors:
            track['\xa9aut'] = '; '.join(md.authors)
        else:
            track['\xa9aut'] = md.author
        # Genres (MP4)
        if hasattr(md, 'genres') and md.genres:
            track['\xa9gen'] = md.genres
        track['\xa9des'] = md.summary
        # Narrators (MP4) - semicolon-separated for Plex/Audiobookshelf
        if hasattr(md, 'narrators') and md.narrators:
            track['\xa9nrt'] = '; '.join(md.narrators)
        else:
            track['\xa9nrt'] = md.narrator
        track['----:com.thovin:isbn'] = mutagen.mp4.MP4FreeForm(md.isbn.encode('utf-8'))
        track['----:com.thovin:asin'] = mutagen.mp4.MP4FreeForm(md.asin.encode('utf-8'))
        track['----:com.thovin:series'] = mutagen.mp4.MP4FreeForm(md.series.encode('utf-8'))

    else:
        log.error("Audio file not detected as MP3, MP4, or M4A/B. Unable to clean metadata.")
        return

    log.debug("Metadata cleaned")
    track.save()

#TODO Either the template or some part of writing into the opf results in some bad fields
def createOpf(md):
    log.info("Creating OPF")
    dcLink = "{http://purl.org/dc/elements/1.1/}"
    package = ET.Element("package", version="3.0", xmlns="http://www.idpf.org/2007/opf", unique_identifier="BookId")
    metadata = ET.SubElement(package, "metadata", nsmap={'dc' : dcLink})

    # Authors: write multiple creators when available; keep first as primary
    if hasattr(md, 'authors') and md.authors:
        for name in md.authors:
            a_el = ET.SubElement(metadata, f"{dcLink}creator", attrib={ET.QName(dcLink, "role"): "aut"})
            a_el.text = name
    else:
        author = ET.SubElement(metadata, f"{dcLink}creator", attrib={ET.QName(dcLink, "role"): "aut"})
        author.text = md.author

    title = ET.SubElement(metadata, f"{dcLink}title")
    title.text = md.title

    summary = ET.SubElement(metadata, f"{dcLink}description")
    summary.text = md.summary

    # Genres as dc:subject entries (multiple allowed)
    if hasattr(md, 'genres') and md.genres:
        for g in md.genres:
            subject = ET.SubElement(metadata, f"{dcLink}subject")
            subject.text = g

    # subtitle = ET.SubElement(metadata, f"{dcLink}subtitle")
    # subtitle.text = md.subtitle

    narrator = ET.SubElement(metadata, f"{dcLink}contributor", attrib={ET.QName(dcLink, "role"): "nrt"})
    narrator.text = md.narrator

    publisher = ET.SubElement(metadata, f"{dcLink}publisher")
    publisher.text = md.publisher

    publishYear = ET.SubElement(metadata, f"{dcLink}date")
    publishYear.text = md.publishYear

    isbn = ET.SubElement(metadata, f"{dcLink}identifier", attrib={ET.QName(dcLink, "scheme"): "ISBN"})
    isbn.text = md.isbn

    asin = ET.SubElement(metadata, f"{dcLink}identifier", attrib={ET.QName(dcLink, "scheme"): "ASIN"})
    asin.text = md.asin

    series = ET.SubElement(metadata, f"{dcLink}meta", attrib={"property" : "belongs-to-collection", "id" : "series-id"})
    series.text = md.series

    volumeNumber = ET.SubElement(metadata, f"{dcLink}meta", attrib={"refines" : "#series-id", "property" : "group-position"})
    volumeNumber.text = md.volumeNumber


    tree = ET.ElementTree(package)
    with open (md.bookPath + "/metadata.opf", "wb") as outFile:
        log.debug("Write OPF file")
        tree.write(outFile, xml_declaration=True, encoding="utf-8", method="xml")
            



def getUniquePath(fileName, outpath):
    counter = 1
    #TODO (rename) temp change while working on rename
    type = Path(fileName).suffix
    currPath = Path(outpath) / fileName
    while os.path.exists(currPath):
        currPath = Path(outpath) / Path(str(Path(fileName).stem) + " - " + str(counter) + type)
        counter += 1

    return currPath


def calculateWorkerCount():
    log.debug("Finding worker count")
    numCores = os.cpu_count()
    availableMemory = psutil.virtual_memory().available / (1024 ** 3)   #converts to Gb

    return numCores / 2 if numCores / 2 < availableMemory - 2 else availableMemory - 2

def sanitizeFile(file):
    file = Path(file)  # Ensure file is a Path object (may be string after ProcessPoolExecutor pickling)
    log.debug("Sanitize in - " + file.name)
    name = file.name
    parent = str(file.parent)

    #The users dirs are checked at init, so it should be safe to affect any with a special char at this point
    subs = {
        "&": "and"
    }

    for og, new in subs.items():
        name = name.replace(og, new)

    name = re.sub(r'[<>"|?:\*]', '', name)
    # name = re.sub(r'[^\x00-\x7F]+', '', name) #non-ASCII characters, in case they end up being trouble

    newParent = re.sub(r'[<>"|?\*]', '', parent) #since this is a dir path, no colons allowed
    Path(newParent).mkdir(parents = True, exist_ok = True)

    newPath = Path(newParent) / name

    if file == newPath:
        log.debug("Sanitize out - no changes")
        return file

    else:
        log.debug("Sanitize out - " + newPath.name)
        return file.rename(newPath)

