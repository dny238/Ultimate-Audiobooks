import logging
from pathlib import Path
from Settings import getSettings
import shutil
from datetime import datetime

log = logging.getLogger(__name__)
settings = None

# Marker file name for failed books
FAIL_MARKER_FILE = "ultimate-audiobook-fail.txt"

# Track all skipped and failed books
_skips = []
_fails = []

# Map current paths to original paths (for items moved to temp folders)
_originalPaths = {}

# Track files that were merged from multiple chapters (should always convert to m4b)
_mergedFromChapters = set()

# Temp folder name constant
TEMP_FOLDER_NAME = "Ultimate temp"

def _isInTempFolder(item):
    """Check if an item is in the Ultimate temp folder."""
    item = Path(item)
    return TEMP_FOLDER_NAME in item.parts

def _deleteTempFile(item):
    """Delete a file if it's in the temp folder."""
    item = Path(item)
    if _isInTempFolder(item) and item.exists():
        try:
            if item.is_file():
                item.unlink()
                log.debug(f"Deleted temp file: {item.name}")
            elif item.is_dir():
                shutil.rmtree(item)
                log.debug(f"Deleted temp folder: {item.name}")
        except Exception as e:
            log.warning(f"Failed to delete temp item {item.name}: {e}")

def loadSettings():
    global settings
    settings = getSettings()

def setOriginalPath(currentPath, originalPath):
    """
    Register the original path for an item that may be moved (e.g., to temp folder).

    Args:
        currentPath: Current path of the item (may be in temp folder)
        originalPath: Original path of the item (before moving to temp)
    """
    # Resolve to absolute paths for consistent lookups
    current = Path(currentPath).resolve()
    original = Path(originalPath).resolve()
    _originalPaths[current] = original

def getOriginalPath(currentPath):
    """
    Get the original path for an item that was moved to temp folder.

    Args:
        currentPath: Current path of the item (may be in temp folder)

    Returns:
        Original path if found, otherwise None
    """
    current = Path(currentPath).resolve()
    return _originalPaths.get(current)


def setMergedFromChapters(filePath):
    """
    Mark a file as having been merged from multiple chapter files.
    These files should always be converted to m4b to preserve chapter info.

    Args:
        filePath: Path to the merged file
    """
    resolved = Path(filePath).resolve()
    _mergedFromChapters.add(resolved)
    log.debug(f"Marked as merged from chapters: {resolved.name}")


def isMergedFromChapters(filePath):
    """
    Check if a file was merged from multiple chapter files.

    Args:
        filePath: Path to check

    Returns:
        True if file was merged from chapters, False otherwise
    """
    resolved = Path(filePath).resolve()
    return resolved in _mergedFromChapters

def _getRelativePath(item):
    """
    Get the relative path from the master input directory to the item.
    Uses original path if the item was moved to a temp folder.
    Returns the item name if it's not under the input directory.
    """
    if settings is None:
        loadSettings()
    
    item = Path(item).resolve()
    
    # Check if we have an original path for this item (e.g., moved to temp)
    if item in _originalPaths:
        item = _originalPaths[item]
    
    try:
        # Get relative path from master input directory
        masterInput = Path(settings.input).resolve()
        relPath = item.relative_to(masterInput)
        relPathStr = str(relPath).replace('\\', '/')
        # If the relative path is "." (item is the input dir), use the folder name
        if relPathStr == ".":
            return item.name
        return relPathStr
    except ValueError:
        # Item is not under master input directory, just return the name
        return item.name

def _getSkipDir():
    """Get the skip directory path (does not create it)."""
    if settings is None:
        loadSettings()
    infolder = Path(settings.input)
    return infolder.parent.joinpath("Ultimate Audiobook skips")

def _getFailDir():
    """Get the fail directory path (does not create it)."""
    if settings is None:
        loadSettings()
    infolder = Path(settings.input)
    return infolder.parent.joinpath("Ultimate Audiobook fails")


def _createFailMarker(folderPath, reason=None, files=None):
    """
    Create a .fail marker file in the source folder with details about the failure.

    Args:
        folderPath: Path to the folder where the marker should be created
        reason: Reason for the failure
        files: Optional list of files found in the folder
    """
    folderPath = Path(folderPath)
    if not folderPath.is_dir():
        folderPath = folderPath.parent

    markerPath = folderPath / FAIL_MARKER_FILE

    try:
        with open(markerPath, 'w', encoding='utf-8') as f:
            f.write("ULTIMATE AUDIOBOOKS - MANUAL REVIEW REQUIRED\n")
            f.write("=" * 44 + "\n\n")

            if reason:
                f.write(f"Reason: {reason}\n\n")

            if files:
                f.write("Found files:\n")
                for file in files:
                    f.write(f"  - {Path(file).name}\n")
                f.write("\n")

            f.write("Suggested fix:\n")
            f.write("  - Review the files in this folder\n")
            f.write("  - Fix any issues (remove duplicates, rename files, etc.)\n")
            f.write("  - Delete this .fail file to retry processing\n\n")

            f.write(f"Detected: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

        log.debug(f"Created fail marker: {markerPath}")
    except Exception as e:
        log.warning(f"Failed to create fail marker file: {e}")


def hasFailMarker(folderPath):
    """
    Check if a folder has a .fail marker file.

    Args:
        folderPath: Path to check for fail marker

    Returns:
        True if fail marker exists, False otherwise
    """
    folderPath = Path(folderPath)
    if not folderPath.is_dir():
        folderPath = folderPath.parent

    markerPath = folderPath / FAIL_MARKER_FILE
    return markerPath.exists()


def getFailMarkerReason(folderPath):
    """
    Get the reason from a .fail marker file if it exists.

    Args:
        folderPath: Path to check for fail marker

    Returns:
        Reason string if found, None otherwise
    """
    folderPath = Path(folderPath)
    if not folderPath.is_dir():
        folderPath = folderPath.parent

    markerPath = folderPath / FAIL_MARKER_FILE
    if not markerPath.exists():
        return None

    try:
        with open(markerPath, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith("Reason:"):
                    return line[7:].strip()
    except Exception:
        pass

    return "Unknown (fail marker exists)"


def checkOutputExists(outputFolder, title, requireM4B=False):
    """
    Check if an output file already exists for this book.
    Looks for .m4b, .mp3, .m4a files matching the title.

    Args:
        outputFolder: Path to the output folder (e.g., output/Author/Title)
        title: The book title to check for
        requireM4B: If True, only .m4b files count as existing output (for -CV mode)

    Returns:
        Path to existing file if found, None otherwise
    """
    outputFolder = Path(outputFolder)
    if not outputFolder.exists():
        return None

    # Clean the title for filename matching
    from Util import cleanTitleForPath
    cleanTitle = cleanTitleForPath(title) if title else None

    # Check for common audiobook extensions
    # If requireM4B is set, only accept .m4b as valid output
    if requireM4B:
        extensions = ['.m4b']
    else:
        extensions = ['.m4b', '.mp3', '.m4a', '.flac', '.wav']

    for ext in extensions:
        # Check exact title match
        if cleanTitle:
            exactPath = outputFolder / f"{cleanTitle}{ext}"
            if exactPath.exists():
                return exactPath

        # Check for any audiobook file in the folder
        for audioFile in outputFolder.glob(f"*{ext}"):
            return audioFile

    return None

def _moveItem(item, destDir, itemType="book"):
    """
    Move a file or folder to destination directory.
    Creates the destination directory if it doesn't exist.
    
    Args:
        item: Path object (file or folder) to move
        destDir: Destination directory Path
        itemType: String for logging ("book", "file", "folder")
    """
    item = Path(item)
    if not item.exists():
        log.warning(f"{itemType.capitalize()} no longer exists, cannot move: {item.name}")
        return False
    
    # Create destination directory only when needed
    destDir.mkdir(exist_ok=True)
    dest = destDir / item.name
    
    # Handle name conflicts
    counter = 1
    original_dest = dest
    while dest.exists():
        if item.is_dir():
            dest = destDir / f"{item.name} - {counter}"
        else:
            stem = item.stem
            suffix = item.suffix
            dest = destDir / f"{stem} - {counter}{suffix}"
        counter += 1
    
    try:
        if item.is_dir():
            shutil.move(str(item), str(dest))
            log.info(f"Moved {itemType} folder: {item.name} -> {dest.name}")
        else:
            item.rename(dest)
            log.info(f"Moved {itemType} file: {item.name} -> {dest.name}")
        return True
    except Exception as e:
        log.error(f"Error moving {itemType} {item.name}: {e}")
        return False

def skipBook(item, reason=None):
    """
    Mark a book (file or folder) as skipped.
    Only moves items when settings.move is True (user chose to move files).
    When copying, items remain in place with just logging.
    Files in temp folder are always deleted on skip.

    Args:
        item: Path object (file or folder) to mark as skipped
        reason: Optional reason string for logging
    """
    item = Path(item)

    # Avoid duplicates
    if item in _skips:
        log.debug(f"Book already in skip list: {item.name}")
        return

    # Use original path for skip list tracking, not temp path
    originalItem = _originalPaths.get(item.resolve(), item)
    _skips.append(originalItem)

    # Format: "Skipping (reason): title" - short and clear
    relPath = _getRelativePath(originalItem)
    title = originalItem.stem if originalItem.is_file() else originalItem.name
    if reason:
        log.info(f"Skipping ({reason}): \"{title}\"")
    else:
        log.info(f"Skipping: \"{relPath}\"")

    # Always delete temp files - they're just intermediate artifacts
    _deleteTempFile(item)

    # Only move original items when user chose move mode
    if settings and settings.move and not _isInTempFolder(item):
        skipDir = _getSkipDir()
        _moveItem(item, skipDir, "book")

def failBook(item, reason=None, files=None):
    """
    Mark a book (file or folder) as failed.
    Creates a .fail marker file in the source folder.
    Only moves items when settings.move is True (user chose to move files).
    When copying, items remain in place with just logging.
    Files in temp folder are always deleted on fail.

    Args:
        item: Path object (file or folder) to mark as failed
        reason: Optional reason string for logging
        files: Optional list of files found (for inclusion in fail marker)
    """
    item = Path(item)

    # Get original path if item was moved to temp folder
    originalItem = _originalPaths.get(item.resolve(), item)

    # Avoid duplicates
    if originalItem in _fails:
        log.debug(f"Book already in fail list: {originalItem.name}")
        return

    _fails.append(originalItem)

    reason_msg = f" - {reason}" if reason else ""
    relPath = _getRelativePath(originalItem)
    log.error(f"Failed book: \"{relPath}\"{reason_msg}")

    # Create fail marker in the original source folder
    _createFailMarker(originalItem, reason, files)

    # Always delete temp files - they're just intermediate artifacts
    _deleteTempFile(item)

    # Only move original items when user chose move mode
    if settings and settings.move and not _isInTempFolder(item):
        failDir = _getFailDir()
        _moveItem(originalItem, failDir, "book")

def getSkips():
    """Get a copy of the skip list."""
    return _skips.copy()

def getFails():
    """Get a copy of the fail list."""
    return _fails.copy()

def getSkipCount():
    """Get the number of skipped books."""
    return len(_skips)

def getFailCount():
    """Get the number of failed books."""
    return len(_fails)

def clearSkips():
    """Clear the skip list (for testing/reset)."""
    _skips.clear()

def clearFails():
    """Clear the fail list (for testing/reset)."""
    _fails.clear()

def printSummary():
    """Print a summary of all skipped and failed books at the end."""
    if len(_skips) == 0 and len(_fails) == 0:
        log.info("No books were skipped or failed.")
        return
    
    log.info("=" * 60)
    log.info("SKIP/FAIL SUMMARY")
    log.info("=" * 60)
    
    if len(_skips) > 0:
        log.info(f"\nSkipped books ({len(_skips)}):")
        for item in _skips:
            relPath = _getRelativePath(item)
            log.info(f"  - {relPath}")
    
    if len(_fails) > 0:
        log.info(f"\nFailed books ({len(_fails)}):")
        for item in _fails:
            relPath = _getRelativePath(item)
            log.info(f"  - {relPath}")
    
    log.info("=" * 60)
