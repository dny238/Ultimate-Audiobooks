import logging
import sys
from Settings import getSettings
from pathlib import Path
from Util import *
from FileMerger import combineAndFindChapters, findBooks, mergeBook, getDuplicateVersionLog, clearDuplicateVersionLog
from BookStatus import skipBook, failBook, checkOutputExists, isMergedFromChapters, _isInTempFolder, _deleteTempFile
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait, as_completed
import math
import re
import shutil



log = logging.getLogger(__name__)
settings = None
conversions = []

# List of books/files deferred for interactive metadata fetch
# Each entry is a dict with 'type' ('single' or 'chapters'), 'file'/'book', 'track'
deferredBooks = []

# Simple progress tracking: current index and total count
progress_current = 0
progress_total = 0

def setProgress(current, total):
    """Set current progress for log messages."""
    global progress_current, progress_total
    progress_current = current
    progress_total = total

def getProgressPrefix():
    """Return progress prefix like '10.5% (5/47)' for log messages."""
    if progress_total > 0:
        pct = (progress_current / progress_total) * 100
        return f"{pct:.1f}% ({progress_current}/{progress_total}) "
    return ""

def isConversionQueued(bookPath):
    """
    Check if a conversion is already queued for the given output path.
    This prevents duplicate processing when one file is queued for conversion
    and another file with the same metadata arrives later.
    """
    for c in conversions:
        if c.md.bookPath == bookPath:
            return c.file.name
    return None

# Track duplicate version decisions made during processing
single_file_duplicate_log = []

def detectDuplicateSingleFiles(files):
    """
    Detect duplicate complete audiobook files (e.g., multiple m4b versions).
    Groups files by their likely output path (based on metadata) and selects
    the best version when duplicates are found.

    Priority:
    1. m4b (chaptered audiobook)
    2. m4a
    3. mp3

    Returns filtered list of files to process.
    """
    global single_file_duplicate_log

    if len(files) <= 1:
        return files

    # Group files by their metadata (author + title)
    groups = {}
    totalFiles = len(files)
    log.info(f"Checking {totalFiles} files for duplicates (reading metadata)...")
    for i, file in enumerate(files):
        if (i + 1) % 25 == 0 or (i + 1) == totalFiles:
            log.info(f"  Metadata read progress: {i + 1}/{totalFiles}")
        try:
            track = mutagen.File(file, easy=True)
            if track is None:
                # Can't read metadata, keep the file
                groups[str(file)] = [file]
                continue

            author = getAuthor(track) or "Unknown"
            title = getTitle(track) or "Unknown"
            key = f"{author}|{title}".lower()

            if key not in groups:
                groups[key] = []
            groups[key].append(file)
        except Exception as e:
            log.debug(f"Error reading metadata for duplicate detection: {e}")
            groups[str(file)] = [file]

    # Select best version from each group
    result = []
    for key, group_files in groups.items():
        if len(group_files) == 1:
            result.append(group_files[0])
        else:
            # Check if files look like numbered chapters (e.g., 01_, 02_, Track 1, etc.)
            import re
            numbered_files = []
            for f in group_files:
                # Look for leading numbers in filename
                if re.match(r'^(\d+[-_\s]|track\s*\d+|chapter\s*\d+|part\s*\d+)', f.stem, re.IGNORECASE):
                    numbered_files.append(f)

            # If most files look like numbered chapters, warn user instead of treating as duplicates
            if len(numbered_files) >= len(group_files) * 0.6:  # 60% or more have numbers
                # Find common parent folder (grandparent of the files since they're in chapter subfolders)
                common_parent = group_files[0].parent.parent
                log.warning(f"POSSIBLE CHAPTER FILES in separate folders:")
                log.warning(f"  Location: {common_parent}")
                log.warning(f"  These {len(group_files)} files look like chapters of the same book but are in different folders:")
                for f in sorted(group_files, key=lambda x: x.stem):
                    log.warning(f"    - {f.parent.name}/{f.name}")
                log.warning(f"  Consider moving them into a single folder so they can be merged.")
                # Still process all of them individually since we can't merge across folders
                result.extend(group_files)
                continue

            # Multiple files with same metadata - select best version
            selected = selectBestVersion(group_files, key)
            result.append(selected)

            # Log the decision
            skipped = [f for f in group_files if f != selected]
            if skipped:
                # Check if filenames look unrelated (possible metadata mismatch)
                selectedStem = selected.stem.lower()
                for skippedFile in skipped:
                    skippedStem = skippedFile.stem.lower()
                    # If filenames share no common words (3+ chars), likely a metadata error
                    selectedWords = set(w for w in selectedStem.replace('-', ' ').replace('_', ' ').split() if len(w) >= 3)
                    skippedWords = set(w for w in skippedStem.replace('-', ' ').replace('_', ' ').split() if len(w) >= 3)
                    if not selectedWords.intersection(skippedWords):
                        log.warning(f"POSSIBLE METADATA ERROR: '{skippedFile.name}' has metadata claiming it's '{key}'")
                        log.warning(f"  This file may have incorrect ID3 tags - please verify and fix manually")

                log.warning(f"Multiple files with same metadata: {key}")
                log.info(f"  Selected: {selected.name}")
                log.info(f"  Skipped (duplicate metadata): {[f.name for f in skipped]}")
                single_file_duplicate_log.append({
                    "selected": selected.name,
                    "selected_type": selected.suffix,
                    "skipped_count": len(skipped),
                    "skipped_files": [f.name for f in skipped],
                    "key": key
                })

    return result

def selectBestVersion(files, key):
    """
    Select the best version from a list of files representing the same audiobook.
    Priority: m4b > m4a > flac > wav > mp3 > others
    """
    # Priority order for file types (lower = better)
    priority = {'.m4b': 1, '.m4a': 2, '.flac': 3, '.wav': 4, '.mp3': 5}

    # Sort by priority, then by file size (larger = better quality typically)
    def sort_key(f):
        ext_priority = priority.get(f.suffix.lower(), 99)
        try:
            size = f.stat().st_size
        except:
            size = 0
        return (ext_priority, -size)  # Negative size so larger files come first

    sorted_files = sorted(files, key=sort_key)
    return sorted_files[0]

def printDuplicateVersionSummary():
    """Print a summary of all duplicate version decisions made during processing."""
    from FileMerger import getDuplicateVersionLog

    chapter_log = getDuplicateVersionLog()

    if not chapter_log and not single_file_duplicate_log:
        return

    log.info("")
    log.info("=" * 60)
    log.info("DUPLICATE VERSION SUMMARY")
    log.info("=" * 60)

    if chapter_log:
        log.info("")
        log.info("Chapter folder duplicates (alternate versions in same folder):")
        for entry in chapter_log:
            folder = entry.get('folder', 'Unknown')
            selected = entry.get('selected', 'Unknown')
            skipped = entry.get('skipped_count', 0)
            log.info(f"  {folder}:")
            log.info(f"    Selected: {selected}")
            log.info(f"    Skipped: {skipped} files")
            if 'all_patterns' in entry:
                log.info(f"    Patterns found: {entry['all_patterns']}")

    if single_file_duplicate_log:
        log.info("")
        log.info("Single file duplicates (same author|title metadata):")
        for entry in single_file_duplicate_log:
            key = entry.get('key', 'Unknown')
            selected = entry.get('selected', 'Unknown')
            skipped = entry.get('skipped_files', [])
            log.info(f"  {key}:")
            log.info(f"    Selected: {selected}")
            log.info(f"    Skipped: {skipped}")

    log.info("")
    log.info("=" * 60)

def loadSettings():
    global settings
    settings = getSettings()

def processConversion(c, settings): #This is run through ProcessPoolExecutor, which limits access to globals
    file = c.file
    type = c.type
    track = c.track
    md = c.md
    sourceFolderPath = c.sourceFolderPath

    file = convertToM4B(file, type, md, settings, sourceFolderPath)
    track = mutagen.File(file, easy=True)

    if settings.fetch and settings.clean and settings.move:
        #if copying, we will only clean the copied file
        cleanMetadata(track, md)
    
    if settings.rename:
        #TODO rename
        #again, only apply to copy
        pass



def processConversions():
    log.info("Processing conversions")

    numWorkers = settings.workers
    if numWorkers == -1:
        numWorkers = math.floor(calculateWorkerCount())

        if numWorkers > 0:
            log.info(f"Number of workers not specified, set to {numWorkers} based on system CPU count and available memory")
        else:
            numWorkers = 1
            log.info("Number of workers not specified and unable to retrieve relevant system information. Defaulting to 1 worker.")

    with ProcessPoolExecutor(max_workers=numWorkers) as controller:
        futures = [controller.submit(processConversion, c, settings) for c in conversions]

        wait(futures)

        for future in futures:
            try:
                future.result()
            except Exception as e:
                log.error("Error processing conversion: " + str(e))

def processDeferredBooks():
    """
    Process books that were deferred during the auto-fetch phase.
    These books need user interaction to complete metadata fetch.
    """
    global deferredBooks

    if not deferredBooks:
        return

    log.info(f"\n{'='*60}")
    log.info(f"PHASE 2: Processing {len(deferredBooks)} deferred books requiring user interaction")
    log.info(f"{'='*60}\n")

    total = len(deferredBooks)
    for i, deferred in enumerate(deferredBooks, 1):
        setProgress(i, total)
        log.info(f"Deferred {i}/{total}: Processing...")

        if deferred['type'] == 'single':
            processDeferredSingleFile(deferred['file'], deferred['track'])
        elif deferred['type'] == 'chapters':
            processDeferredChapterBook(deferred['book'], deferred['track'])

    # Clear the deferred list after processing
    deferredBooks = []

    # Process any conversions that were queued during deferred processing
    if len(conversions) > 0:
        processConversions()

def processDeferredSingleFile(file, track):
    """Process a single file that was deferred for interactive metadata fetch."""
    file_type = Path(file).suffix.lower()

    # Re-open track in case it was invalidated
    try:
        track = mutagen.File(file, easy=True)
    except Exception as e:
        failBook(file, f"Cannot re-open file: {e}")
        return

    if track is None:
        failBook(file, "Cannot re-open file for metadata")
        return

    # Now call fetchMetadata WITHOUT autoOnly - this will prompt the user
    md = fetchMetadata(file, track, autoOnly=False)

    if md is None:
        # Book was skipped or failed during metadata fetch
        return

    # Continue with normal processing (same as processFile after fetchMetadata)
    if settings.inPlace:
        log.info(f"Writing metadata in-place to {file.name}")
        try:
            track = mutagen.File(file, easy=True)
            if track:
                cleanMetadata(track, md)
                log.info(f"Metadata updated in-place for {file.name}")
        except Exception as e:
            log.error(f"Error writing metadata to {file.name}: {e}")
        return

    # Build output path
    cleanAuthor = cleanAuthorForPath(md.author)
    cleanTitle = cleanTitleForPath(md.title)
    md.bookPath = settings.output + f"/{cleanAuthor}/{cleanTitle}"

    # Check if output already exists
    # If -CV mode, only .m4b counts as existing output
    existingFile = checkOutputExists(md.bookPath, md.title, requireM4B=settings.convert)
    if existingFile:
        skipBook(file, f"Output already exists: {existingFile.name}")
        return

    # Create output directory
    Path(md.bookPath).mkdir(parents=True, exist_ok=True)

    # Determine output path
    newPath = Path(md.bookPath) / Path(cleanTitle).with_suffix(file_type)

    # Convert to m4b if needed
    shouldConvert = (settings.convert or isMergedFromChapters(file)) and file_type != '.m4b'
    if shouldConvert:
        from BookStatus import getOriginalPath
        originalPath = getOriginalPath(file)
        sourceFolderPath = str(originalPath) if originalPath and originalPath.is_dir() else str(file.parent)
        conversions.append(Conversion(file, track, file_type, md, sourceFolderPath))
        return

    # Copy/move file
    if settings.move:
        log.info(f"Moving '{file.name}' to {newPath}")
        file.rename(newPath)
        copyCoverImage(file.parent, md.bookPath)
    else:
        log.info(f"Copying '{file.name}' to {newPath}")
        shutil.copy(file, newPath)
        if settings.fetch:
            cleanMetadata(mutagen.File(newPath, easy=True), md)
        copyCoverImage(file.parent, md.bookPath)

def processDeferredChapterBook(book, track):
    """Process a chapter book that was deferred for interactive metadata fetch."""
    sourcePath = book['source_path']
    files = book['files']
    firstFile = files[0]
    # Use source_name if provided (for sibling disc pattern), otherwise use folder name
    bookName = book.get('source_name', sourcePath.name)

    # Re-open track in case it was invalidated
    try:
        track = mutagen.File(firstFile, easy=True)
    except Exception as e:
        failBook(sourcePath, f"Cannot re-open file: {e}")
        return

    if track is None:
        failBook(sourcePath, "Cannot re-open file for metadata")
        return

    # Now call fetchMetadata WITHOUT autoOnly - this will prompt the user
    md = fetchMetadata(firstFile, track, autoOnly=False)

    if md is None:
        # Book was skipped or failed during metadata fetch
        return

    # Continue with normal processing (same as processChapterBook after fetchMetadata)
    if settings.inPlace and settings.recurseCombine:
        log.info(f"Merging chapter files in-place for: {bookName}")
        cleanTitle = cleanTitleForPath(md.title) if md.title else bookName
        # mergeBook always outputs M4B to preserve chapters
        finalOutputPath = sourcePath / (cleanTitle + '.m4b')

        if finalOutputPath.exists():
            log.info(f"Merged file already exists: {finalOutputPath.name}, skipping")
            return

        mergedFile = mergeBook(sourcePath, finalOutputPath=finalOutputPath, move=True)
        if mergedFile:
            try:
                mergedTrack = mutagen.File(mergedFile, easy=True)
                if mergedTrack:
                    cleanMetadata(mergedTrack, md)
                    log.info(f"Successfully merged to M4B with chapters: {Path(mergedFile).name}")
            except Exception as e:
                log.warning(f"Could not update metadata on merged file: {e}")
        return
    elif settings.inPlace:
        log.info(f"Writing metadata in-place to chapter files in {bookName}")
        for chapterFile in files:
            try:
                chapterTrack = mutagen.File(chapterFile, easy=True)
                if chapterTrack:
                    cleanMetadata(chapterTrack, md)
            except Exception as e:
                log.warning(f"Could not update metadata for {chapterFile.name}: {e}")
        return

    # Build output path
    cleanAuthor = cleanAuthorForPath(md.author)
    cleanTitle = cleanTitleForPath(md.title)
    bookPath = settings.output + f"/{cleanAuthor}/{cleanTitle}"

    # Check if output already exists
    # If -CV mode, only .m4b counts as existing output
    existingFile = checkOutputExists(bookPath, md.title, requireM4B=settings.convert)
    if existingFile:
        skipBook(sourcePath, f"Output already exists: {existingFile.name}")
        return

    # Create output directory
    Path(bookPath).mkdir(parents=True, exist_ok=True)

    # Merge directly to output - always M4B to preserve chapters
    finalOutputPath = Path(bookPath) / (cleanTitle + '.m4b')

    # Check if M4B already exists
    if finalOutputPath.exists():
        log.info(f"M4B already exists: {finalOutputPath.name}, skipping")
        return

    # Check for old intermediate MP3 from previous incomplete runs
    oldMp3Path = Path(bookPath) / (cleanTitle + Path(files[0]).suffix.lower())
    if oldMp3Path.exists() and oldMp3Path.suffix.lower() != '.m4b':
        log.info(f"Found old intermediate file {oldMp3Path.name}, deleting to re-merge with chapters")
        oldMp3Path.unlink()

    log.info(f"Merging to output: {finalOutputPath}")
    mergedFile = mergeBook(sourcePath, finalOutputPath=finalOutputPath)

    if mergedFile:
        # Copy cover image
        copyCoverImage(sourcePath, bookPath)

        try:
            mergedTrack = mutagen.File(mergedFile, easy=True)
            if mergedTrack:
                cleanMetadata(mergedTrack, md)
                log.info(f"Successfully merged chapter book to M4B with chapters: {finalOutputPath.name}")
        except Exception as e:
            log.warning(f"Could not update metadata on merged file: {e}")

def processFile(file):
    # Show parent folder for context (e.g., "Author/Book.mp3")
    parentName = file.parent.name if file.parent else ""
    prefix = getProgressPrefix()
    log.info(f"{prefix}Processing {parentName}/{file.name}" if parentName else f"{prefix}Processing {file.name}")
    file_type = Path(file).suffix.lower()
    md = Metadata()
    md.bookPath = settings.output
    newPath = ""

    try:
        track = mutagen.File(file, easy=True)
    except mutagen.mp3.HeaderNotFoundError:
        failBook(file, "Corrupt or unreadable audio file")
        return
    except mutagen.MutagenError as e:
        failBook(file, f"Cannot read file: {e}")
        return
    except PermissionError as e:
        failBook(file, f"Permission denied: {e}")
        return

    if track == None:
        failBook(file, "Unable to process file - mutagen returned None")
        return

    # Extract metadata from existing tags to create proper folder structure
    if not settings.fetch:
        # Get author and title from existing metadata
        author = getAuthor(track)
        title = getTitle(track)

        if author and title:
            md.author = author
            md.title = title
            # Clean author name for path (strips credits, replaces slashes, removes invalid chars)
            cleanAuthor = cleanAuthorForPath(author)
            cleanTitle = cleanTitleForPath(title)
            md.bookPath = settings.output + f"/{cleanAuthor}/{cleanTitle}"

            # Check if output already exists or is queued for conversion - skip if so
            # Skip this check in in-place mode since output = input
            # If -CV mode, only .m4b counts as existing output
            if not settings.inPlace:
                existingFile = checkOutputExists(md.bookPath, title, requireM4B=settings.convert)
                if existingFile:
                    skipBook(file, f"Output already exists: {existingFile.name}")
                    return
                queuedFile = isConversionQueued(md.bookPath)
                if queuedFile:
                    skipBook(file, f"Conversion already queued: {queuedFile}")
                    return

                log.debug(f"Making directory {md.bookPath} if not exists")
                Path(md.bookPath).mkdir(parents = True, exist_ok = True)

    # Handle fetch/fetchUpdate mode - only fetch if metadata is incomplete
    shouldFetch = False
    if settings.fetch or settings.fetchUpdate:
        assessment = assessMetadata(track)
        if assessment['complete']:
            log.info(f"Metadata complete for {file.name} - skipping fetch (author: {assessment['author']}, title: {assessment['title']})")
            shouldFetch = False
            # In-place mode with complete metadata - nothing to do
            if settings.inPlace:
                return

            # Non-in-place mode: set up bookPath with author/title from existing metadata
            md.author = assessment['author']
            md.title = assessment['title']
            cleanAuthor = cleanAuthorForPath(md.author)
            cleanTitle = cleanTitleForPath(md.title)
            md.bookPath = settings.output + f"/{cleanAuthor}/{cleanTitle}"

            # Check if output already exists or is queued for conversion - skip if so
            # If -CV mode, only .m4b counts as existing output
            existingFile = checkOutputExists(md.bookPath, md.title, requireM4B=settings.convert)
            if existingFile:
                skipBook(file, f"Output already exists: {existingFile.name}")
                return
            queuedFile = isConversionQueued(md.bookPath)
            if queuedFile:
                skipBook(file, f"Conversion already queued: {queuedFile}")
                return

            log.debug(f"Making directory {md.bookPath} if not exists")
            Path(md.bookPath).mkdir(parents = True, exist_ok = True)
        else:
            log.info(f"Metadata incomplete for {file.name} - missing: {assessment['missing']}")
            # Use fetchUpdate value if set, otherwise use fetch value
            if settings.fetchUpdate and not settings.fetch:
                settings.fetch = settings.fetchUpdate
            shouldFetch = True

    # Before prompting for metadata, check if output already exists
    # This prevents fetching metadata for books that already have output (e.g., from a previous run)
    if shouldFetch and not settings.inPlace:
        # First, check using the existing metadata's author/title (more reliable)
        # If -CV mode, only .m4b counts as existing output
        if assessment and assessment.get('author') and assessment.get('title'):
            metaAuthor = cleanAuthorForPath(assessment['author'])
            metaTitle = cleanTitleForPath(assessment['title'])
            potentialOutputPath = settings.output + f"/{metaAuthor}/{metaTitle}"
            existingFile = checkOutputExists(potentialOutputPath, assessment['title'], requireM4B=settings.convert)
            if existingFile:
                skipBook(file, f"Output already exists: {existingFile.name}")
                return

        # Fall back to folder structure check
        sourceTitleFolder = file.parent.name
        sourceAuthorFolder = file.parent.parent.name if file.parent.parent else None
        if sourceAuthorFolder:
            potentialOutputPath = settings.output + f"/{sourceAuthorFolder}/{sourceTitleFolder}"
            existingFile = checkOutputExists(potentialOutputPath, sourceTitleFolder, requireM4B=settings.convert)
            if existingFile:
                skipBook(file, f"Output already exists (from folder structure): {existingFile.name}")
                return

    if shouldFetch:
        #existing OPF is ignored in single level batch
        log.debug(f"  Before fetchMetadata: track type = {type(track).__name__ if track else 'None'}")
        md = fetchMetadata(file, track, autoOnly=True)
        log.debug(f"  After fetchMetadata: track type = {type(track).__name__ if track else 'None'}")

        # Restore original fetch setting if we changed it
        if settings.fetchUpdate and not getattr(settings, '_original_fetch', None):
            settings.fetch = None

        if md == METADATA_DEFERRED:
            # Auto-fetch failed, needs user interaction - defer for later
            log.info(f"Deferring {file.name} for interactive metadata fetch")
            deferredBooks.append({
                'type': 'single',
                'file': file,
                'track': track
            })
            return

        if md is None:
            # Book was skipped or failed during metadata fetch
            return

        # In-place mode: write metadata directly to source file and return
        if settings.inPlace:
            log.info(f"Writing metadata in-place to {file.name}")
            try:
                # Always re-open the file fresh for writing - the original track object
                # may have been invalidated during the potentially long fetchMetadata wait
                track = mutagen.File(file, easy=True)
                log.debug(f"  Re-opened track type: {type(track).__name__ if track else 'None'}")
                if track:
                    cleanMetadata(track, md)
                    log.info(f"Metadata updated in-place for {file.name}")
                else:
                    # Try to diagnose why mutagen can't open the file
                    log.warning(f"Could not open {file.name} for metadata writing - mutagen returned None")
                    # Try without easy mode to see if it's an easy mode issue
                    raw_track = mutagen.File(file, easy=False)
                    if raw_track:
                        log.warning(f"  File opens in raw mode as {type(raw_track).__name__} - may need special handling")
                    else:
                        log.warning(f"  File also fails in raw mode - file may be corrupted or unsupported format")
            except mutagen.MutagenError as e:
                log.error(f"Mutagen error writing to {file.name}: {e}")
            except PermissionError as e:
                log.error(f"Permission denied writing to {file.name}: {e}")
            except Exception as e:
                log.error(f"Unexpected error writing metadata to {file.name}: {type(e).__name__}: {e}")
            return

        #TODO (rename) set md.bookPath according to rename
        # Clean author name for path (strips credits, replaces slashes, removes invalid chars)
        cleanAuthor = cleanAuthorForPath(md.author)
        cleanTitle = cleanTitleForPath(md.title)
        md.bookPath = settings.output + f"/{cleanAuthor}/{cleanTitle}"

        # Check if output already exists or is queued for conversion - skip if so
        # If -CV mode, only .m4b counts as existing output
        existingFile = checkOutputExists(md.bookPath, md.title, requireM4B=settings.convert)
        if existingFile:
            skipBook(file, f"Output already exists: {existingFile.name}")
            return
        queuedFile = isConversionQueued(md.bookPath)
        if queuedFile:
            skipBook(file, f"Conversion already queued: {queuedFile}")
            return

        log.debug(f"Making directory {md.bookPath} if not exists")
        Path(md.bookPath).mkdir(parents = True, exist_ok = True)

        if settings.create:
            createOpf(md)

        # Convert to m4b if: -CV flag is set OR file was merged from chapters (to preserve chapter markers)
        shouldConvert = (settings.convert or isMergedFromChapters(file)) and file_type != '.m4b'
        if shouldConvert:
            if isMergedFromChapters(file) and not settings.convert:
                log.info(f"Auto-converting merged chapter book to m4b: {file.name}")
            else:
                log.debug(f"Queueing {file.name} for conversion")
            # Get original source folder path for cover image
            from BookStatus import getOriginalPath
            originalPath = getOriginalPath(file)
            # For merged chapter books, originalPath is the book folder itself
            # For single files, originalPath is the file, so we need .parent
            if originalPath:
                sourceFolderPath = str(originalPath) if originalPath.is_dir() else str(originalPath.parent)
            else:
                sourceFolderPath = str(file.parent)
            log.info(f"Queueing conversion with source folder: {sourceFolderPath}")
            conversions.append(Conversion(file, track, file_type, md, sourceFolderPath))
            return
        else:
            newPath = Path(md.bookPath) / Path(cleanTitle).with_suffix(file_type)

        if settings.clean and settings.move:
            #if copying, we will only clean the copied file
            cleanMetadata(track, md)

    # Convert to m4b if: -CV flag is set OR file was merged from chapters (to preserve chapter markers)
    shouldConvert = (settings.convert or isMergedFromChapters(file)) and file_type != '.m4b'
    if shouldConvert:
        if isMergedFromChapters(file) and not settings.convert:
            log.info(f"Auto-converting merged chapter book to m4b: {file.name}")
        # Get original source folder path for cover image
        from BookStatus import getOriginalPath
        originalPath = getOriginalPath(file)
        # For merged chapter books, originalPath is the book folder itself
        # For single files, originalPath is the file, so we need .parent
        if originalPath:
            sourceFolderPath = str(originalPath) if originalPath.is_dir() else str(originalPath.parent)
        else:
            sourceFolderPath = str(file.parent)
        log.info(f"Queueing conversion with source folder: {sourceFolderPath}")
        conversions.append(Conversion(file, track, file_type, md, sourceFolderPath))
        return

    if settings.rename:
        #TODO rename
        #again, only apply to copy
        pass

    if newPath == "":
        # Use title from metadata if available for filename, otherwise use original filename
        if md.title:
            cleanTitle = cleanTitleForPath(md.title)
            newPath = Path(md.bookPath) / (cleanTitle + file_type)
        else:
            newPath = getUniquePath(file.name, md.bookPath)

    # Get source folder for cover image lookup
    from BookStatus import getOriginalPath
    originalPath = getOriginalPath(file)
    # For merged chapter books, originalPath is the book folder itself
    # For single files, originalPath is the file, so we need .parent
    if originalPath:
        sourceFolderPath = originalPath if originalPath.is_dir() else originalPath.parent
    else:
        sourceFolderPath = file.parent

    if settings.move:
        log.info(f"Moving '{file.name}' to {newPath}")
        # TODO (rename) temporarily use title while working on rename
        file.rename(newPath)
        # Copy cover image to output folder
        copyCoverImage(sourceFolderPath, md.bookPath)
    else:
        log.info(f"Copying '{file.name}' to {newPath}")
        shutil.copy(file, newPath)

        if settings.fetch:
            cleanMetadata(mutagen.File(newPath, easy=True), md)

        # Copy cover image to output folder
        copyCoverImage(sourceFolderPath, md.bookPath)

        # Clean up temp file after copying to output
        _deleteTempFile(file)

def processChapterBook(book):
    """
    Process a multi-file chapter book by merging directly to output.
    No temp folder - merges directly to final destination.
    """
    sourcePath = book['source_path']
    files = book['files']
    # Use source_name if provided (for sibling disc pattern), otherwise use folder name
    bookName = book.get('source_name', sourcePath.name)

    # Show parent folder for context (e.g., "Author/Book")
    parentName = sourcePath.parent.name if sourcePath.parent else ""
    prefix = getProgressPrefix()
    log.info(f"{prefix}Processing chapter book from: {parentName}/{bookName}" if parentName else f"{prefix}Processing chapter book from: {bookName}")

    # Read metadata from first file to determine output path
    try:
        firstFile = files[0]
        track = mutagen.File(firstFile, easy=True)
        if track is None:
            failBook(sourcePath, "Unable to read metadata from chapter files")
            return
    except Exception as e:
        failBook(sourcePath, f"Error reading chapter file: {e}")
        return

    # Handle fetchUpdate mode for chapter books - only fetch if metadata is incomplete
    shouldFetch = settings.fetch
    assessment = None
    if settings.fetchUpdate and not settings.fetch:
        assessment = assessMetadata(track)
        if assessment['complete']:
            log.info(f"Metadata complete for {bookName} - skipping fetch (author: {assessment['author']}, title: {assessment['title']})")
            shouldFetch = False
        else:
            log.info(f"Metadata incomplete for {bookName} - missing: {assessment['missing']}")
            # Temporarily set fetch to the fetchUpdate value so fetchMetadata knows which source to use
            settings.fetch = settings.fetchUpdate
            shouldFetch = True

    # Before fetching, check if output already exists using existing metadata
    if shouldFetch and not settings.inPlace:
        # Get author/title from assessment if available, otherwise from track directly
        existingAuthor = assessment['author'] if assessment else getAuthor(track)
        existingTitle = assessment['title'] if assessment else getTitle(track)
        if existingAuthor and existingTitle:
            metaAuthor = cleanAuthorForPath(existingAuthor)
            metaTitle = cleanTitleForPath(existingTitle)
            potentialOutputPath = settings.output + f"/{metaAuthor}/{metaTitle}"
            # If -CV mode, only .m4b counts as existing output
            existingFile = checkOutputExists(potentialOutputPath, existingTitle, requireM4B=settings.convert)
            if existingFile:
                skipBook(sourcePath, f"Output already exists: {existingFile.name}")
                return

    if shouldFetch:
        md = fetchMetadata(firstFile, track, autoOnly=True)
        # Restore original fetch setting
        if settings.fetchUpdate:
            settings.fetch = None

        if md == METADATA_DEFERRED:
            # Auto-fetch failed, needs user interaction - defer for later
            log.info(f"Deferring {bookName} for interactive metadata fetch")
            deferredBooks.append({
                'type': 'chapters',
                'book': book,
                'track': track
            })
            return

        if md is None:
            # Book was skipped or failed during metadata fetch
            return

        # In-place mode for chapter books with recurseCombine: merge in place
        if settings.inPlace and settings.recurseCombine:
            log.info(f"Merging chapter files in-place for: {sourcePath.name}")
            # Merge to the same folder, delete chapters after
            # Always output M4B to preserve chapters
            cleanTitle = cleanTitleForPath(md.title) if md.title else sourcePath.name
            finalOutputPath = sourcePath / (cleanTitle + '.m4b')

            # Check if merged file already exists
            if finalOutputPath.exists():
                log.info(f"Merged file already exists: {finalOutputPath.name}, skipping")
                return

            mergedFile = mergeBook(sourcePath, finalOutputPath=finalOutputPath, move=True)
            if mergedFile:
                # Apply metadata to merged file
                try:
                    mergedTrack = mutagen.File(mergedFile, easy=True)
                    if mergedTrack:
                        cleanMetadata(mergedTrack, md)
                        log.info(f"Successfully merged to M4B with chapters: {Path(mergedFile).name}")
                except Exception as e:
                    log.warning(f"Could not update metadata on merged file: {e}")
            return
        elif settings.inPlace:
            # In-place mode without recurseCombine: just update metadata on chapter files
            log.info(f"Writing metadata in-place to chapter files in {sourcePath.name}")
            for chapterFile in files:
                try:
                    chapterTrack = mutagen.File(chapterFile, easy=True)
                    if chapterTrack:
                        cleanMetadata(chapterTrack, md)
                        log.debug(f"Updated metadata for: {chapterFile.name}")
                except Exception as e:
                    log.warning(f"Could not update metadata for {chapterFile.name}: {e}")
            log.info(f"Metadata updated in-place for {len(files)} chapter files")
            return

        author = md.author
        title = md.title
    else:
        # Get author and title for output path
        author = getAuthor(track)
        title = getTitle(track)

        # In-place mode with complete metadata and recurseCombine: merge in place
        if settings.inPlace and settings.recurseCombine:
            log.info(f"Merging chapter files in-place (no fetch needed): {sourcePath.name}")
            # Always output M4B to preserve chapters
            cleanTitle = cleanTitleForPath(title) if title else sourcePath.name
            finalOutputPath = sourcePath / (cleanTitle + '.m4b')

            # Check if merged file already exists
            if finalOutputPath.exists():
                log.info(f"Merged file already exists: {finalOutputPath.name}, skipping")
                return

            mergedFile = mergeBook(sourcePath, finalOutputPath=finalOutputPath, move=True)
            if mergedFile:
                log.info(f"Successfully merged to M4B with chapters: {Path(mergedFile).name}")
            return
        elif settings.inPlace:
            # In-place mode without recurseCombine: nothing to do for complete metadata
            return

    if not author or not title:
        # Try to use folder name as title
        title = sourcePath.name
        author = sourcePath.parent.name if sourcePath.parent != Path(settings.input) else "Unknown"

    # Clean for path
    cleanAuthor = cleanAuthorForPath(author)
    cleanTitle = cleanTitleForPath(title)
    bookPath = settings.output + f"/{cleanAuthor}/{cleanTitle}"

    # Check if output already exists
    # If -CV mode, only .m4b counts as existing output
    existingFile = checkOutputExists(bookPath, title, requireM4B=settings.convert)
    if existingFile:
        skipBook(sourcePath, f"Output already exists: {existingFile.name}")
        return

    # Check if already queued for conversion
    queuedFile = isConversionQueued(bookPath)
    if queuedFile:
        skipBook(sourcePath, f"Conversion already queued: {queuedFile}")
        return

    # Create output directory
    log.debug(f"Making directory {bookPath} if not exists")
    Path(bookPath).mkdir(parents=True, exist_ok=True)

    # Determine final output file path
    # mergeBook always outputs M4B to preserve chapters
    finalOutputPath = Path(bookPath) / (cleanTitle + '.m4b')

    # Check if M4B already exists (from previous run)
    if finalOutputPath.exists():
        log.info(f"M4B already exists: {finalOutputPath.name}, skipping")
        return

    # Check for old intermediate MP3 from previous incomplete runs
    oldMp3Path = Path(bookPath) / (cleanTitle + Path(files[0]).suffix.lower())
    if oldMp3Path.exists() and oldMp3Path.suffix.lower() != '.m4b':
        log.info(f"Found old intermediate file {oldMp3Path.name}, deleting to re-merge with chapters")
        oldMp3Path.unlink()

    log.info(f"Merging {len(files)} chapter files directly to: {finalOutputPath}")

    # Merge directly to output location (always outputs M4B with chapters)
    mergedFile = mergeBook(sourcePath, finalOutputPath=finalOutputPath)

    if mergedFile is None:
        # failBook already called in mergeBook
        return

    # Copy cover image to output folder
    copyCoverImage(sourcePath, bookPath)

    log.info(f"Successfully merged chapter book to M4B with chapters: {cleanTitle}")


def recursivelyCombineBatch(offset=0):
    """
    Recursively find and process chapter books.
    Single files are processed directly from source.
    Multi-file books are merged directly to output (no temp folder).

    Args:
        offset: Number of books to skip (for batch continuation)
    """
    log.info("Begin recursively finding and processing chapter books (no temp folder)")
    log.info("PHASE 1: Auto-fetch only (no user interaction)")
    infolder = Path(settings.input)

    # Clear duplicate version logs and deferred list at start of processing
    clearDuplicateVersionLog()
    global single_file_duplicate_log, deferredBooks
    single_file_duplicate_log = []
    deferredBooks = []

    # Scan for books
    books = findBooks(infolder, settings.batch, offset=offset)

    if len(books) == 0:
        log.info("No more books to process.")
        return

    # Separate single files and chapter books
    singleFiles = [b for b in books if b['type'] == 'single']
    chapterBooks = [b for b in books if b['type'] == 'chapters']
    log.info(f"Found {len(singleFiles)} single files, {len(chapterBooks)} chapter books")

    # Process single files (with duplicate detection)
    singleFilePaths = []
    if singleFiles:
        singleFilePaths = [b['source_file'] for b in singleFiles]
        if len(singleFilePaths) > 1:
            singleFilePaths = detectDuplicateSingleFiles(singleFilePaths)

    # Calculate total items for progress
    total = len(singleFilePaths) + len(chapterBooks)
    current = 0

    # Process single files (sequential - usually quick, just copying)
    for file in singleFilePaths:
        current += 1
        setProgress(current, total)
        processFile(file)

    # Process chapter books in parallel (merging is CPU-intensive)
    numWorkers = settings.workers if settings.workers > 0 else 2
    if chapterBooks:
        log.info(f"Processing {len(chapterBooks)} chapter books with {numWorkers} parallel workers")

        executor = ThreadPoolExecutor(max_workers=numWorkers)
        try:
            # Submit all chapter book jobs
            futures = {executor.submit(processChapterBook, book): book for book in chapterBooks}

            # Process results as they complete
            for future in as_completed(futures):
                current += 1
                setProgress(current, total)
                try:
                    future.result()
                except Exception as e:
                    book = futures[future]
                    log.error(f"Error processing chapter book {book.get('source_path', 'unknown')}: {e}")
        except KeyboardInterrupt:
            log.warning("\nCtrl+C detected - shutting down workers...")
            # Cancel pending futures
            for future in futures:
                future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            # Kill any running ffmpeg processes
            import subprocess
            try:
                if sys.platform == 'win32':
                    subprocess.run(['taskkill', '/F', '/IM', 'ffmpeg.exe'], capture_output=True)
                else:
                    subprocess.run(['pkill', '-9', 'ffmpeg'], capture_output=True)
            except:
                pass
            log.info("Shutdown complete.")
            raise
        finally:
            executor.shutdown(wait=True)

    # Process any queued conversions from Phase 1
    if len(conversions) > 0:
        processConversions()

    # Log Phase 1 completion
    deferredCount = len(deferredBooks)
    log.info(f"Phase 1 complete: {total} books scanned, {deferredCount} deferred for user interaction")

    # Phase 2: Process deferred books that need user interaction
    if deferredCount > 0:
        processDeferredBooks()

    # Print duplicate version summary at the end
    printDuplicateVersionSummary()

    log.info(f"Batch completed ({total} books processed).")

    # Prompt to continue with next batch
    next_offset = offset + len(books)
    if not settings.quick:
        response = input("Process another batch? (y/n): ").strip().lower()
        if response == 'y' or response == 'yes':
            recursivelyCombineBatch(next_offset)


def recursivelyPreserveBatch():
    log.info("Begin resurively finding and processing chapter books (chapters will be preserved)")
    return


def singleLevelBatch(infolder = None, skipDuplicateSummary = False, offset = 0):
    log.info("Begin single level batch processing")
    log.info("PHASE 1: Auto-fetch only (no user interaction)")
    global deferredBooks
    deferredBooks = []

    if infolder == None:
        infolder = Path(settings.input)
    files = getAudioFiles(infolder, settings.batch, recurse=False, offset=offset)

    if files == -1 or len(files) == 0:
        log.warning(f"No audio files found in '{infolder}'. Do you need to use -RC or -RF to search subdirectories?")
        return

    # Detect and filter duplicate versions before processing
    if isinstance(files, list) and len(files) > 1:
        files = detectDuplicateSingleFiles(files)

    total = len(files)
    for i, file in enumerate(files, 1):
        setProgress(i, total)
        processFile(file)

    if len(conversions) > 0:
        processConversions()

    # Log Phase 1 completion
    deferredCount = len(deferredBooks)
    log.info(f"Phase 1 complete: {total} files scanned, {deferredCount} deferred for user interaction")

    # Phase 2: Process deferred books that need user interaction
    if deferredCount > 0:
        processDeferredBooks()

    # Print duplicate version summary (unless called from recursivelyCombineBatch which does its own)
    if not skipDuplicateSummary:
        printDuplicateVersionSummary()

    log.info("Batch completed. Enjoy your audiobooks!")

    # Calculate next offset
    next_offset = offset + settings.batch

    # Prompt to continue with next batch
    if not settings.quick:
        response = input("Process another batch? (y/n): ").strip().lower()
        if response == 'y' or response == 'yes':
            singleLevelBatch(infolder, skipDuplicateSummary, next_offset)


def recursivelyFetchBatch(offset = 0):    #Since the only difference is passing true to getAudioFiles, I could probably fold this into another batch
    log.info("Begin processing complete books in all subdirectories (recursively fetch batch)")
    log.info("PHASE 1: Auto-fetch only (no user interaction)")
    global deferredBooks
    deferredBooks = []

    infolder = Path(settings.input)
    files = getAudioFiles(infolder, settings.batch, recurse=True, offset=offset)

    if files == -1 or len(files) == 0:
        log.info("No more files to process.")
        return

    # Detect and filter duplicate versions before processing
    if isinstance(files, list) and len(files) > 1:
        files = detectDuplicateSingleFiles(files)

    total = len(files)
    for i, file in enumerate(files, 1):
        setProgress(i, total)
        processFile(file)

    if len(conversions) > 0:
        processConversions()

    # Log Phase 1 completion
    deferredCount = len(deferredBooks)
    log.info(f"Phase 1 complete: {total} files scanned, {deferredCount} deferred for user interaction")

    # Phase 2: Process deferred books that need user interaction
    if deferredCount > 0:
        processDeferredBooks()

    # Print duplicate version summary
    printDuplicateVersionSummary()

    log.info("Batch completed. Enjoy your audiobooks!")

    # Calculate next offset
    next_offset = offset + settings.batch

    # Prompt to continue with next batch
    if not settings.quick:
        response = input("Process another batch? (y/n): ").strip().lower()
        if response == 'y' or response == 'yes':
            recursivelyFetchBatch(next_offset)

    return