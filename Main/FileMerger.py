from Settings import getSettings
from itertools import islice
import mutagen
import re
import subprocess
import logging
import tempfile
from pathlib import Path
import os
import shutil
from Util import sanitizeFile, getAudioFiles, cleanAuthorForPath, cleanTitleForPath
from BookStatus import skipBook, failBook, setOriginalPath, hasFailMarker, getFailMarkerReason, checkOutputExists, setMergedFromChapters

log = logging.getLogger(__name__)
settings = None

def loadSettings():
    global settings
    settings = getSettings()

def findTitleNum(title, whichNum) -> int:
    title = title.upper()
    try:
        return int(re.findall(r'\d+', title)[whichNum])  #find all numbers, return specified
    except IndexError:
        if any(word in title for word in ["intro".upper(), "prologue".upper()]):
            log.debug("Intro or prologue detected. Setting as first element in trackmap.")
            return 0
        if any(word in title for word in ["outro".upper(), "epilogue".upper(), "credits".upper()]):
            log.debug("Outro, epilogue, or credits detected. Setting as last element in trackmap.")
            return 999
        else:
            log.debug("Failed to find keyword or number in title on numberPosition " + str(whichNum))
            return -1   #no more numbers in title


def findAlphanumericKey(title, whichNum):
    """
    Extract alphanumeric chapter keys like '01a', '01b', '02a' from titles.
    Returns a tuple (number, letter_suffix) for proper sorting, or None if not found.
    Examples: 'Chapter 01a' -> (1, 'a'), 'Track 12b' -> (12, 'b'), 'Chapter 05' -> (5, '')
    """
    title = title.upper()
    # Find patterns like '01a', '12b', '05' (number optionally followed by letter)
    matches = re.findall(r'(\d+)([A-Z])?', title)

    if whichNum < len(matches):
        num_str, letter = matches[whichNum]
        num = int(num_str)
        letter = letter if letter else ''
        return (num, letter)

    # Check for special keywords
    if any(word in title for word in ["INTRO", "PROLOGUE"]):
        log.debug("Intro or prologue detected. Setting as first element.")
        return (0, '')
    if any(word in title for word in ["OUTRO", "EPILOGUE", "CREDITS"]):
        log.debug("Outro, epilogue, or credits detected. Setting as last element.")
        return (999, 'ZZZ')

    return None


def orderByTrackNumber(tracks, hasMultipleDisks):
    log.debug(f"Attempting to order files by track number... hasMultipleDisks={hasMultipleDisks}, numTracks={len(tracks)}")
    chapters = [None] * (len(tracks) + 1)

    try:
        if hasMultipleDisks:
            log.debug("Processing multiple disks...")
            tracksDone = 0
            disk = 1
            maxDisk = 10  # Safety limit to prevent infinite loop
            while tracksDone < len(tracks) and disk <= maxDisk:
                offset = tracksDone
                log.debug(f"Processing disk {disk}, tracksDone={tracksDone}")
                for track in tracks:
                    log.debug(f"Checking track for disk {disk}...")
                    diskNumber = int(track['discnumber'][0])
                    trackNumber = int(track['tracknumber'][0].split('/')[0])

                    if diskNumber == disk:
                        chapters[trackNumber + offset] = track
                        tracksDone += 1
                disk += 1
            if disk > maxDisk:
                log.debug("Hit max disk limit, aborting")
                return []
        else:
            log.debug("Processing single disk...")
            for i, track in enumerate(tracks):
                log.debug(f"Getting track number for track {i+1}/{len(tracks)}: {Path(track.filename).name[:30]}...")
                if 'tracknumber' not in track:
                    log.debug(f"No tracknumber tag, aborting")
                    return []
                trackNumber = int(track['tracknumber'][0].split('/')[0])
                log.debug(f"Track {i+1} has tracknumber={trackNumber}")
                if chapters[trackNumber] == None:
                    chapters[trackNumber] = track
                else:
                    log.debug("Overlapping track numbers detected. Aborting track number sort.")
                    return []
            log.debug("All tracks processed successfully")

        if chapters[0] == None:
            chapters = chapters[1:]

        if chapters[-1] == None:
            chapters = chapters[:-1]

        log.debug(f"Ordered {len(chapters)} chapters by track number")
        return chapters
    except (KeyError, IndexError, ValueError) as e:
        log.debug(f"Track number ordering failed: {e}")
        return []


def orderByTitle(tracks, folderPath=None):
    log.debug("Attempting to order files by name...")

    # First try alphanumeric ordering (handles '01a', '01b', '02a' patterns)
    result = orderByTitleAlphanumeric(tracks, folderPath)
    if result:
        return result

    # Fall back to original numeric-only ordering
    log.debug("Alphanumeric ordering failed, trying numeric-only...")
    whichNum = 0
    maxNumericAttempts = 5  # Prevent infinite loops

    while whichNum < maxNumericAttempts:
        trackMap = {}
        for track in tracks:
            key = findTitleNum(Path(track.filename).stem, whichNum)
            if key in trackMap and key != -1:
                log.debug("Duplicate track numbers detected at position " + str(whichNum))
                trackMap = {999:"error"}
                break
            else:
                trackMap[key] = track
        ordered = sorted(trackMap.keys())

        if -1 in trackMap:
            log.debug("Failed to order files by name")
            break  # Try alphabetical fallback
        elif ordered[0] != 0 and ordered[0] != 1:
            whichNum += 1
            continue
        else:
            tracksOut = []
            for key in ordered:
                tracksOut.append(trackMap[key])
            return tracksOut

    # Final fallback: simple alphabetical sort by filename
    log.debug("Numeric ordering failed, trying alphabetical sort...")
    try:
        sortedTracks = sorted(tracks, key=lambda t: Path(t.filename).name.lower())
        log.debug(f"Alphabetical ordering succeeded: {[Path(t.filename).name for t in sortedTracks[:3]]}...")
        return sortedTracks
    except Exception as e:
        log.debug(f"Alphabetical ordering failed: {e}")
        return []


def detectDuplicateVersions(files, folderPath):
    """
    Detect if a folder contains multiple versions of the same audiobook.
    Returns the best set of files to use based on priority:
    1. Single m4b file (already has chapters)
    2. Single m4a file
    3. Set with more chapter files (more granular = better)
    4. Single mp3 file

    Also returns info about what was skipped for logging.
    """
    if len(files) <= 1:
        return files, None  # No duplicates possible

    # Group files by naming pattern
    # Common patterns: "01 - Title (1 of 4).mp3" vs "01 Title.mp3"
    patterns = {}

    for f in files:
        name = f.stem
        # Try to identify the pattern type by looking for common structures
        # Pattern 1: "XX - Title (N of M)" or "XX-Title-PartNN"
        # Pattern 2: "XX Title" or "Title XX"

        # Check for "(N of M)" pattern
        if re.search(r'\(\d+\s*of\s*\d+\)', name, re.IGNORECASE):
            pattern_key = "n_of_m"
        # Check for "-PartNN" pattern
        elif re.search(r'-Part\d+', name, re.IGNORECASE):
            pattern_key = "part_suffix"
        # Check for "Part N" or "Part NN" pattern (space before Part)
        elif re.search(r'\sPart\s*\d+', name, re.IGNORECASE):
            pattern_key = "part_word"
        # Check for " - " separator pattern (often indicates different source)
        elif ' - ' in name and re.search(r'\d+\s*-\s*', name):
            pattern_key = "dash_separator"
        else:
            # Default: group by whether it has leading numbers
            if re.match(r'^\d+\s', name):
                pattern_key = "numbered_prefix"
            else:
                pattern_key = "other"

        if pattern_key not in patterns:
            patterns[pattern_key] = []
        patterns[pattern_key].append(f)

    # If only one pattern, no duplicates
    if len(patterns) <= 1:
        return files, None

    # Multiple patterns detected - we have duplicate versions!
    log.warning(f"Multiple audiobook versions detected in: {folderPath.name}")
    for pattern, pattern_files in patterns.items():
        log.warning(f"  Pattern '{pattern}': {len(pattern_files)} files")

    # Priority selection:
    # 1. Check for single m4b file
    m4b_files = [f for f in files if f.suffix.lower() == '.m4b']
    if len(m4b_files) == 1:
        skipped = [f for f in files if f not in m4b_files]
        log.info(f"Selected single m4b file: {m4b_files[0].name}")
        log.info(f"Skipped {len(skipped)} files (alternate version in same folder)")
        return m4b_files, {"selected": "single_m4b", "skipped_count": len(skipped), "folder": folderPath.name}

    # 2. Check for single m4a file
    m4a_files = [f for f in files if f.suffix.lower() == '.m4a']
    if len(m4a_files) == 1:
        skipped = [f for f in files if f not in m4a_files]
        log.info(f"Selected single m4a file: {m4a_files[0].name}")
        log.info(f"Skipped {len(skipped)} files (alternate version in same folder)")
        return m4a_files, {"selected": "single_m4a", "skipped_count": len(skipped), "folder": folderPath.name}

    # 3. Select the pattern with the most files (more chapters = better)
    best_pattern = max(patterns.keys(), key=lambda k: len(patterns[k]))
    best_files = patterns[best_pattern]
    skipped_files = [f for f in files if f not in best_files]

    log.info(f"Selected version with most chapters: {len(best_files)} files (pattern: {best_pattern})")
    log.info(f"Skipped {len(skipped_files)} files (alternate version in same folder)")

    return best_files, {
        "selected": f"most_chapters_{best_pattern}",
        "selected_count": len(best_files),
        "skipped_count": len(skipped_files),
        "folder": folderPath.name,
        "all_patterns": {k: len(v) for k, v in patterns.items()}
    }


# Global list to track duplicate version decisions for end-of-run summary
duplicate_version_log = []

def getDuplicateVersionLog():
    """Return the log of duplicate version decisions for summary reporting."""
    return duplicate_version_log

def clearDuplicateVersionLog():
    """Clear the duplicate version log (call at start of processing)."""
    global duplicate_version_log
    duplicate_version_log = []


def orderByTitleAlphanumeric(tracks, folderPath=None):
    """
    Order tracks by alphanumeric chapter keys like '01a', '01b', '02a'.
    Returns ordered list of tracks, or empty list if ordering fails.
    """
    log.debug("Attempting alphanumeric ordering...")
    whichNum = 0
    maxAttempts = 5  # Prevent infinite loops

    while whichNum < maxAttempts:
        trackMap = {}
        allFound = True

        for track in tracks:
            key = findAlphanumericKey(Path(track.filename).stem, whichNum)
            if key is None:
                allFound = False
                break
            if key in trackMap:
                log.debug(f"Duplicate alphanumeric key {key} at position {whichNum}")
                trackMap = {}
                allFound = False
                break
            trackMap[key] = track

        if not allFound or not trackMap:
            whichNum += 1
            continue

        # Sort by tuple (number, letter) - this naturally sorts (1,'a') < (1,'b') < (2,'a')
        ordered = sorted(trackMap.keys())

        # Check if sequence starts reasonably (0 or 1)
        if ordered[0][0] != 0 and ordered[0][0] != 1:
            whichNum += 1
            continue

        log.debug(f"Alphanumeric ordering succeeded with keys: {ordered[:5]}{'...' if len(ordered) > 5 else ''}")
        return [trackMap[key] for key in ordered]

    log.debug("Alphanumeric ordering failed")
    return []
    

def mergeBook(folderPath, outPath = False, move = False, finalOutputPath = None, outputAsM4B = False):
    """
    Merge chapter files into a single audiobook file.

    Args:
        folderPath: Source folder containing chapter files
        outPath: Temp output path (deprecated - use finalOutputPath instead)
        move: If True, delete source files after merge
        finalOutputPath: If provided, merge directly to this path (skips temp)
        outputAsM4B: If True, output directly as M4B with chapters (transcodes MP3 to AAC)

    Returns:
        Path to the merged file, or None if merge failed/skipped
    """
    log.debug("Begin merging chapters in " + folderPath.name)

    # Check for fail marker - skip if previously failed
    if hasFailMarker(folderPath):
        reason = getFailMarkerReason(folderPath)
        skipBook(folderPath, f"Previously failed: {reason}")
        return None

    files = list(folderPath.glob("*.mp*"))
    hasMultipleDisks = False

    if len(files) < 1:
        files = list(folderPath.glob("*.m4*"))

    if len(files) < 1:
        files = list(folderPath.glob("*.flac"))

    if len(files) < 1:
        files = list(folderPath.glob("*.wav"))

    if len(files) < 1:
        log.debug(f"No audio files found in {folderPath.name}")
        return None

    # Detect and handle duplicate versions (e.g., 4-part vs 16-part versions)
    files, version_info = detectDuplicateVersions(files, folderPath)
    if version_info:
        duplicate_version_log.append(version_info)

    # Determine output filepath
    # Always output to M4B for chapter merges - MP3 can't store chapter markers
    inputExt = Path(files[0]).suffix.lower()
    outputExt = '.m4b'  # Always M4B to preserve chapters

    if finalOutputPath:
        # Direct output mode - merge directly to final location
        # Change extension to .m4b to preserve chapters
        newFilepath = Path(str(finalOutputPath).rsplit('.', 1)[0] + '.m4b')
    elif outPath:
        newFilepath = outPath / (folderPath.name + outputExt)
    else:
        newFilepath = folderPath / (folderPath.name + outputExt)

    log.debug(str(len(files)) + " chapters detected")

    #TODO (rename) when --rename is working, apply here
    #TODO process merges at end like conversions?
    #TODO improve processing for multiple disks not in metadata


    # Save metadata from first source file BEFORE copying/sanitizing
    # Store as dictionary to preserve data after file operations
    savedMetadata = {}
    sourceFile = files[0]
    log.debug(f"Attempting to capture metadata from: {sourceFile}")
    try:
        # Load metadata from the source file
        sourceMetadata = mutagen.File(sourceFile, easy=True)
        if sourceMetadata:
            log.info(f"Successfully captured metadata from source file: {sourceFile.name}")
            # Extract metadata into a plain dictionary for persistence
            # Note: 'title' is excluded - in chapter files it's the chapter title, not book title
            # The book title comes from 'album'
            for tag in ['artist', 'albumartist', 'album', 'date', 'genre']:
                if tag in sourceMetadata:
                    savedMetadata[tag] = sourceMetadata[tag]
                    log.info(f"  {tag.capitalize()}: {sourceMetadata[tag]}")

            # Normalize artist/albumartist if they differ only in case
            # Use artist value (usually has better capitalization)
            if 'artist' in savedMetadata and 'albumartist' in savedMetadata:
                artist_val = savedMetadata['artist'][0] if savedMetadata['artist'] else ''
                albumartist_val = savedMetadata['albumartist'][0] if savedMetadata['albumartist'] else ''
                if artist_val.lower() == albumartist_val.lower() and artist_val != albumartist_val:
                    log.info(f"  Normalizing albumartist capitalization: '{albumartist_val}' -> '{artist_val}'")
                    savedMetadata['albumartist'] = savedMetadata['artist']
        else:
            log.debug(f"Source file has no metadata tags: {sourceFile.name}")
    except Exception as e:
        log.error(f"EXCEPTION reading metadata from {sourceFile.name}: {type(e).__name__}: {e}")
        import traceback
        log.error(traceback.format_exc())

    # Fallback to folder names if no metadata found
    if not savedMetadata:
        # Use folder structure: Author Folder / Book Folder / chapters
        bookName = folderPath.name
        authorName = folderPath.parent.name if folderPath.parent else None

        if authorName and bookName:
            log.info(f"Source files have no metadata tags - using folder structure for initial metadata (will be updated if fetched from Audible)")
            savedMetadata['artist'] = [authorName]
            savedMetadata['albumartist'] = [authorName]
            savedMetadata['album'] = [bookName]
        else:
            failBook(folderPath, f"No metadata and cannot determine author/title from folder structure")
            return

    # Check if output already exists BEFORE copying any files
    # This prevents duplicate books from being processed when they'd create the same output
    if savedMetadata and settings:
        # Get author from albumartist or artist
        author = None
        if 'albumartist' in savedMetadata:
            author = savedMetadata['albumartist'][0]
        elif 'artist' in savedMetadata:
            author = savedMetadata['artist'][0]

        # Get title from album
        title = savedMetadata.get('album', [None])[0]

        if author and title:
            # Clean author and title for path
            cleanAuthor = cleanAuthorForPath(author)
            cleanTitle = cleanTitleForPath(title)
            expectedOutputPath = Path(settings.output) / cleanAuthor / cleanTitle

            # If -CV mode, only .m4b counts as existing output
            existingFile = checkOutputExists(expectedOutputPath, title, requireM4B=settings.convert)
            if existingFile:
                skipBook(folderPath, f"Output already exists: {existingFile.name}")
                return

    # Determine where to put temp copies during merge
    if finalOutputPath:
        # Direct output mode - put temp copies in output folder's parent
        tempCopyDir = Path(finalOutputPath).parent
    elif outPath:
        tempCopyDir = outPath
    else:
        tempCopyDir = folderPath

    for i in range(len(files)):
        # Handle file preparation based on operation mode
        if move:
            # Moving: sanitize files in place
            files[i] = sanitizeFile(files[i])
        elif finalOutputPath:
            # Direct output mode: copy chapter files to output folder temporarily
            path, name = os.path.split(files[i])
            tempCopy = shutil.copy(files[i], os.path.join(tempCopyDir, name))
            files[i] = sanitizeFile(tempCopy)
        elif outPath:
            # Copying to temp folder: copy first, then sanitize the copy
            path, name = os.path.split(files[i])
            tempCopy = shutil.copy(files[i], os.path.join(outPath, name))
            files[i] = sanitizeFile(tempCopy)
        else:
            # Copying in same folder: create COPY prefix versions
            path, name = os.path.split(files[i])
            copyFile = shutil.copy(files[i], os.path.join(path, f"COPY{name}"))
            files[i] = sanitizeFile(copyFile)

    pieces = orderFiles(files, folderPath)

    if len(pieces) == 0:
        # Clean up copied files when ordering fails
        if outPath or finalOutputPath:
            log.debug("Cleaning up temp files after ordering failure")
            for f in files:
                try:
                    Path(f).unlink()
                except Exception as e:
                    log.debug(f"Failed to clean up temp file: {e}")
        return

    # TODO When sanitizing chapter files, worth trying to keep the original name in chapter metadata?
    # Create temp files in the directory where the audio files actually are
    tempDir = tempCopyDir
    tempConcatFilePath, tempChapFilePath = createTempFiles(pieces, tempDir)

    # Detect if input files need transcoding to AAC for M4B output
    # MP3, FLAC, WAV all need transcoding - only M4A/M4B can be stream-copied
    needsTranscode = inputExt in ['.mp3', '.flac', '.wav', '.wave']

    # Build FFmpeg command
    if needsTranscode:
        log.info(f"Transcoding {inputExt.upper()} to AAC for M4B with chapters")

        cmd = ['ffmpeg',
            '-f', 'concat',
            '-safe', '0',
            '-i', tempConcatFilePath,
            '-i', tempChapFilePath, "-map_metadata", "1",
            '-c:a', 'aac',           # Transcode to AAC
            '-b:a', '128k',          # 128kbps bitrate (good for audiobooks)
            '-ar', '44100',          # 44.1kHz sample rate
            '-ac', '2',              # Stereo
            '-vn',   #disable video
            '-loglevel', 'warning',
            '-stats',    #adds back the progress bar loglevel hides
            str(newFilepath)
            ]
    else:
        # M4A/M4B input - can stream-copy to M4B
        cmd = ['ffmpeg',
            '-f', 'concat',
            '-safe', '0',
            '-i', tempConcatFilePath,
            '-i', tempChapFilePath, "-map_metadata", "1",
            '-codec', 'copy',    #copy audio streams instead of re-encoding
            '-vn',   #disable video
            '-loglevel', 'warning',
            '-stats',    #adds back the progress bar loglevel hides
            str(newFilepath)
            ]

        #TODO manually parse out ffmpeg warnings like "Error reading comment frame, skipped", "Incorrect BOM value", "Application provided invalid, non monotonically increasing dts to muxer in <stream 0: 182540921472 >= 182539260288>"

    log.debug("Begin combining")
    try:
        subprocess.run(cmd, check=True)

        # Copy metadata from source to merged file
        # ffmpeg concat doesn't preserve ID3 tags, so we need to add them manually
        log.debug("Copying metadata from source to merged file")
        log.debug(f"savedMetadata contents: {savedMetadata}")
        try:
            mergedFile = mutagen.File(newFilepath, easy=True)
            log.debug(f"mergedFile loaded: {mergedFile is not None}")
            if mergedFile is None:
                log.error(f"Failed to load merged file: {newFilepath}")
            elif len(savedMetadata) == 0:
                log.error(f"savedMetadata is empty - cannot copy metadata")
            else:
                # Copy saved metadata to merged file
                for tag, value in savedMetadata.items():
                    if tag != 'title':  # Don't copy title - let it use the folder name
                        mergedFile[tag] = value
                        log.debug(f"Copied {tag}: {value}")
                mergedFile.save()
                log.info("Metadata copied successfully to merged file")

                # Check for cover image and embed it
                coverPath = folderPath / "cover.jpg"
                if coverPath.exists():
                    try:
                        log.debug(f"Found cover image: {coverPath}")
                        with open(coverPath, 'rb') as f:
                            coverData = f.read()

                        # Check file type and use appropriate method
                        if str(newFilepath).lower().endswith(('.m4a', '.m4b', '.mp4')):
                            from mutagen.mp4 import MP4, MP4Cover
                            mp4File = MP4(newFilepath)
                            mp4File['covr'] = [MP4Cover(coverData, imageformat=MP4Cover.FORMAT_JPEG)]
                            mp4File.save()
                            log.info("Cover image embedded successfully")
                        elif str(newFilepath).lower().endswith('.mp3'):
                            from mutagen.mp3 import MP3
                            from mutagen.id3 import ID3, APIC
                            mp3File = MP3(newFilepath)
                            if mp3File.tags is None:
                                mp3File.add_tags()
                            mp3File.tags.add(APIC(
                                encoding=3,  # UTF-8
                                mime='image/jpeg',
                                type=3,  # Cover (front)
                                desc='Cover',
                                data=coverData
                            ))
                            mp3File.save()
                            log.info("Cover image embedded successfully")
                        else:
                            log.warning(f"Cannot embed cover image: unsupported file type {newFilepath.suffix}")
                    except Exception as e:
                        log.warning(f"Failed to embed cover image: {e}")
                else:
                    log.debug("No cover.jpg found in source folder")

        except Exception as e:
            log.warning(f"Failed to copy metadata to merged file: {e}")
            import traceback
            log.error(traceback.format_exc())

        # Register original path for merged file (only needed for temp folder mode)
        if outPath and not finalOutputPath:
            setOriginalPath(newFilepath, folderPath)
            # Mark file as merged from chapters - should always convert to m4b
            setMergedFromChapters(newFilepath)

            # Copy cover.jpg to temp folder if it exists in source
            coverPath = folderPath / "cover.jpg"
            if coverPath.exists():
                tempCoverPath = outPath / "cover.jpg"
                shutil.copy(coverPath, tempCoverPath)
                log.debug(f"Copied cover.jpg to temp folder")

        # For direct output mode, mark the merged file so it converts to m4b
        if finalOutputPath:
            setMergedFromChapters(newFilepath)

        # Clean up chapter files after successful merge
        # When moving, delete source files
        # When copying to outPath (temp folder), delete the temp copies
        # For finalOutputPath mode, delete the temp copies in output folder
        if move or outPath or finalOutputPath:
            if os.path.exists(tempConcatFilePath):
                with open(tempConcatFilePath, 'r') as t:
                    for line in t:
                        # Unescape apostrophes that were escaped for ffmpeg
                        filepath = line[5:].strip().strip('\'').replace("'\\''", "'")
                        try:
                            os.remove(filepath)
                            log.debug(f"Deleted chapter file: {Path(filepath).name}")
                        except Exception as e:
                            log.warning(f"Failed to delete chapter file {filepath}: {e}")
            else:
                log.warning(f"Temp concat file not found for cleanup: {tempConcatFilePath}")

        # Clean up temp concat/chapter files
        try:
            os.remove(tempConcatFilePath)
            os.remove(tempChapFilePath)
        except Exception as e:
            log.debug(f"Failed to remove temp files: {e}")

    except subprocess.CalledProcessError as e:
        failBook(folderPath, "ffmpeg error during chapter merge")
        # Clean up temp files even on failure
        try:
            # Clean up audio chapter files that were copied to temp
            if outPath or finalOutputPath:
                with open(tempConcatFilePath, 'r') as t:
                    for line in t:
                        try:
                            # Unescape apostrophes that were escaped for ffmpeg
                            filepath = line[5:].strip().strip('\'').replace("'\\''", "'")
                            os.remove(filepath)
                        except:
                            pass
            os.remove(tempConcatFilePath)
            os.remove(tempChapFilePath)
            # Also clean up partial output file if it exists
            if finalOutputPath and Path(finalOutputPath).exists():
                try:
                    os.remove(finalOutputPath)
                except:
                    pass
        except:
            pass
        return None

    return newFilepath

def orderFiles(files, folderPath=None):
    pieces = []
    tracks = []
    hasMultipleDisks = False

    for file in files:
        try:
            track = mutagen.File(file, easy=True)
        except mutagen.mp3.HeaderNotFoundError:
            failBook(folderPath, "Corrupt or unreadable audio file")
            return []
        except Exception as e:
            log.error(f"Error reading file {file}: {e}")
            failBook(folderPath, f"Error reading chapter file: {e}")
            return []

        if track is None:
            log.error(f"Mutagen returned None for file: {file}")
            failBook(folderPath, f"Cannot read audio file: {file.name}")
            return []

        tracks.append(track)

        try:
            if int(track['discnumber'][0]) != 1:
                hasMultipleDisks = True
        except (KeyError, ValueError):
            pass

    try:
        pieces = orderByTrackNumber(tracks, hasMultipleDisks)
    except Exception as e:
        log.debug("Failed to order files by track number")
        pass

    if len(pieces) == 0:
        pieces = orderByTitle(tracks, folderPath)

    if len(pieces) == 0:
        if folderPath:
            # Include file list in fail marker for user reference
            fileNames = [str(f) for f in files]
            failBook(folderPath, "Failed to order files", fileNames)
    else:
        log.debug("Pieces ordered")

    return pieces

def createTempFiles(pieces, folderPath):
    log.debug("Write files to tempConcatFileList")
    tempConcatFilepath = ""
    tempChapFilepath = ""
    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.txt', dir=folderPath) as tempConcatFile, \
    tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.txt', dir=folderPath) as tempChapFile:
        #TODO skip books when this errors instead of crashing whole script? Especially on the for p loop. //This should be solved by checking for empty pieces list. Keep an eye on it.
        runningTime = 0
        chapCount = 1
        tempChapFile.write(";FFMETADATA1\n")

        for p in pieces: #p = mutagen easyMP*
            # Safety check - skip None entries
            if p is None:
                log.error("Encountered None track in pieces list - this shouldn't happen")
                continue
            # Escape apostrophes in filename for ffmpeg concat format
            # Replace ' with '\'' (end quote, escaped apostrophe, start quote)
            escapedFilename = p.filename.replace("'", "'\\''")
            tempConcatFile.write(f"file '{escapedFilename}'\n")

            tempChapFile.write("[CHAPTER]\n")
            tempChapFile.write("TIMEBASE=1/1000\n")
            tempChapFile.write(f"START={runningTime}\n")
            runningTime += p.info.length * 1000
            tempChapFile.write(f"END={runningTime}\n")
            tempChapFile.write(f"title=Chapter {chapCount}\n\n")
            chapCount += 1

        tempConcatFilepath = tempConcatFile.name
        tempChapFilepath = tempChapFile.name



    return tempConcatFilepath, tempChapFilepath


def findBooks(startPath, batchLimit, root=None, books=None, offset=0, scanState=None):
    """
    Recursively scan directories and return a list of books to process.
    Does NOT copy or move any files - just identifies what needs processing.

    Args:
        startPath: Directory to scan
        batchLimit: Maximum number of books to return
        root: Root directory (used internally for recursion)
        books: Accumulated list of books (used internally for recursion)
        offset: Number of books to skip before collecting (for batch continuation)
        scanState: Dict for tracking scan progress (used internally)

    Returns a list of dicts, each containing:
    - type: "single" or "chapters"
    - source_path: Path to the source folder
    - source_file: For single files, the file path
    - files: For chapter books, list of files (not used for single)
    """
    if root is None:
        root = startPath
    if books is None:
        books = []
    if scanState is None:
        # Initialize scan state at root level
        scanState = {'folders_scanned': 0, 'last_log': 0}
        log.info(f"Scanning for audiobooks in {startPath}...")

    # We need to collect offset + batchLimit books to return the right slice
    # Stop if we've reached the total needed
    totalNeeded = offset + batchLimit
    if len(books) >= totalNeeded:
        return books

    subfolders = [path for path in startPath.glob('*') if path.is_dir()]

    # Smart multi-disc detection: Look for folders with similar names and incrementing numbers
    # This handles: "CD 1", "Disc 2", "Book [Disc 1]", "Book - Part 2", "1", "2", etc.
    def extractBaseAndNumber(folderName):
        """Extract base name and number from folder name. Returns (baseName, number) or (None, None)."""
        name = folderName.strip()

        # Pattern 1: Just a number like "1", "2", "3"
        if re.match(r'^\d+$', name):
            return ('', int(name))

        # Pattern 2: Ends with a number, possibly with separators/brackets
        # Matches: "CD 1", "Disc-2", "Book [Disc 3]", "Part 4", "Book Name - 5", etc.
        match = re.match(r'^(.+?)[\s_-]*[\[\(]?(?:cd|disc|disk|part|volume|vol)?[\s_-]*(\d+)[\]\)]?\s*$', name, re.IGNORECASE)
        if match:
            base = match.group(1).strip()
            # Clean trailing separators from base
            base = re.sub(r'[\s_,-]+$', '', base)
            return (base, int(match.group(2)))

        return (None, None)

    def normalizeBaseName(name):
        """Normalize base name for comparison (lowercase, remove punctuation)."""
        if not name:
            return ''
        return re.sub(r'[^\w\s]', '', name.lower()).strip()

    # Extract base name and number for each subfolder
    folderInfo = []  # [(folder, baseName, number), ...]
    unmatchedFolders = []

    for folder in subfolders:
        base, num = extractBaseAndNumber(folder.name)
        if num is not None:
            folderInfo.append((folder, base, num))
        else:
            unmatchedFolders.append(folder)

    # Group folders by similar base name
    baseGroups = {}  # normalizedBase -> [(folder, originalBase, number), ...]
    for folder, base, num in folderInfo:
        normBase = normalizeBaseName(base)
        if normBase not in baseGroups:
            baseGroups[normBase] = []
        baseGroups[normBase].append((folder, base, num))

    # Process groups with 2+ folders as multi-disc books
    processedFolders = set()
    for normBase, group in baseGroups.items():
        if len(group) >= 2:
            # Sort by number
            group.sort(key=lambda x: x[2])

            # Verify numbers are reasonable (incrementing, not huge gaps)
            numbers = [x[2] for x in group]
            if max(numbers) - min(numbers) < len(numbers) * 2:  # Allow some gaps but not crazy ones
                # Collect all audio files
                allFiles = []
                for folder, base, num in group:
                    files = getAudioFiles(folder)
                    if files != -1 and len(files) > 0:
                        for f in files:
                            allFiles.append((num, f))
                    processedFolders.add(folder)

                if allFiles:
                    # Sort by number, then filename
                    allFiles.sort(key=lambda x: (x[0], x[1].name))
                    fileList = [f for _, f in allFiles]

                    # Determine book name: use the original base name from first folder, or parent folder name
                    originalBase = group[0][1]
                    bookName = originalBase if originalBase else startPath.name
                    sourceFolder = group[0][0].parent

                    log.debug(f"Found multi-part book: {bookName} ({len(group)} parts, {len(fileList)} total files)")
                    books.append({
                        'type': 'chapters',
                        'source_path': sourceFolder,
                        'source_name': bookName,
                        'files': fileList,
                        'multi_cd': True
                    })

                    scanState['folders_scanned'] += len(group)

                    if len(books) >= totalNeeded:
                        if startPath == root:
                            log.info(f"Scan complete: found {len(books)} books in {scanState['folders_scanned']} folders/subfolders")
                            if offset > 0:
                                return books[offset:]
                        return books

    # Folders that weren't part of a multi-disc group go back to normal processing
    remainingFolders = [f for f in subfolders if f not in processedFolders]
    subfolders = remainingFolders

    # Legacy check for pure CD/Disc subfolders (now mostly handled above, but keep as fallback)
    cdFolderPattern = re.compile(r'^(cd|disc|disk)\s*[-_]?\s*(\d+)$', re.IGNORECASE)
    cdFolders = [f for f in subfolders if cdFolderPattern.match(f.name)]
    nonCdFolders = [f for f in subfolders if not cdFolderPattern.match(f.name)]

    # If we have CD folders and they're the majority, treat this as a multi-CD book
    if cdFolders and len(cdFolders) >= len(nonCdFolders):
        # Sort CD folders by disc number
        def getCdNumber(folder):
            match = cdFolderPattern.match(folder.name)
            return int(match.group(2)) if match else 0
        cdFolders.sort(key=getCdNumber)

        # Collect all audio files from all CD folders
        allCdFiles = []
        for cdFolder in cdFolders:
            cdFiles = getAudioFiles(cdFolder)
            if cdFiles != -1 and len(cdFiles) > 0:
                discNum = getCdNumber(cdFolder)
                for f in cdFiles:
                    allCdFiles.append((discNum, f))

        if allCdFiles:
            # Sort by disc number, then by filename
            allCdFiles.sort(key=lambda x: (x[0], x[1].name))
            files = [f for _, f in allCdFiles]

            log.debug(f"Found multi-CD book: {startPath.name} ({len(cdFolders)} discs, {len(files)} total files)")
            books.append({
                'type': 'chapters',
                'source_path': startPath,
                'files': files,
                'multi_cd': True
            })

            scanState['folders_scanned'] += len(cdFolders)

            for folder in nonCdFolders:
                if len(books) >= totalNeeded:
                    return books
                scanState['folders_scanned'] += 1
                if scanState['folders_scanned'] - scanState['last_log'] >= 100:
                    log.info(f"  Scanned {scanState['folders_scanned']} folders/subfolders, found {len(books)} books so far...")
                    scanState['last_log'] = scanState['folders_scanned']
                books = findBooks(folder, batchLimit, root, books, offset, scanState)

            if startPath == root:
                log.info(f"Scan complete: found {len(books)} books in {scanState['folders_scanned']} folders/subfolders")
                if offset > 0:
                    return books[offset:]
            return books

    # Normal processing for non-CD folders
    for folder in subfolders:
        if len(books) >= totalNeeded:
            return books

        # Update scan progress
        scanState['folders_scanned'] += 1
        if scanState['folders_scanned'] - scanState['last_log'] >= 100:
            log.info(f"  Scanned {scanState['folders_scanned']} folders/subfolders, found {len(books)} books so far...")
            scanState['last_log'] = scanState['folders_scanned']

        books = findBooks(folder, batchLimit, root, books, offset, scanState)

    # Get audio files in this folder
    files = getAudioFiles(startPath)

    # Check if we should process files in this folder
    # Skip if no files, but allow processing root folder if it has no subfolders with audio
    shouldSkipRoot = startPath == root and len(subfolders) > 0
    if files == -1 or shouldSkipRoot:
        pass
    elif len(files) == 1:
        # Single-file book - add to list
        books.append({
            'type': 'single',
            'source_path': startPath,
            'source_file': files[0]
        })
        log.debug(f"Found single-file book: {files[0].name}")
    elif len(files) > 1:
        # Multi-file chapter book - add to list
        books.append({
            'type': 'chapters',
            'source_path': startPath,
            'files': files
        })
        log.debug(f"Found chapter book: {startPath.name} ({len(files)} files)")

    # At the root level, log completion and apply offset slicing before returning
    if startPath == root:
        log.info(f"Scan complete: found {len(books)} books in {scanState['folders_scanned']} folders/subfolders")
        if offset > 0:
            return books[offset:]
    return books


# Keep old function for backwards compatibility, but mark as deprecated
def combineAndFindChapters(startPath, outPath, counter, root):
    """DEPRECATED: Use findBooks() instead. This function is kept for backwards compatibility."""
    log.warning("combineAndFindChapters is deprecated - use findBooks() and process directly instead")

    subfolders = [path for path in startPath.glob('*') if path.is_dir()]
    if outPath in subfolders:
        subfolders.remove(outPath)
    for folder in subfolders:
        if counter <= settings.batch:
            counter = combineAndFindChapters(folder, outPath, counter, root)
        else:
            return counter

    files = getAudioFiles(startPath)
    shouldSkipRoot = startPath == root and len(subfolders) > 0
    if files == -1 or shouldSkipRoot:
        pass
    elif len(files) == 1:
        counter += 1
        originalFile = files[0]
        newFile = outPath / f"{files[0].name}"
        if settings.move:
            files[0].rename(newFile)
        else:
            shutil.copy(files[0], newFile)
        setOriginalPath(newFile, originalFile)
    elif len(files) > 1:
        counter += 1
        mergeBook(startPath, outPath, settings.move)

    return counter

    '''
    If -M, nuke emptied folder. Ensure there are no unchecked subfolders first!
    Either way, combined files should be put into outpath

    '''