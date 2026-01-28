import argparse
import Settings
import Util
import Processing
import FileMerger
import BookStatus
import PlexIntegration
import logging as log
from pathlib import Path
import sys

'''
DEBUG
INFO
WARNING
ERROR
CRITICAL
'''
settings = None

def main(args):
    #Yes, I know this approach isn't super elegant. Feel free to recommend an alternative that isn't more of a pain in the ass like a config file.
    global settings
    settings = Settings.Settings(args)
    Settings.setSettings(settings)
    Util.loadSettings()
    Processing.loadSettings()
    FileMerger.loadSettings()
    BookStatus.loadSettings()

    # Fix author separators mode - standalone operation
    if settings.fixSeparators:
        log.info("Running author separator fix on: " + settings.input)
        Util.fixAuthorSeparators(settings.input)
        return  # Don't continue with normal processing

    log.debug("Creating output directory if not exists: " + settings.output)
    Path(settings.output).mkdir(parents = True, exist_ok = True)
    processBooks()

    # Trigger Plex library refresh if enabled
    if settings.plexRefresh:
        PlexIntegration.refresh_library()


def processBooks():
    global settings

    if (settings.recurseFetch and settings.recurseCombine) or (settings.recurseFetch and settings.recursePreserve) or (settings.recurseCombine and settings.recursePreserve):
        log.critical("Incompatible processing modes selected. Enable only one processing mode. Exiting...")
        sys.exit()

    elif settings.recurseFetch:
        Processing.recursivelyFetchBatch()

    elif settings.recurseCombine:
        Processing.recursivelyCombineBatch()

    elif settings.recursePreserve:
        Processing.recursivelyPreserveBatch()

    else:
        Processing.singleLevelBatch()
    
    # Final summary (in case any were missed or for overall view)
    BookStatus.printSummary()


if __name__ == "__main__":
    # Parse arguments first to get log level
    parser = argparse.ArgumentParser(prog = "Ultimate Audiobooks")
    parser.add_argument("-B", "--batch", type=int, default = 10) #batch size
    parser.add_argument("-CL", "--clean", action = "store_true") #overwrite audio file metadata
    parser.add_argument("-CV", "--convert", action = "store_true") #convert to .m4b
    parser.add_argument("-CR", "--create", default = None, type=str.upper, choices = ["INFOTEXT", "OPF"]) #create metadata file where nonexistant. Where existant, skip unless --force is enabled
    parser.add_argument("-D", "--default", action = "store_true") #Reset saved settings to default
    parser.add_argument("-FO", "--force", action = "store_true") #When used with --create, this overwrites existing metadata files
    parser.add_argument("-FM", "--fetch", type=str.lower, choices = ["audible", "goodreads", "spotify", "all"]) #interactively fetch metadata from the web
    parser.add_argument("-FU", "--fetchUpdate", type=str.lower, choices = ["audible", "goodreads", "spotify", "all"]) #fetch metadata only for files missing author/title
    parser.add_argument("-I", "--input", required = True) #input folder
    parser.add_argument("-IP", "--inPlace", action = "store_true") #fix metadata in place without copying/moving files
    parser.add_argument("-L", "--load", action = "store_true")  #load saved settings
    parser.add_argument("-LF", "--logFile", type=str, default = None, help = "Write log output to file") #log file
    parser.add_argument("-LL", "--logLevel", type=str.upper, default = "INFO", choices = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help = "Set logging level") #log level
    parser.add_argument("-M", "--move", action = "store_true") #move files to output (copies by default)
    parser.add_argument("-O", "--output", default = None) #output folder. Will default to a named sub of input, set in setter method
    parser.add_argument("-Q", "--quick", action = "store_true") #skip confirmation of settings
    parser.add_argument("-RN", "--rename", default = None) #rename files
    parser.add_argument("-RF", "--recurseFetch", action = "store_true") #recursively fetch audio files, presumed to be entire books. Recursives are exclusive.
    parser.add_argument("-RC", "--recurseCombine", action = "store_true") #recursively fetch audio files, combining files sharing a dir. Recursives are exclusive.
    parser.add_argument("-RP", "--recursePreserve", action = "store_true") #recursively fetch audio files, preserving chapter files. Recursives are exclusive.
    parser.add_argument("-S", "--save", action = "store_true") #save settings for future executions
    parser.add_argument("-W", "--workers", type=int, default = 2)  #set number of workers for parallel processing (merges and conversions)
    parser.add_argument("-PX", "--plexRefresh", action = "store_true") #trigger Plex library refresh after processing
    parser.add_argument("-FS", "--fixSeparators", action = "store_true") #fix comma-separated authors to semicolons in existing files

    args = parser.parse_args()
    
    # Configure logging based on args
    numeric_level = getattr(log, args.logLevel, log.INFO)
    log_format = "[%(asctime)s][%(levelname)s] %(message)s"
    log_datefmt = '%H:%M:%S'

    if args.logFile:
        # Log to both console and file
        log.basicConfig(level=numeric_level, format=log_format, datefmt=log_datefmt,
                        handlers=[
                            log.StreamHandler(),
                            log.FileHandler(args.logFile, mode='w', encoding='utf-8')
                        ])
    else:
        log.basicConfig(level=numeric_level, format=log_format, datefmt=log_datefmt)
    
    log.debug("Arguments parsed successfully")

    final_message = ""
    exit_exc = None
    unexpected_error = False
    try:
        main(args)
        final_message = "Processing complete."
    except SystemExit as se:
        # Allow graceful pause on explicit exits
        final_message = "Exited."
        exit_exc = se
    except Exception as e:
        log.exception("Unhandled exception during execution")
        final_message = "An unexpected error occurred."
        unexpected_error = True
    finally:
        # Present final message as info only on non-error paths
        if final_message and not unexpected_error:
            log.info(final_message)
        # Preserve exit semantics
        if exit_exc is not None:
            raise exit_exc
        if unexpected_error:
            sys.exit(1)