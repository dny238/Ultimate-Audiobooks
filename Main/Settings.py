import json
import logging
from pathlib import Path
import sys
import re
import os

log = logging.getLogger(__name__)

settings = None

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

class Settings:
    def __init__(self, args):
        '''
        if self.load:  #TODO either move below args parsing or manually extract load from args. This way doesn't work.
            try:
                self.loadSaveFile()
            except FileNotFoundError:
                log.debug("No saved settings found! Skipping load.")
        '''
                
        log.info("Parsing settings")
        for arg, value in vars(args).items():
            setattr(self, arg, value)

        if self.save:   
            self.createSaveFile()

        # Handle in-place mode: if -IP flag or output equals input
        if self.inPlace:
            self.output = self.input
            log.debug("In-place mode enabled - files will be modified in place")
        elif not self.output:
            outPath = str(Path(self.input).parent / "Ultimate Output")
            self.output = outPath
            log.debug("Output path defaulting to: " + outPath)

        # Auto-detect in-place mode if output equals input
        if Path(self.output).resolve() == Path(self.input).resolve():
            self.inPlace = True
            log.debug("In-place mode auto-detected (output = input)")

        if not self.quick:
            self.confirm()

        self.checkFolders()

        log.debug("Settings parsed")

    def loadSaveFile(self):
        log.debug("Loading settings")
        with open ('settings.json', 'r') as inFile:
            settingsMap = json.load(inFile)

        setSettings(Settings(**settingsMap))


    def createSaveFile(self): 
        log.debug("Saving settings")
        settingsMap = self.__dict__
        settingsJSON = json.dumps(settingsMap)

        with open ('settings.json', 'w') as outFile:
            outFile.write(settingsJSON)

    def confirm(self):
        log.debug("Confirming settings")
        for key, value in self.__dict__.items():
            print(f"{key}: {value}")

        while True:
            userInput = input("Continue program execution? (y/n): ").lower()

            if userInput == 'y':
                break
            elif userInput == 'n':
                print("Confirmed, exiting...")
                sys.exit()

    def checkFolders(self): #TODO this is really only a problem when using ffmpeg, I think. Cut this check and/or only check when going to use it and only check in those functions?
        # Apostrophes (both straight ' and curly ') are now handled via escaping in ffmpeg concat files
        # Input paths can have commas (common in multi-author folder names)
        # Output paths should not have commas (we control the output structure)
        inputSpecials = re.compile(r'[<>"|?*\x00-\x1F]')  # Allow commas in input paths
        outputSpecials = re.compile(r'[<,>"|?*\x00-\x1F]')  # Disallow commas in output paths
        inDirs = self.input.split(os.sep)
        outDirs = self.output.split(os.sep)

        for folder in inDirs:
            if inputSpecials.search(folder):
                log.error("ERROR: special character detected in input directory: " + str(folder) + \
                    ". Special characters can cause unexpected behavior and are not allowed. Aborting...")
                sys.exit(1)
        for folder in outDirs:
            if outputSpecials.search(folder):
                log.error("ERROR: special character detected in output directory: " + str(folder) + \
                    ". Special characters can cause unexpected behavior and are not allowed. Aborting...")
                sys.exit(1)
        
        
    
def setSettings(s):
    global settings
    settings = s

def getSettings():
    return settings