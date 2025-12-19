import os
from pathlib import Path

def debug_files():
    path = Path("data/raw")
    print(f"Checking directory: {path.absolute()}")
    if not path.exists():
        print("❌ Error: Directory 'data/raw' does not exist!")
        return

    all_files = list(path.glob("*"))
    print(f"Total files found in folder: {len(all_files)}")
    for f in all_files:
        print(f" - Found file: '{f.name}'")
        
    dirty_files = list(path.glob("*_DIRTY.CSV"))
    print(f"Files matching '*_DIRTY.CSV': {len(dirty_files)}")

debug_files()
