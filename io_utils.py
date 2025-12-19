import os
from pathlib import Path

def ensure_dir(directory_path):
    """Creates the directory if it does not exist[cite: 67]."""
    os.makedirs(directory_path, exist_ok=True)

def find_raw_files(directory_name):
    """
    Locates all text files ending in '_DIRTY.CSV' and extracts sensor names.
    Returns a list of dictionaries with file paths and sensor identifiers[cite: 48].
    """
    data_path = Path(directory_name)
    if not data_path.exists():
        return []
    
    files_info = []
    for file_path in data_path.glob("*_DIRTY.CSV"):
        sensor_name = file_path.stem.replace("_DIRTY", "")
        files_info.append({
            "path": file_path,
            "sensor": sensor_name
        })
    return files_info