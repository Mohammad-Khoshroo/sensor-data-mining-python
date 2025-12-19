from pathlib import Path

def find_raw_files(directory_name):
    
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