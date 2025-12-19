import io_utils
from datetime import datetime

def is_valid_date(date_str):
    """Checks if the date follows the DD.MM.YYYY format [cite: 151]"""
    try:
        datetime.strptime(date_str.strip(), "%d.%m.%Y")
        return True
    except ValueError:
        return False

def main():
    RAW_DIR = "data/raw"
    timeline = [f"{h:02d}:{m:02d}" for h in range(24) for m in range(60)]
    
    files_info = io_utils.find_raw_files(RAW_DIR)
    sensor_names = [info["sensor"] for info in files_info]
    
    valid_data_store = {t: {s: {"temp": "NoF", "hum": "NoF"} for s in sensor_names} for t in timeline}
    
    error_logs = []
    INVALID_TOKENS = ['NaN', '-99', '999.0']
    TEMP_RANGE = (-30.0, 60.0)
    HUM_RANGE = (0.0, 100.0)

    for info in files_info:
        sensor = info["sensor"]
        file_name = info["path"].name
        
        with info["path"].open("r", encoding="utf-8") as f:
            next(f, None)
            
            for line_no, line in enumerate(f, 2):
                clean_line = line.strip()
                if not clean_line: continue
                
                parts = clean_line.split(";")
                
                if len(parts) != 4:
                    error_logs.append({"file": file_name, "line": line_no, "reason": "Col count mismatch", "data": clean_line})
                    continue
                
                date_v, time_v, t_raw, h_raw = [p.strip() for p in parts]

                if not is_valid_date(date_v):
                    error_logs.append({"file": file_name, "line": line_no, "reason": "Invalid date format", "data": clean_line})
                    continue
                
                if time_v not in valid_data_store:
                    error_logs.append({"file": file_name, "line": line_no, "reason": "Time out of range", "data": clean_line})
                    continue

                final_temp = "NoF"
                try:
                    if t_raw not in INVALID_TOKENS and t_raw != "":
                        t_num = float(t_raw)
                        if TEMP_RANGE[0] <= t_num <= TEMP_RANGE[1]:
                            final_temp = t_num
                        else:
                            error_logs.append({"file": file_name, "line": line_no, "reason": f"Temp range ({t_num})", "data": clean_line})
                except ValueError:
                    error_logs.append({"file": file_name, "line": line_no, "reason": "Non-numeric temp", "data": clean_line})

                final_hum = "NoF"
                try:
                    if h_raw not in INVALID_TOKENS and h_raw != "":
                        h_num = float(h_raw)
                        if HUM_RANGE[0] <= h_num <= HUM_RANGE[1]:
                            final_hum = h_num
                        else:
                            error_logs.append({"file": file_name, "line": line_no, "reason": f"Hum range ({h_num})", "data": clean_line})
                except ValueError:
                    error_logs.append({"file": file_name, "line": line_no, "reason": "Non-numeric hum", "data": clean_line})

                valid_data_store[time_v][sensor] = {"temp": final_temp, "hum": final_hum}

    with open("data/processed/errors.log", "w", encoding="utf-8") as log_file:
        header = f"{'File':<18} | {'Line':<6} | {'Reason':<22} | {'Original Content'}\n"
        separator = "-" * len(header) + "\n"
        log_file.write("=== EXERCISE 2 DETAILED ERROR REPORT ===\n\n")
        log_file.write(header + separator)
        for err in error_logs:
            log_file.write(f"{err['file']:<18} | {err['line']:<6} | {err['reason']:<22} | {err['data']}\n")

    print(f"Validation finished. Tabular report saved to data/processed/errors.log.")

if __name__ == "__main__":
    main()