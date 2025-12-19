import io_utils
import os
from datetime import datetime


def main():
    # 1. Project Directory Configuration
    RAW_DIR = "data/raw"
    PROCESSED_DIR = "data/processed"
    
    # Ensure the processed directory exists [cite: 32, 67]
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    # Generate reference timeline (1440 minutes for a full day)
    timeline = [f"{h:02d}:{m:02d}" for h in range(24) for m in range(60)]
    
    # Locate all raw files via io_utils [cite: 43, 48]
    files_info = io_utils.find_raw_files(RAW_DIR)
    sensor_names = [info["sensor"] for info in files_info]
    
    # In-memory storage for normalized data [cite: 97, 100]
    # Initialize all sensors for every minute with "NoF" (No Field)
    normalized_data = {t: {s: {"temp": "NoF", "hum": "NoF"} for s in sensor_names} for t in timeline}
    error_list = []
    
    # Validation Rules and Constants [cite: 147, 150]
    TEMP_RANGE, HUM_RANGE = (-30.0, 60.0), (0.0, 100.0)
    INVALID_TOKENS = ['NAN', 'INF', 'ERR', 'OUT', '?']

    # 2. Data Processing and Field Validation
    for info in files_info:
        sensor = info["sensor"]
        file_name = info["path"].name
        found_minutes = set()

        # Open file safely using 'with' [cite: 54, 60]
        with info["path"].open("r", encoding="utf-8") as f:
            next(f, None)  # Skip CSV Header [cite: 61]
            
            for line_no, line in enumerate(f, 2):
                clean_line = line.strip()  # Remove whitespace [cite: 70, 77]
                if not clean_line: 
                    continue
                
                parts = [p.strip() for p in clean_line.split(";")]
                
                # Check Row Structure [cite: 149]
                if len(parts) != 4:
                    error_list.append({
                        "time": "99:99", 
                        "sensor": sensor, 
                        "type": "Structure", 
                        "msg": "Column count mismatch", 
                        "raw": clean_line
                    })
                    continue
                
                date_v, time_v, t_raw, h_raw = parts
                
                # Validate Timeline Range [cite: 151]
                if time_v not in timeline:
                    error_list.append({
                        "time": time_v, 
                        "sensor": sensor, 
                        "type": "Timeline", 
                        "msg": "Invalid hour/min range", 
                        "raw": clean_line
                    })
                    continue

                found_minutes.add(time_v)
                
                # Process Temperature and Humidity fields independently
                field_results = {"temp": "NoF", "hum": "NoF"}
                field_configs = [
                    (t_raw, "temp", TEMP_RANGE), 
                    (h_raw, "hum", HUM_RANGE)
                ]

                for val_raw, field_name, v_range in field_configs:
                    error_type = None
                    final_val = "NoF"
                    
                    # Rule: Empty Field -> Missing Data
                    if not val_raw:
                        error_type = "Missing Data"
                    # Rule: Invalid Tokens -> Invalid Data
                    elif val_raw.upper() in INVALID_TOKENS:
                        error_type = "Invalid Data"
                    else:
                        try:
                            val_num = float(val_raw)
                            # Rule: Out of Logical Range -> Sensor Fault [cite: 150]
                            if v_range[0] <= val_num <= v_range[1]:
                                final_val = val_num
                            else:
                                error_type = "Sensor Fault"
                                final_val = "NoF"
                        except ValueError:
                            error_type = "Invalid Data" # Non-numeric text
                    
                    if error_type:
                        error_list.append({
                            "time": time_v, 
                            "sensor": sensor, 
                            "type": error_type,
                            "msg": f"{field_name.capitalize()}: {val_raw}", 
                            "raw": clean_line
                        })
                    field_results[field_name] = final_val

                normalized_data[time_v][sensor] = field_results

        # 3. Identify Data Gaps (Inactivity) [cite: 120, 184]
        missing_minutes = set(timeline) - found_minutes
        for m_time in missing_minutes:
            error_list.append({
                "time": m_time, 
                "sensor": sensor, 
                "type": "GAP", 
                "msg": "No data entry for minute", 
                "raw": "N/A"
            })

    # 4. Sort Errors Chronologically [cite: 124]
    sorted_errors = sorted(error_list, key=lambda x: x['time'])

    # 5. Generate Chronological Error Log
    error_log_path = os.path.join(PROCESSED_DIR, "errors.log")
    with open(error_log_path, "w", encoding="utf-8") as log_file:
        log_file.write("=== CHRONOLOGICAL SENSOR ERROR & GAP REPORT ===\n\n")
        header = f"{'Time':<8} | {'Sensor':<10} | {'Error Type':<15} | {'Reason/Message':<22} | {'Raw Content'}\n"
        log_file.write(header + "-"*len(header) + "\n")
        
        last_time = None
        for err in sorted_errors:
            # Group by time: Only print time for the first error in a minute block
            display_time = "" if err['time'] == last_time else err['time']
            log_file.write(
                f"{display_time:<8} | "
                f"{err['sensor']:<10} | "
                f"{err['type']:<15} | "
                f"{err['msg']:<22} | "
                f"{err['raw']}\n"
            )
            last_time = err['time']

    print(f"Error report generated at: {error_log_path}")
    
    # 6. Generate Structured Data Report (In-Memory Content) [cite: 126, 178]
    data_report_path = os.path.join(PROCESSED_DIR, "data_report.log")
    with open(data_report_path, "w", encoding="utf-8") as data_file:
        data_file.write("=== FINAL STRUCTURED DATA REPORT (Cleaned Content) ===\n")
        data_file.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        table_header = f"{'Time':<8} | {'Sensor':<10} | {'Temp':<8} | {'Humidity':<8}\n"
        data_file.write(table_header + "="*len(table_header) + "\n")
        
        for t in timeline:
            first_entry = True
            for s in sensor_names:
                time_col = t if first_entry else ""
                temp = normalized_data[t][s]["temp"]
                hum = normalized_data[t][s]["hum"]
                
                # Format float values or print NoF
                t_str = f"{temp:.2f}" if isinstance(temp, float) else str(temp)
                h_str = f"{hum:.2f}" if isinstance(hum, float) else str(hum)
                
                data_file.write(f"{time_col:<8} | {s:<10} | {t_str:<8} | {h_str:<8}\n")
                first_entry = False
            
            data_file.write("-" * 40 + "\n")

    print(f"Data report generated at: {data_report_path}")
    
if __name__ == "__main__":
    main()