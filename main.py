import io_utils
import proc
import report
import os

def main():
    # Configuration
    RAW_DIR, PROCESSED_DIR = "data/raw", "data/processed"
    io_utils.ensure_dir(PROCESSED_DIR)
    
    timeline = [f"{h:02d}:{m:02d}" for h in range(24) for m in range(60)]
    files_info = io_utils.find_raw_files(RAW_DIR)
    sensor_names = [info["sensor"] for info in files_info]
    
    # Storage
    normalized_data = {t: {s: {"temp": "NoF", "hum": "NoF"} for s in sensor_names} for t in timeline}
    all_errors = []
    
    # Constants
    TEMP_RANGE, HUM_RANGE = (-30.0, 60.0), (0.0, 100.0)
    INVALID_TOKENS = ['NAN', 'INF', 'ERR', 'OUT', '?']

    # Process files
    for info in files_info:
        found_minutes = set()
        with info["path"].open("r", encoding="utf-8") as f:
            next(f, None)
            for line_no, line in enumerate(f, 2):
                clean_line = line.strip()
                if not clean_line: continue
                
                parts = [p.strip() for p in clean_line.split(";")]
                if len(parts) != 4:
                    all_errors.append({"time": "99:99", "sensor": info["sensor"], "type": "Structure", "msg": "Col count mismatch", "raw": clean_line})
                    continue
                
                _, time_v, t_raw, h_raw = parts
                if time_v not in timeline:
                    all_errors.append({"time": time_v, "sensor": info["sensor"], "type": "Timeline", "msg": "Out of range", "raw": clean_line})
                    continue

                found_minutes.add(time_v)
                
                # Validate Fields
                t_val, t_err = proc.validate_field(t_raw, "temp", TEMP_RANGE, INVALID_TOKENS)
                h_val, h_err = proc.validate_field(h_raw, "hum", HUM_RANGE, INVALID_TOKENS)
                
                if t_err: all_errors.append({"time": time_v, "sensor": info["sensor"], "type": t_err, "msg": f"Temp: {t_raw}", "raw": clean_line})
                if h_err: all_errors.append({"time": time_v, "sensor": info["sensor"], "type": h_err, "msg": f"Hum: {h_raw}", "raw": clean_line})
                
                normalized_data[time_v][info["sensor"]] = {"temp": t_val, "hum": h_val}

        # Handle GAPs
        all_errors.extend(proc.identify_gaps(timeline, found_minutes, info["sensor"]))

    # Output Reports
    sorted_errors = sorted(all_errors, key=lambda x: x['time'])
    report.generate_error_log(sorted_errors, os.path.join(PROCESSED_DIR, "errors.log"))
    report.generate_data_log(normalized_data, timeline, sensor_names, os.path.join(PROCESSED_DIR, "data_report.log"))
    
    print("Processing Complete.")
    
    stats = proc.statistics(normalized_data, sensor_names)
    
    # ۲. ذخیره داده‌های تمیز در قالب JSON (به جای CSV) 
    json_path = os.path.join(PROCESSED_DIR, "clean_data.json")
    report.generate_data_json(normalized_data, json_path)
    
    # ۳. تولید گزارش‌های متنی
    sorted_errors = sorted(all_errors, key=lambda x: x['time'])
    report.generate_error_log(sorted_errors, os.path.join(PROCESSED_DIR, "errors.log"))
    report.generate_data_log(normalized_data, timeline, sensor_names, os.path.join(PROCESSED_DIR, "data_report.log"))
    report.statistics_report(stats, os.path.join(PROCESSED_DIR, "stats_report.log"))

if __name__ == "__main__":
    main()