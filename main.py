import io_utils
import proc
import report
import os

def main():
    
    RAW_DIR = "data/raw"
    PROCESSED_DIR = "data/processed"
    all_errors = []
    
    timeline = [f"{h:02d}:{m:02d}:{s:02d}" for h in range(24) for m in range(60) for s in range(0,60,5)]
    TEMP_RANGE = (-30.0, 60.0)
    HUM_RANGE  = (0.0, 100.0)
    INVALID_TOKEN = ['NAN']
        
    files_info = io_utils.find_raw_files(RAW_DIR)
    sensor_names = [info["sensor"] for info in files_info]
    normalized_data = {t: {s: {"temp": "N/A", "hum": "N/A"} for s in sensor_names} for t in timeline}

    for info in files_info:
        
        found_time_interval = set()
        f = info["path"].open("r", encoding="utf-8")
        
        next(f, None)
        
        for line_no, line in enumerate(f, 2):    
            
            clean_line = line.strip()
            
            if not clean_line: 
                continue
            
            parts = [p.strip() for p in clean_line.split(";")]
            
            _ , time_v, t_raw, h_raw = parts
            if time_v not in timeline:
                all_errors.append(
                    {
                        "time": time_v,
                        "sensor": info["sensor"],
                        "type": "Timeline",
                        "msg": "Out of range",
                        "raw": clean_line
                    }
                )
                continue

            found_time_interval.add(time_v)
            
            t_val, t_err = proc.validate_field(t_raw, "temp", TEMP_RANGE, INVALID_TOKEN)
            h_val, h_err = proc.validate_field(h_raw, "hum", HUM_RANGE, INVALID_TOKEN)
            
            if t_err: 
                all_errors.append(
                    {
                        "time": time_v,
                        "sensor": info["sensor"],
                        "type": t_err,
                        "msg": f"Temp: {t_raw}",
                        "raw": clean_line
                    }
                )
            
            if h_err: 
                all_errors.append(
                    {
                        "time": time_v,
                        "sensor": info["sensor"],
                        "type": h_err,
                        "msg": f"Hum: {h_raw}",
                        "raw": clean_line
                    }
                )
            
            normalized_data[time_v][info["sensor"]] = {
                                                        "temp": t_val,
                                                        "hum": h_val
                                                      }

        all_errors.extend(proc.identify_gaps(timeline, found_time_interval, info["sensor"]))

    
    sorted_errors = sorted(all_errors, key=lambda x: x['time'])
    report.generate_error_log(sorted_errors, os.path.join(PROCESSED_DIR, "errors.log"))
    report.generate_data_log(normalized_data, timeline, sensor_names, os.path.join(PROCESSED_DIR, "clean_data.log"))
    
    print("Pre Processing Complete")
    
    print("Calculating statistics...")
    city_stats, sensors_stats = proc.statistics(normalized_data, sensor_names)
    report.statistics_log(city_stats, sensors_stats, os.path.join(PROCESSED_DIR, "stats_report.log"))
    
    
    print("Aggregating to minutely level...")
    minutely_data = proc.aggregate_data(normalized_data, sensor_names, level="minutely")
    
    print("Aggregating to hourly level...")
    hourly_data = proc.aggregate_data(minutely_data, sensor_names, level="hourly")

    print("Build JSON files...")
    report.generate_data_json(normalized_data, os.path.join(PROCESSED_DIR, "clean_data.json"))
    report.generate_data_json(minutely_data, os.path.join(PROCESSED_DIR, "data_minutely.json"))
    report.generate_data_json(hourly_data, os.path.join(PROCESSED_DIR, "data_hourly.json"))
    
    print("Multi-level processing complete")

if __name__ == "__main__":
    main()