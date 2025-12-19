import json
from datetime import datetime

def generate_error_log(sorted_errors, output_path):
    """Generates the chronological error log with grouped timestamps[cite: 120]."""
    with open(output_path, "w", encoding="utf-8") as log_file:
        log_file.write("=== CHRONOLOGICAL SENSOR ERROR & GAP REPORT ===\n\n")
        header = f"{'Time':<8} | {'Sensor':<10} | {'Error Type':<15} | {'Reason/Message':<22} | {'Raw Content'}\n"
        log_file.write(header + "-"*len(header) + "\n")
        
        last_time = None
        for err in sorted_errors:
            display_time = "" if err['time'] == last_time else err['time']
            log_file.write(
                f"{display_time:<8} | {err['sensor']:<10} | "
                f"{err['type']:<15} | {err['msg']:<22} | {err['raw']}\n"
            )
            last_time = err['time']

def generate_data_log(normalized_data, timeline, sensor_names, output_path):
    """Generates a structured report of all in-memory cleaned data[cite: 126]."""
    with open(output_path, "w", encoding="utf-8") as data_file:
        data_file.write("=== FINAL STRUCTURED DATA REPORT (Cleaned) ===\n")
        data_file.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        header = f"{'Time':<8} | {'Sensor':<10} | {'Temp':<8} | {'Humidity':<8}\n"
        data_file.write(header + "="*len(header) + "\n")
        
        for t in timeline:
            first_entry = True
            for s in sensor_names:
                time_col = t if first_entry else ""
                temp = normalized_data[t][s]["temp"]
                hum = normalized_data[t][s]["hum"]
                t_str = f"{temp:.2f}" if isinstance(temp, float) else str(temp)
                h_str = f"{hum:.2f}" if isinstance(hum, float) else str(hum)
                
                data_file.write(f"{time_col:<8} | {s:<10} | {t_str:<8} | {h_str:<8}\n")
                first_entry = False
            data_file.write("-" * 40 + "\n")
                

def generate_data_json(data_dict, output_path):
    f = open(output_path, "w", encoding="utf-8")
    json.dump(data_dict, f, ensure_ascii=False, indent=2)


def statistics_report(stats, output_path):
    """
    Generates a full report for console and file, preserving all city and sensor metrics.
    """
    print("\n" + "="*65)
    print("      CITY-WIDE INFRASTRUCTURE & SENSOR ACTIVITY REPORT      ")
    print("="*65)
    
    city = stats["city_metrics"]
    print(f"CITY AVERAGES | Temp: {city.get('avg_temp', 'N/A')}°C | Humidity: {city.get('avg_hum', 'N/A')}%")
    print("-" * 65)

    for s, data in stats["individual_sensors"].items():
        t, h, act = data["temperature"], data["humidity"], data["activity"]
        print(f"SENSOR: {s}")
        print(f"  [Uptime] Valid Records: {t['valid_count']} | Active: {act['total_active_mins']} mins | Range: {act['start_time']} to {act['end_time']}")
        print(f"  [Temp]   Avg: {t['avg']:<6} | StdDev: {t['std']:<6} | Min/Max: {t['min']}/{t['max']}")
        print(f"  [Hum]    Avg: {h['avg']:<6} | StdDev: {h['std']:<6} | Min/Max: {h['min']}/{h['max']}")
        print("-" * 50)
        
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("=== FINAL STATISTICAL ANALYSIS REPORT ===\n\n")
        f.write(f"OVERALL CITY TEMPERATURE AVERAGE: {city.get('avg_temp', 'N/A')}°C\n")
        f.write(f"OVERALL CITY HUMIDITY AVERAGE:    {city.get('avg_hum', 'N/A')}%\n")
        f.write("="*65 + "\n\n")

        for s, data in stats["individual_sensors"].items():
            t, h, act = data["temperature"], data["humidity"], data["activity"]
            f.write(f"SENSOR: {s}\n")
            f.write(f"  > Activity Info: {act['total_active_mins']} mins active ({act['start_time']} -> {act['end_time']})\n")
            f.write(f"  > Valid Records: Temp: {t['valid_count']}, Hum: {h['valid_count']}\n")
            f.write(f"  > Temperature:   Avg: {t['avg']}, StdDev: {t['std']}, Min: {t['min']}, Max: {t['max']}\n")
            f.write(f"  > Humidity:      Avg: {h['avg']}, StdDev: {h['std']}, Min: {h['min']}, Max: {h['max']}\n")
            f.write("-" * 55 + "\n")
            
    print(f"\n[SUCCESS] Full statistics saved at: {output_path}")