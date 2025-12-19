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

def generate_data_report(normalized_data, timeline, sensor_names, output_path):
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