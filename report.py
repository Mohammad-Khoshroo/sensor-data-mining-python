import json
from datetime import datetime

def generate_error_log(sorted_errors, output_path):
    
    log_file = open(output_path, "w", encoding="utf-8")
    
    log_file.write("=== SENSOR ERROR & GAP REPORT TABLE ===\n\n")
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
    
    data_file = open(output_path, "w", encoding="utf-8")
    
    data_file.write("=== CLEAN EXTRACTED DATA REPORT ===\n")
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


def statistics_log(city_summary, sensors_summary, output_path):
    print("\n-----------------------------------------------------\n")
    print("                CITY WEATHER REPORT                  \n")
    
    print(f"City Averages:")
    print(f"  Temperature : {city_summary.get('avg_temp', 'N/A')} °C")
    print(f"  Humidity    : {city_summary.get('avg_hum', 'N/A')} %")
    print("-----------------------------------------------------\n")

    for sensor, data in sensors_summary.items():
        temp = data["temperature"]
        hum = data["humidity"]
        act = data["activity"]

        print(f"Sensor: {sensor}")
        print(f"  Active minutes     : {act['active_count']}")
        print(f"  Time range         : {act['start_time']} → {act['end_time']}")
        print(f"  Valid records      : Temp={temp['valid_count']} | Hum={hum['valid_count']}")
        print(f"  Temperature (°C)   : avg={temp['avg']} | min={temp['min']} | max={temp['max']} | std={temp['std']}")
        print(f"  Humidity (%)       : avg={hum['avg']} | min={hum['min']} | max={hum['max']} | std={hum['std']}")
        print("-" * 50)

    stats_file = open(output_path, "w", encoding="utf-8")
    stats_file.write("=== CITY WEATHER STATISTICS REPORT ===\n\n")
    stats_file.write(f"City Averages:\n")
    stats_file.write(f"  Temperature : {city_summary.get('avg_temp', 'N/A')} °C\n")
    stats_file.write(f"  Humidity    : {city_summary.get('avg_hum', 'N/A')} %\n")
    stats_file.write("\n" + "=" * 60 + "\n\n")

    for sensor, data in sensors_summary.items():
        temp = data["temperature"]
        hum = data["humidity"]
        act = data["activity"]

        stats_file.write(f"Sensor: {sensor}\n")
        stats_file.write(f"  Active time_quantum: {act['active_count']}\n")
        stats_file.write(f"  Time range         : {act['start_time']} → {act['end_time']}\n")
        stats_file.write(f"  Valid records      : Temp={temp['valid_count']} | Hum={hum['valid_count']}\n")
        stats_file.write(f"  Temperature (°C)   : avg={temp['avg']}, min={temp['min']}, max={temp['max']}, std={temp['std']}\n")
        stats_file.write(f"  Humidity (%)       : avg={hum['avg']}, min={hum['min']}, max={hum['max']}, std={hum['std']}\n")
        stats_file.write("-" * 50 + "\n")