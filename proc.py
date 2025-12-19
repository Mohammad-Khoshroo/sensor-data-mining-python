import math

def validate_field(val_raw, field_name, v_range, invalid_tokens):
    
    error_type = None
    final_val = "N/A"
    
    if not val_raw:
        error_type = "Missing Data"
    
    elif val_raw.upper() in invalid_tokens:
        error_type = "Invalid Data"
    
    else:
        try:
            val_num = float(val_raw)
            if v_range[0] <= val_num <= v_range[1]:
                final_val = val_num
            else:
                error_type = "Sensor Fault"
        except ValueError:
            error_type = "Invalid Data"
            
    return final_val, error_type

def identify_gaps(timeline, found_minutes, sensor):
    
    missing_minutes = set(timeline) - found_minutes
    gaps = []
    for m_time in missing_minutes:
        gaps.append(
            {
            "time": m_time,
            "sensor": sensor,
            "type": "UNRECIEVED",
            "msg": "Missing data entry",
            "raw": "N/A"
            }
        )
    return gaps

def get_average(values):
    valid_vals = [v for v in values if isinstance(v, (int, float))]
    return round(sum(valid_vals) / len(valid_vals), 2) if valid_vals else "N/A"

def get_std(data, avg):
    if len(data) < 2:
        return 0
    variance = sum((x - avg) ** 2 for x in data) / len(data)
    return math.sqrt(variance)

def statistics(normalized_data, sensor_names):

    sensors_summary = {}
    city_summary = {
        "temps": [],
        "hums": []
    }

    for s in sensor_names:

        temps = []
        hums = []
        active_times = []

        for t, row in normalized_data.items():
            temp = row[s]["temp"]
            hum = row[s]["hum"]

            valid = False

            if isinstance(temp, float):
                temps.append(temp)
                valid = True

            if isinstance(hum, float):
                hums.append(hum)
                valid = True

            if valid:
                active_times.append(t)

        city_summary["temps"].extend(temps)
        city_summary["hums"].extend(hums)

        t_avg = sum(temps) / len(temps) if temps else None
        h_avg = sum(hums) / len(hums) if hums else None

        sensors_summary[s] = {
            "temperature": {
                "values": temps,
                "avg": round(t_avg, 2) if t_avg is not None else None,
                "min": min(temps) if temps else None,
                "max": max(temps) if temps else None,
                "std": round(get_std(temps, t_avg), 2) if t_avg is not None else None,
                "valid_count": len(temps)
            },
            "humidity": {
                "values": hums,
                "avg": round(h_avg, 2) if h_avg is not None else None,
                "min": min(hums) if hums else None,
                "max": max(hums) if hums else None,
                "std": round(get_std(hums, h_avg), 2) if h_avg is not None else None,
                "valid_count": len(hums)
            },
            "activity": {
                "active_count": len(active_times),
                "start_time": min(active_times) if active_times else None,
                "end_time": max(active_times) if active_times else None
            }
        }

    temps = city_summary["temps"]
    hums = city_summary["hums"]

    city_summary.update({
        "avg_temp": round(sum(temps) / len(temps), 2) if temps else None,
        "min_temp": min(temps) if temps else None,
        "max_temp": max(temps) if temps else None,
        "avg_hum": round(sum(hums) / len(hums), 2) if hums else None,
        "min_hum": min(hums) if hums else None,
        "max_hum": max(hums) if hums else None,
        "total_temp_records": len(temps),
        "total_hum_records": len(hums)
    })

    return city_summary, sensors_summary


def aggregate_data(data, sensor_names, level="minutely"):
    new_data = {}
    
    if level == "minutely":
        time_keys = [f"{h:02d}:{m:02d}" for h in range(24) for m in range(60)]
        for tk in time_keys:
            new_data[tk] = {}
            for s in sensor_names:
                samples = [data[f"{tk}:{sec:02d}"][s] for sec in range(0, 60, 5)]
                new_data[tk][s] = {
                    "temp": get_average([samp["temp"] for samp in samples]),
                    "hum":  get_average([samp["hum"] for samp in samples])
                }
                
    elif level == "hourly":
        time_keys = [f"{h:02d}" for h in range(24)]
        for tk in time_keys:
            new_data[tk] = {}
            for s in sensor_names:
                samples = [data[f"{tk}:{m:02d}"][s] for m in range(60)]
                new_data[tk][s] = {
                    "temp": get_average([samp["temp"] for samp in samples]),
                    "hum":  get_average([samp["hum"] for samp in samples])
                }
    return new_data