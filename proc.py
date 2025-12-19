import math

def validate_field(val_raw, field_name, v_range, invalid_tokens):
    """
    Validates a single field (Temperature or Humidity).
    Categorizes errors into Missing Data, Invalid Data, or Sensor Fault [cite: 89-91, 150].
    """
    error_type = None
    final_val = "NoF"
    
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
    """Identifies time slots where no entry was recorded for a sensor[cite: 182]."""
    missing_minutes = set(timeline) - found_minutes
    gaps = []
    for m_time in missing_minutes:
        gaps.append({
            "time": m_time,
            "sensor": sensor,
            "type": "GAP",
            "msg": "Missing data entry",
            "raw": "N/A"
        })
    return gaps

def statistics(normalized_data, sensor_names):
    """
    Calculates advanced metrics including Standard Deviation and City-wide averages.
    Filters out 'NoF' entries to ensure mathematical accuracy.
    """
    stats_summary = {"individual_sensors": {}, "city_metrics": {}}
    all_city_temps = []
    all_city_hums = []

    for s in sensor_names:
        # Extract numeric values only
        temps = [row[s]['temp'] for row in normalized_data.values() if isinstance(row[s]['temp'], (int, float))]
        hums = [row[s]['hum'] for row in normalized_data.values() if isinstance(row[s]['hum'], (int, float))]
        
        all_city_temps.extend(temps)
        all_city_hums.extend(hums)

        def get_std(data, avg):
            if len(data) < 2: return 0
            variance = sum((x - avg) ** 2 for x in data) / len(data)
            return math.sqrt(variance)

        # Individual Sensor Stats
        t_avg = sum(temps) / len(temps) if temps else 0
        h_avg = sum(hums) / len(hums) if hums else 0
        
        stats_summary["individual_sensors"][s] = {
            "temperature": {
                "avg": round(t_avg, 2),
                "min": min(temps) if temps else "N/A",
                "max": max(temps) if temps else "N/A",
                "std": round(get_std(temps, t_avg), 2) if temps else 0
            },
            "humidity": {
                "avg": round(h_avg, 2),
                "min": min(hums) if hums else "N/A",
                "max": max(hums) if hums else "N/A",
                "std": round(get_std(hums, h_avg), 2) if hums else 0
            }
        }

    # City-wide Aggregates (Global Average)
    if all_city_temps:
        stats_summary["city_metrics"]["avg_temp"] = round(sum(all_city_temps) / len(all_city_temps), 2)
    if all_city_hums:
        stats_summary["city_metrics"]["avg_hum"] = round(sum(all_city_hums) / len(all_city_hums), 2)

    return stats_summary