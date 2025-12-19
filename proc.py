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