import pandas as pd
from datetime import datetime
import os

# Define ranges
days = [f"{i:02d}" for i in range(1, 11)]  # 01 to 10
sensors = [str(i) for i in range(1, 10)]   # 1 to 9

# Setup
log_file = f"data/raw/_separation.log"
empty_df = pd.DataFrame(columns=['Date', 'Time', 'Temperature (C)', 'Humidity (%)'])

results = []

with open(log_file, 'w', encoding='utf-8') as log:
    log.write(f"Batch Processing Report\n")
    log.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    log.write(f"Days: {', '.join(days)}\n")
    log.write(f"Sensors: {', '.join(sensors)}\n")
    log.write("=" * 60 + "\n\n")
    
    for day in days:
        target_date = f"{day}.09.2024"
        print(f"\nProcessing day {day}...")
        log.write(f"\nDay {day} ({target_date}):\n")
        
        day_results = {"day": day, "total": 0, "with_data": 0, "empty": 0}
        
        for sensor in sensors:
            # Format sensor number with leading zero
            sensor_num = f"0{sensor}" if len(sensor) == 1 else sensor
            input_path = f'data/raw/SENSOR{sensor_num}.CSV'
            output_path = f'data/raw/SENSOR{sensor_num}_DAY{day}.csv'
            
            day_results["total"] += 1
            
            try:
                if not os.path.exists(input_path):
                    empty_df.to_csv(output_path, index=False, sep=';')
                    log.write(f"  Sensor {sensor}: EMPTY (file not found)\n")
                    day_results["empty"] += 1
                    continue
                
                df = pd.read_csv(input_path, sep=';', low_memory=False)
                df = df[df['Date'] != 'Date']
                df = df.dropna(subset=['Date', 'Time'])
                df = df[df['Date'] == target_date]
                
                if df.empty:
                    empty_df.to_csv(output_path, index=False, sep=';')
                    log.write(f"  Sensor {sensor}: EMPTY (no data)\n")
                    day_results["empty"] += 1
                    continue
                
                # Process data
                df['Temperature (C)'] = pd.to_numeric(df['Temperature (C)'], errors='coerce')
                df['Humidity (%)'] = pd.to_numeric(df['Humidity (%)'], errors='coerce')
                
                df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], 
                                                format='%d.%m.%Y %H:%M:%S', errors='coerce')
                df = df.dropna(subset=['Datetime'])
                df = df.sort_values(by='Datetime')
                df['Time'] = df['Datetime'].dt.strftime('%H:%M:%S')
                
                final_cols = ['Date', 'Time', 'Temperature (C)', 'Humidity (%)']
                df[final_cols].to_csv(output_path, index=False, sep=';')
                
                log.write(f"  Sensor {sensor}: SUCCESS ({len(df)} rows)\n")
                day_results["with_data"] += 1
                
                print(f"  Sensor {sensor}: {len(df)} rows")
                
            except Exception as e:
                empty_df.to_csv(output_path, index=False, sep=';')
                log.write(f"  Sensor {sensor}: ERROR - {str(e)[:50]}...\n")
                day_results["empty"] += 1
        
        results.append(day_results)
        log.write(f"  Summary: {day_results['with_data']}/{day_results['total']} with data\n")
    
    # Final report
    log.write("\n" + "=" * 60 + "\n")
    log.write("FINAL SUMMARY\n")
    log.write("=" * 60 + "\n")
    
    total_files = sum(r["total"] for r in results)
    total_with_data = sum(r["with_data"] for r in results)
    total_empty = sum(r["empty"] for r in results)
    
    log.write(f"Total days processed: {len(days)}\n")
    log.write(f"Total sensors per day: {len(sensors)}\n")
    log.write(f"Total output files: {total_files}\n")
    log.write(f"Files with data: {total_with_data}\n")
    log.write(f"Empty files: {total_empty}\n")
    
    if total_files > 0:
        coverage = (total_with_data / total_files) * 100
        log.write(f"Data coverage: {coverage:.1f}%\n")
    
    # Day-by-day breakdown
    log.write("\nDay-by-day breakdown:\n")
    for r in results:
        coverage = (r["with_data"] / r["total"] * 100) if r["total"] > 0 else 0
        log.write(f"  Day {r['day']}: {r['with_data']}/{r['total']} ({coverage:.1f}%)\n")

print(f"\n✅ Processing complete!")
print(f"📊 Generated {total_files} files")
print(f"📈 {total_with_data} files contain data ({coverage:.1f}% coverage)")
print(f"📁 Log file: {log_file}")