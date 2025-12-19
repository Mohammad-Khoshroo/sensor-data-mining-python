import pandas as pd
import numpy as np
import random
import io
import os
from datetime import datetime

# Set corruption parameters
CORRUPTION_RATE = 0.15   # 15% chance to corrupt numeric values
SPACE_CHANCE = 0.001     # Chance to add whitespace
BLANK_LINE_CHANCE = 0.05 # 5% chance to add blank lines

# File ranges - Updated to match your expected files
days = [f"{i:02d}" for i in range(1, 11)]      # Days 01 to 10
sensors = [str(i) for i in range(1, 10)]       # Sensors 1 to 9

# Setup logging with timestamp
log_filename = f"data/raw/_corruption.log"

class CorruptionStats:
    """Class to track corruption statistics"""
    def __init__(self):
        self.total_files = 0
        self.successful = 0
        self.failed = 0
        self.skipped = 0
        self.total_rows_processed = 0
        self.rows_corrupted = 0
        self.rows_deleted = 0
        self.empty_files = 0
        self.corruption_by_type = {
            'empty_string': 0,
            'nan_string': 0,
            'invalid_number': 0,
            'time_corrupted': 0,
            'whitespace_added': 0,
            'blank_lines_added': 0
        }

def corrupt_file_with_stats(input_file_path, output_file_path, stats, log_file):
    """Corrupt a file and collect detailed statistics"""
    
    if not os.path.exists(input_file_path):
        log_file.write(f"ERROR: Input file not found: {input_file_path}\n")
        stats.failed += 1
        return False
    
    try:
        # Read the original file
        df = pd.read_csv(input_file_path, sep=';', low_memory=False)
        original_rows = len(df)
        stats.total_rows_processed += original_rows
        
        if original_rows == 0:
            log_file.write(f"SKIPPED: File is empty: {input_file_path}\n")
            df.to_csv(output_file_path, index=False, sep=';')
            stats.skipped += 1
            stats.empty_files += 1
            return True
        
        # Convert columns to object type
        for col in ['Temperature (C)', 'Humidity (%)']:
            if col in df.columns:
                df[col] = df[col].astype(object)
        
        # 1. Randomly delete 5% of rows
        drop_indices = df.sample(frac=0.05).index
        rows_deleted = len(drop_indices)
        df = df.drop(drop_indices)
        stats.rows_deleted += rows_deleted
        
        # Initialize counters
        temp_corruptions = 0
        humidity_corruptions = 0
        time_corruptions = 0
        whitespace_additions = 0
        
        # 2. Corrupt numeric values
        for col in ['Temperature (C)', 'Humidity (%)']:
            if col in df.columns:
                for i in df.index:
                    if random.random() < CORRUPTION_RATE:
                        stats.rows_corrupted += 1
                        if col == 'Temperature (C)':
                            temp_corruptions += 1
                        else:
                            humidity_corruptions += 1
                            
                        rand_val = random.random()
                        if rand_val < 0.4:
                            df.at[i, col] = ""
                            stats.corruption_by_type['empty_string'] += 1
                        elif rand_val < 0.7:
                            df.at[i, col] = 'NaN'
                            stats.corruption_by_type['nan_string'] += 1
                        else:
                            df.at[i, col] = random.choice([-99, 999, 0.0])
                            stats.corruption_by_type['invalid_number'] += 1
        
        # 3. Corrupt Time column
        if 'Time' in df.columns:
            for i in df.index:
                if random.random() < 0.02:
                    df.at[i, 'Time'] = "25:70"
                    stats.corruption_by_type['time_corrupted'] += 1
                    time_corruptions += 1
        
        # 4. Add whitespace to values
        def add_whitespace_with_stats(val):
            nonlocal whitespace_additions
            val_str = str(val) if pd.notnull(val) else ""
            if random.random() < SPACE_CHANCE:
                before = " " * random.randint(1, 2)
                after = " " * random.randint(1, 2)
                stats.corruption_by_type['whitespace_added'] += 1
                whitespace_additions += 1
                return f"{before}{val_str}{after}"
            return val_str
        
        df = df.map(add_whitespace_with_stats)
        
        # 5. Prepare output and apply random blank lines
        output = io.StringIO()
        df.to_csv(output, index=False, sep=';', quoting=3, escapechar=' ')
        lines = output.getvalue().split('\n')
        
        final_lines = []
        blank_lines_count = 0
        for line in lines:
            if not line.strip(): 
                continue  # Skip empty lines from split
            final_lines.append(line)
            if random.random() < BLANK_LINE_CHANCE:
                for _ in range(random.randint(1, 2)):
                    final_lines.append("")
                    blank_lines_count += 1
                    stats.corruption_by_type['blank_lines_added'] += 1
        
        # 6. Write to final file
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(final_lines))
        
        # Log detailed statistics for this file
        log_file.write(f"SUCCESS: {os.path.basename(input_file_path)} -> {os.path.basename(output_file_path)}\n")
        log_file.write(f"  Original rows: {original_rows}, After corruption: {len(df)} (Deleted: {rows_deleted})\n")
        log_file.write(f"  Temperature corruptions: {temp_corruptions}\n")
        log_file.write(f"  Humidity corruptions: {humidity_corruptions}\n")
        log_file.write(f"  Time corruptions: {time_corruptions}\n")
        log_file.write(f"  Whitespace additions: {whitespace_additions}\n")
        log_file.write(f"  Blank lines added: {blank_lines_count}\n")
        
        stats.successful += 1
        return True
        
    except Exception as e:
        log_file.write(f"ERROR processing {input_file_path}: {str(e)}\n")
        stats.failed += 1
        return False

def generate_detailed_report(stats, log_file, processing_time):
    """Generate a comprehensive report of corruption statistics"""
    
    log_file.write("\n" + "=" * 70 + "\n")
    log_file.write("CORRUPTION PROCESS DETAILED REPORT\n")
    log_file.write("=" * 70 + "\n\n")
    
    # File Statistics
    log_file.write("FILE PROCESSING SUMMARY:\n")
    log_file.write("-" * 40 + "\n")
    log_file.write(f"Total files attempted: {stats.total_files}\n")
    log_file.write(f"Successfully corrupted: {stats.successful}\n")
    log_file.write(f"Failed: {stats.failed}\n")
    log_file.write(f"Skipped (empty/missing): {stats.skipped}\n")
    log_file.write(f"Empty files: {stats.empty_files}\n")
    
    if stats.total_files > 0:
        success_rate = (stats.successful / stats.total_files) * 100
        log_file.write(f"Success rate: {success_rate:.1f}%\n")
    
    # Row Statistics
    log_file.write("\nROW-LEVEL STATISTICS:\n")
    log_file.write("-" * 40 + "\n")
    log_file.write(f"Total rows processed: {stats.total_rows_processed:,}\n")
    log_file.write(f"Rows with corruption: {stats.rows_corrupted:,}\n")
    log_file.write(f"Rows deleted (5% random): {stats.rows_deleted:,}\n")
    
    if stats.total_rows_processed > 0:
        corruption_rate = (stats.rows_corrupted / stats.total_rows_processed) * 100
        deletion_rate = (stats.rows_deleted / stats.total_rows_processed) * 100
        log_file.write(f"Corruption rate: {corruption_rate:.1f}%\n")
        log_file.write(f"Deletion rate: {deletion_rate:.1f}%\n")
    
    # Corruption Type Breakdown
    log_file.write("\nCORRUPTION TYPE BREAKDOWN:\n")
    log_file.write("-" * 40 + "\n")
    total_corruptions = sum(stats.corruption_by_type.values())
    
    for corruption_type, count in stats.corruption_by_type.items():
        if total_corruptions > 0:
            percentage = (count / total_corruptions) * 100
            log_file.write(f"{corruption_type.replace('_', ' ').title()}: {count:,} ({percentage:.1f}%)\n")
        else:
            log_file.write(f"{corruption_type.replace('_', ' ').title()}: {count:,}\n")
    
    # Performance Metrics
    log_file.write("\nPERFORMANCE METRICS:\n")
    log_file.write("-" * 40 + "\n")
    log_file.write(f"Processing time: {processing_time:.2f} seconds\n")
    
    if stats.successful > 0:
        avg_time_per_file = processing_time / stats.successful
        log_file.write(f"Average time per file: {avg_time_per_file:.3f} seconds\n")
    
    # Expected vs Actual Corruption Rates
    log_file.write("\nEXPECTED VS ACTUAL CORRUPTION RATES:\n")
    log_file.write("-" * 40 + "\n")
    log_file.write(f"Expected corruption rate: {CORRUPTION_RATE * 100:.0f}%\n")
    
    if stats.total_rows_processed > 0:
        actual_corruption_rate = (stats.rows_corrupted / stats.total_rows_processed) * 100
        log_file.write(f"Actual corruption rate: {actual_corruption_rate:.1f}%\n")
        
        # Calculate for Temperature/Humidity specifically (since CORRUPTION_RATE applies to each)
        if stats.total_rows_processed > 0:
            expected_corrupted_values = stats.total_rows_processed * 2 * CORRUPTION_RATE  # 2 columns per row
            actual_corrupted_values = stats.corruption_by_type['empty_string'] + \
                                     stats.corruption_by_type['nan_string'] + \
                                     stats.corruption_by_type['invalid_number']
            log_file.write(f"Expected corrupted values: {expected_corrupted_values:.0f}\n")
            log_file.write(f"Actual corrupted values: {actual_corrupted_values:,}\n")

def main():
    """Main function to process all files with corruption and detailed reporting"""
    
    # Create directory if it doesn't exist
    if not os.path.exists('data/raw'):
        os.makedirs('data/raw')
    
    # Initialize statistics tracker
    stats = CorruptionStats()
    
    # Start timing
    start_time = datetime.now()
    
    with open(log_filename, 'w', encoding='utf-8') as log_file:
        log_file.write(f"DATA CORRUPTION PROCESS WITH DETAILED REPORTING\n")
        log_file.write(f"Generated: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"Target: {len(days)} days × {len(sensors)} sensors = {len(days)*len(sensors)} files\n")
        log_file.write(f"Corruption Rate: {CORRUPTION_RATE*100:.0f}%\n")
        log_file.write("=" * 70 + "\n\n")
        
        print(f"Starting corruption process with detailed reporting...")
        print(f"Days: {len(days)} days (01-10)")
        print(f"Sensors: {len(sensors)} sensors (1-9)")
        print(f"Expected files: {len(days)*len(sensors)}\n")
        
        # Process all combinations of days and sensors
        for day in days:
            day_start = datetime.now()
            files_for_day = 0
            successful_for_day = 0
            
            print(f"Processing day {day}...")
            log_file.write(f"\nPROCESSING DAY {day}:\n")
            log_file.write("-" * 50 + "\n")
            
            for sensor in sensors:
                stats.total_files += 1
                files_for_day += 1
                
                # Format sensor number with leading zero
                sensor_num = f"0{sensor}" if len(sensor) == 1 else sensor
                
                # Define input and output file paths
                # Changed to match your actual file naming convention
                input_path = f'data/raw/SENSOR{sensor_num}_DAY{day}.csv'
                output_path = f'data/raw/SENSOR{sensor_num}_DAY{day}_raw.csv'
                
                # Corrupt the file with statistics
                if corrupt_file_with_stats(input_path, output_path, stats, log_file):
                    successful_for_day += 1
            
            day_time = (datetime.now() - day_start).total_seconds()
            log_file.write(f"\nDay {day} Summary:\n")
            log_file.write(f"  Files processed: {files_for_day}\n")
            log_file.write(f"  Successful: {successful_for_day}\n")
            log_file.write(f"  Time taken: {day_time:.2f} seconds\n")
            print(f"  Day {day}: {successful_for_day}/{files_for_day} files corrupted")
        
        # Calculate total processing time
        total_time = (datetime.now() - start_time).total_seconds()
        
        # Generate detailed report
        generate_detailed_report(stats, log_file, total_time)
        
        # Final summary in log
        log_file.write("\n" + "=" * 70 + "\n")
        log_file.write("PROCESSING COMPLETE\n")
        log_file.write("=" * 70 + "\n")
        log_file.write(f"Total processing time: {total_time:.2f} seconds\n")
        log_file.write(f"Report saved to: {log_filename}\n")
    
    # Console output summary
    print(f"\n" + "=" * 60)
    print("CORRUPTION PROCESS COMPLETE - SUMMARY")
    print("=" * 60)
    print(f"Total files: {stats.total_files}")
    print(f"Successfully corrupted: {stats.successful}")
    print(f"Failed: {stats.failed}")
    print(f"Skipped: {stats.skipped}")
    print(f"Total rows processed: {stats.total_rows_processed:,}")
    print(f"Rows corrupted: {stats.rows_corrupted:,}")
    print(f"Processing time: {total_time:.2f} seconds")
    print(f"Detailed report: {log_filename}")
    
    # Print corruption type breakdown to console
    if sum(stats.corruption_by_type.values()) > 0:
        print(f"\nCorruption Types:")
        for corruption_type, count in stats.corruption_by_type.items():
            if count > 0:
                print(f"  {corruption_type.replace('_', ' ').title()}: {count:,}")

if __name__ == "__main__":
    main()