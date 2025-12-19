import pandas as pd

input_file = 'data/raw/SENSOR05.CSV'
output_file = 'data/raw/SENSOR05_SPECIFIC_DAY.CSV'
target_date = '02.09.2024'

df = pd.read_csv(input_file, sep=';', low_memory=False)

df = df[df['Date'] != 'Date']
df = df.dropna(subset=['Date', 'Time'])

df = df[df['Date'] == target_date]

df['Temperature (C)'] = pd.to_numeric(df['Temperature (C)'], errors='coerce')
df['Humidity (%)'] = pd.to_numeric(df['Humidity (%)'], errors='coerce')

df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d.%m.%Y %H:%M:%S', errors='coerce')
df = df.dropna(subset=['Datetime'])

df.set_index('Datetime', inplace=True)
df_resampled = df.resample('1min').first()

df_resampled = df_resampled.reset_index()
df_resampled['Date'] = df_resampled['Datetime'].dt.strftime('%d.%m.%Y')

df_resampled['Time'] = df_resampled['Datetime'].dt.strftime('%H:%M')

final_cols = ['Date', 'Time', 'Temperature (C)', 'Humidity (%)']
df_resampled[final_cols].to_csv(output_file, index=False, sep=';')

print(f"Filtered data for {target_date} saved to {output_file}.")