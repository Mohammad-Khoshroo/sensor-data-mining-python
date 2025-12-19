import pandas as pd
import numpy as np
import random
import io

input_file = 'data/raw/SENSOR01_SPECIFIC_DAY.CSV'
output_file = 'data/raw/SENSOR01_DIRTY.CSV'

CORRUPTION_RATE = 0.15   
SPACE_CHANCE = 0.001      
BLANK_LINE_CHANCE = 0.05

df = pd.read_csv(input_file, sep=';')
df['Temperature (C)'] = df['Temperature (C)'].astype(object)
df['Humidity (%)'] = df['Humidity (%)'].astype(object)

drop_indices = df.sample(frac=0.05).index
df = df.drop(drop_indices)

for col in ['Temperature (C)', 'Humidity (%)']:
    for i in df.index:
        if random.random() < CORRUPTION_RATE:
            rand_val = random.random()
            if rand_val < 0.4:
                df.at[i, col] = ""
            elif rand_val < 0.7:
                df.at[i, col] = 'NaN'
            else:
                df.at[i, col] = random.choice([-99, 999, 0.0])

for i in df.index:
    if random.random() < 0.02:
        df.at[i, 'Time'] = "25:70"

def add_whitespace(val):
    val_str = str(val) if pd.notnull(val) else ""
    if random.random() < SPACE_CHANCE:
        before = " " * random.randint(1, 2)
        after = " " * random.randint(1, 2)
        return f"{before}{val_str}{after}"
    return val_str

df = df.map(add_whitespace)

output = io.StringIO()
df.to_csv(output, index=False, sep=';', quoting=3, escapechar=' ')
lines = output.getvalue().split('\n')

final_lines = []
for line in lines:
    final_lines.append(line)
    if line.strip() and random.random() < BLANK_LINE_CHANCE:
        for _ in range(random.randint(1, 2)):
            final_lines.append("")

with open(output_file, 'w') as f:
    f.write('\n'.join(final_lines))
    
print(f"Generated corrupted file at: {output_file}")