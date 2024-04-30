import pandas as pd
import json
import os

def process_files(filenames, out_path):
    data_dict = {}
    for filename in filenames:
        try:
            parts = filename.split('-')
            thread = parts[1]
            core = parts[2]
            key = f"{thread}-{core}"
            df = pd.read_csv(filename, sep=r'\s+')
            df = df.loc[df['#type'] == 'read'] 
            df = df[['target', 'QPS', 'p95']].rename(columns={'target': 'target_QPS', 'QPS': 'achieved_QPS', 'p95': 'p95'})
            if key not in data_dict:
                data_dict[key] = [[] for _ in range(3)]
            run_index = int(parts[3].split('.')[0])
            data_dict[key][run_index-1].append(df.to_dict('records'))

        except Exception as e:
            print(f"Error processing file {filename}: {e}")

    output_file = f'{out_path}/output.json'
    with open(output_file, 'w') as json_file:
        json.dump(data_dict, json_file, indent=4)
    print(f"Data has been written to {output_file}")


if __name__ == "__main__":
    base_path = 'part4/measurements/1/'
    output_path = 'part4/'
    filenames = [os.path.join(base_path, file) for file in os.listdir(base_path) 
                 if (file.endswith('.txt') and not file.endswith('0.txt'))]
    process_files(filenames, output_path)
