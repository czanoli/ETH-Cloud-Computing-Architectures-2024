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
            df = pd.read_csv(filename, sep=r'\s+')
            df = df.loc[df['#type'] == 'read'] 
            df = df[['target', 'QPS', 'p95']].rename(columns={'QPS': 'achieved', 'p95': 'p95'})
            if core not in data_dict:
                data_dict[core] = {}
            if thread not in data_dict[core]:
                data_dict[core][thread] = []

            run_index = int(parts[3].split('.')[0])
            data_dict[core][thread].extend(df.to_dict('records'))

        except Exception as e:
            print(f"Error processing file {filename}: {e}")

    for c in data_dict:
        for t in data_dict[c]:
            df = pd.DataFrame.from_records(data_dict[c][t])
            df = df.groupby(['target'], as_index=False).mean()
            data_dict[c][t] = df.to_dict('records')

    output_file = os.path.join(out_path, 'target_achieved.json')
    with open(output_file, 'w') as json_file:
        json.dump(data_dict, json_file, indent=2)
    print(f"Data has been written to {output_file}")


if __name__ == "__main__":
    base_path = 'part4/measurements/1/'
    output_path = 'part4/'
    filenames = [os.path.join(base_path, file) for file in os.listdir(base_path) 
                 if file.endswith('.txt') and not file.endswith('0.txt')]
    process_files(filenames, output_path)
