import os
import pandas as pd
import glob

def merge_all_csv(directory_path, output_file):
    """
    Merge all CSV files in the specified directory into one CSV file.
    """
    # Check if the directory exists
    if not os.path.exists(directory_path):
        print(f"Directory '{directory_path}' does not exist")
        return False
    
    # Find all CSV files in the directory
    csv_files = glob.glob(os.path.join(directory_path, '*.csv'))
    
    if not csv_files:
        print(f"No CSV files found in '{directory_path}'")
        return False
    
    print(f"Found {len(csv_files)} CSV files to merge")
    
    # Read and combine all CSV files
    all_dataframes = []
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            # Add source file information (optional)
            df['source_file'] = os.path.basename(csv_file)
            all_dataframes.append(df)
            print(f"Read {csv_file}: {len(df)} rows")
        except Exception as e:
            print(f"Error reading {csv_file}: {str(e)}")
    
    if not all_dataframes:
        print("No data could be read from the CSV files")
        return False
    
    # Concatenate all dataframes
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    # Save the combined dataframe to the output file
    try:
        combined_df.to_csv(output_file, index=False)
        print(f"Successfully merged {len(all_dataframes)} CSV files into {output_file}")
        print(f"Total rows in merged file: {len(combined_df)}")
        return True
    except Exception as e:
        print(f"Error saving merged data to {output_file}: {str(e)}")
        return False

if __name__ == "__main__":
    results_dir = 'results'
    output_file = os.path.join(results_dir, 'all_results_combined.csv')
    
    merge_all_csv(results_dir, output_file)