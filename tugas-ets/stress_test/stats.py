import pandas as pd
import matplotlib.pyplot as plt
import os
import logging

def generate_summary_report(csv_file):
    """Generate a summary report from a CSV file"""
    if not os.path.exists(csv_file):
        logging.error(f"CSV file not found: {csv_file}")
        return None
    
    try:
        df = pd.read_csv(csv_file)
        
        # Convert string columns to appropriate types
        df['Throughput per client (MB/s)'] = pd.to_numeric(df['Throughput per client (MB/s)'], errors='coerce')
        df['Waktu total per client (s)'] = pd.to_numeric(df['Waktu total per client (s)'], errors='coerce')
        
        # Group by operation, file size, and client pool size
        grouped = df.groupby(['Operasi', 'Volume File (MB)', 'Jumlah client worker pool'])
        
        # Calculate average metrics
        summary = grouped.agg({
            'Throughput per client (MB/s)': 'mean',
            'Waktu total per client (s)': 'mean',
            'Jumlah worker client sukses': 'sum',
            'Jumlah worker client gagal': 'sum'
        }).reset_index()
        
        # Calculate success rate
        summary['Success Rate (%)'] = (summary['Jumlah worker client sukses'] / 
                                      (summary['Jumlah worker client sukses'] + 
                                       summary['Jumlah worker client gagal'])) * 100
        
        # Save summary to CSV
        summary_file = csv_file.replace('.csv', '_summary.csv')
        summary.to_csv(summary_file, index=False)
        logging.info(f"Summary report saved to {summary_file}")
        
        return summary_file
    except Exception as e:
        logging.error(f"Error generating summary report: {str(e)}")
        return None

def plot_results(csv_file, output_dir="results"):
    """Generate plots from test results CSV file"""
    if not os.path.exists(csv_file):
        logging.error(f"CSV file not found: {csv_file}")
        return
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        df = pd.read_csv(csv_file)
        
        # Convert string columns to appropriate types
        df['Throughput per client (MB/s)'] = pd.to_numeric(df['Throughput per client (MB/s)'], errors='coerce')
        df['Waktu total per client (s)'] = pd.to_numeric(df['Waktu total per client (s)'], errors='coerce')
        
        # Plot throughput vs client pool size for different operations
        plt.figure(figsize=(12, 8))
        for operation in df['Operasi'].unique():
            for file_size in df['Volume File (MB)'].unique():
                op_data = df[(df['Operasi'] == operation) & (df['Volume File (MB)'] == file_size)]
                if not op_data.empty:
                    plt.plot(
                        op_data['Jumlah client worker pool'],
                        op_data['Throughput per client (MB/s)'],
                        marker='o',
                        label=f"{operation} - {file_size}MB"
                    )
        
        plt.xlabel('Number of Concurrent Clients')
        plt.ylabel('Throughput (MB/s)')
        plt.title('Throughput vs Concurrent Clients')
        plt.legend()
        plt.grid(True)
        
        plot_file = os.path.join(output_dir, 'throughput_vs_clients.png')
        plt.savefig(plot_file)
        logging.info(f"Plot saved to {plot_file}")
        
        # Plot response time vs client pool size
        plt.figure(figsize=(12, 8))
        for operation in df['Operasi'].unique():
            for file_size in df['Volume File (MB)'].unique():
                op_data = df[(df['Operasi'] == operation) & (df['Volume File (MB)'] == file_size)]
                if not op_data.empty:
                    plt.plot(
                        op_data['Jumlah client worker pool'],
                        op_data['Waktu total per client (s)'],
                        marker='o',
                        label=f"{operation} - {file_size}MB"
                    )
        
        plt.xlabel('Number of Concurrent Clients')
        plt.ylabel('Response Time (s)')
        plt.title('Response Time vs Concurrent Clients')
        plt.legend()
        plt.grid(True)
        
        plot_file = os.path.join(output_dir, 'response_time_vs_clients.png')
        plt.savefig(plot_file)
        logging.info(f"Plot saved to {plot_file}")
        
    except Exception as e:
        logging.error(f"Error generating plots: {str(e)}")