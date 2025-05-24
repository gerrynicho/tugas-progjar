#!/usr/bin/env python3
import sys
import logging
import argparse
import time
import itertools
import os
from stress_test.client import Client
from stress_test.file_utils import create_test_files
from stress_test.stats import generate_summary_report, plot_results

def parse_arguments():
    parser = argparse.ArgumentParser(description='File Server Stress Test')
    
    # Add a full-test argument
    parser.add_argument('--full-test', action='store_true', default=False,
                      help='Run all parameter combinations (default when no args specified)')
    
    parser.add_argument('--host', default='localhost', 
                        help='Server hostname or IP address')
    parser.add_argument('--port', type=int, default=6666,
                        help='Server port')
    
    parser.add_argument('--operation', choices=['upload', 'get', 'list', 'all'], 
                        default='all', help='Operation to test')
    
    parser.add_argument('--file-size', type=int, default=10,
                        help='File size in MB for upload/get tests')
    
    parser.add_argument('--clients', type=int, default=5,
                        help='Number of concurrent clients')
    
    parser.add_argument('--executor', choices=['thread', 'process', 'both'], 
                        default='thread', help='Type of executor to use')
    
    parser.add_argument('--batch-size', type=int, default=None,
                        help='Maximum number of workers to run at once (to prevent resource exhaustion)')
    
    parser.add_argument('--plot', action='store_true',
                        help='Generate plots from results')
    
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # If no arguments provided, default to full test
    if len(sys.argv) == 1:
        args.full_test = True
    
    return args

def run_full_test(client, batch_size=10, verbose=False):
    """Run a full test with all parameter combinations"""
    # Define all parameter combinations
    operations = ['get', 'upload']
    file_sizes = [10, 50, 100]
    client_pool_sizes = [1, 5, 50]
    executor_types = ['thread', 'process']
    
    all_stats = []
    total_tests = len(operations) * len(file_sizes) * len(client_pool_sizes) * len(executor_types)
    test_count = 0
    
    # Get server configuration
    print("\n=== SERVER CONFIGURATION ===")
    client.server_config['executor_type'] = input("Enter Server's executor type (thread/process): ").strip().lower() or 'thread'
    pool_input = input("Enter Server's worker pool size (default 20): ").strip()
    client.server_config['worker_pool_size'] = int(pool_input) if pool_input else 20
    
    print(f"\nRunning full test suite with {total_tests} combinations...")
    
    # Iterate through all combinations
    for operation, file_size, pool_size, exec_type in itertools.product(
        operations, file_sizes, client_pool_sizes, executor_types):
        
        test_count += 1
        print(f"\nTest {test_count}/{total_tests}: {operation.upper()} test with {file_size}MB files, " 
              f"{pool_size} clients, {exec_type} executor")
        
        # For large client pools, adjust batch size to prevent resource exhaustion
        current_batch_size = min(batch_size, pool_size)
        if pool_size > 20:
            current_batch_size = 10  # More conservative for large pools
        
        stats = client.run_test(
            operation=operation,
            file_size_mb=file_size,
            client_pool_size=pool_size,
            executor_type=exec_type,
            batch_size=current_batch_size
        )
        
        if stats:
            # Add server info to stats
            stats['server_pool_size'] = client.server_config['worker_pool_size']
            all_stats.append(stats)
        
        # Short delay between tests to let system recover
        time.sleep(2)
        
        # Cleanup between tests to prevent disk fill
        client.cleanup()
    
    return all_stats

def main():
    args = parse_arguments()
    
    # Setup logging
    log_level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(level=log_level, 
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
                            logging.FileHandler("stress_test.log"),
                            logging.StreamHandler()
                        ])
    
    # Initialize client
    client = Client(server_address=(args.host, args.port))
    
    # Determine batch size if not specified
    if args.batch_size is None:
        if args.clients <= 10:
            args.batch_size = args.clients
        else:
            args.batch_size = 10
    
    all_stats = []
    
    if args.full_test:
        # Run full parameter sweep
        all_stats = run_full_test(client, batch_size=args.batch_size, verbose=args.verbose)
    else:
        # Run single test with specified parameters
        # Get server configuration
        client.server_config['executor_type'] = input("Enter Server's executor type (thread/process): ").strip().lower() or 'thread'
        pool_input = input("Enter Server's worker pool size (default 20): ").strip()
        client.server_config['worker_pool_size'] = int(pool_input) if pool_input else 20
        
        operations = ['get', 'upload'] if args.operation == 'all' else [args.operation]
        executor_types = ['thread', 'process'] if args.executor == 'both' else [args.executor]
        
        # Run the specified test combinations
        for operation in operations:
            for exec_type in executor_types:
                logging.info(f"Running {operation.upper()} test with {args.clients} clients using {exec_type} executor")
                
                stats = client.run_test(
                    operation=operation,
                    file_size_mb=args.file_size,
                    client_pool_size=args.clients,
                    executor_type=exec_type,
                    batch_size=args.batch_size
                )
                
                if stats:
                    # Add server info to stats
                    stats['server_pool_size'] = client.server_config['worker_pool_size']
                    all_stats.append(stats)
                
                # Small delay between tests
                time.sleep(1)
    
    # Save results
    if all_stats:
        csv_file = client.save_results_to_csv(all_stats)
        print(f"\nResults saved to {csv_file}")
        
        # Generate summary report
        if args.plot:
            summary_file = generate_summary_report(csv_file)
            plot_results(csv_file)
    
    # Clean up
    client.cleanup()
    
    print("\nTest Summary:")
    for stats in all_stats:
        print(f"- {stats['operation'].upper()} test:")
        print(f"  File size: {stats['file_size_mb']} MB, Clients: {stats['client_pool_size']}, Executor: {stats['executor_type']}")
        print(f"  Success: {stats['success_count']}, Failed: {stats['fail_count']}")
        if stats.get('avg_throughput', 0) > 0:
            print(f"  Avg throughput: {stats['avg_throughput']/1024/1024:.2f} MB/s")
        print(f"  Avg duration: {stats.get('avg_duration', 0):.2f}s")
        print()

if __name__ == "__main__":
    main()