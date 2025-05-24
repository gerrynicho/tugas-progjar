import os
import logging
import random

def create_file(filename, size_in_mb):
    """Create a file with random content of specified size"""
    size_in_bytes = size_in_mb * 1024 * 1024
    
    logging.info(f"Creating {filename} with size {size_in_mb}MB...")
    
    chunk_size = 1024 * 1024  # 1MB
    with open(filename, 'wb') as f:
        remaining_bytes = size_in_bytes
        while remaining_bytes > 0:
            current_chunk_size = min(chunk_size, remaining_bytes)
            
            # create random data for chunk
            chunk = os.urandom(current_chunk_size)
            
            f.write(chunk)
            
            remaining_bytes -= current_chunk_size
            
            # if making file is still > 10MB, log progress every 10MB 
            if size_in_mb >= 50 and remaining_bytes % (10 * 1024 * 1024) == 0:
                mb_done = (size_in_bytes - remaining_bytes) / (1024 * 1024)
                logging.info(f"  Progress: {mb_done}MB / {size_in_mb}MB")
    
    actual_size = os.path.getsize(filename) / (1024 * 1024) # divide by 1MB to get MB
    logging.info(f"Created {filename}: {actual_size:.2f}MB")

def create_test_files(file_sizes=None):
    """Create test files with specified sizes"""
    if file_sizes is None:
        file_sizes = [10, 50, 100]
    
    files_to_create = [(f"test_file_{size}mb.bin", size) for size in file_sizes]
    
    for filename, size_mb in files_to_create:
        create_file(filename, size_mb)
    
    logging.info("All test files created successfully!")