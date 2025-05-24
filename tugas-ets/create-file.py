import os
import random
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_file(filename, size_in_mb):
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

def main():
    files_to_create = [
        ("test_file_10mb.bin", 10),
        ("test_file_50mb.bin", 50),
        ("test_file_100mb.bin", 100)
    ]
    
    for filename, size_mb in files_to_create:
        create_file(filename, size_mb)
    
    logging.info("All files created successfully!")

if __name__ == "__main__":
    main()