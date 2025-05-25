from socket import *
import socket
import threading
import logging
import time
import sys
import multiprocessing
import signal
import json # Added for parsing the response from FileProtocol
import os   # Added for joining paths
from concurrent.futures import ProcessPoolExecutor

from file_protocol import FileProtocol
fp = FileProtocol()

running = True
STREAM_BUFFER_SIZE = 65536 # 64KB, tune as needed

def ProcessTheClient(connection, address):
    logging.warning(f"Handling connection from {address}")
    data_received = ""
    file_to_stream_path = None
    file_to_stream_size = 0

    try:
        initial_command_received = False
        # Stage 1: Receive the client's command and send metadata response
        while not initial_command_received:
            data = connection.recv(1024 * 1024) # Buffer for the command string
            if data:
                d = data.decode()
                data_received += d
                if data_received.endswith("\r\n\r\n"): # Standard command terminator
                    command_to_process = data_received[:-4].strip() # Get the actual command
                    data_received = "" # Reset
                    initial_command_received = True

                    # Get the JSON response string from FileProtocol
                    json_response_string = fp.proses_string(command_to_process)
                    
                    # Send this JSON response (metadata) to the client
                    connection.sendall(json_response_string.encode() + b"\r\n\r\n")

                    # Now, check if this was a GET command that requires file streaming
                    try:
                        response_dict = json.loads(json_response_string)
                        if command_to_process.upper().startswith("GET") and response_dict.get('status') == 'OK_STREAM':
                            filename_to_stream = response_dict.get('data_namafile')
                            file_to_stream_size = response_dict.get('data_filesize')

                            if filename_to_stream and isinstance(file_to_stream_size, int):
                                # Construct the full path to the file on the server
                                # fp.file.file_path is 'files/' from FileInterface
                                file_to_stream_path = os.path.join(fp.file.file_path, os.path.basename(filename_to_stream))
                                logging.info(f"Server: Preparing to stream {file_to_stream_path} ({file_to_stream_size} bytes) for {address}")
                            else:
                                logging.error(f"Server: Invalid metadata for streaming to {address}. Filename: {filename_to_stream}, Size: {file_to_stream_size}")
                                file_to_stream_path = None # Prevent streaming attempt
                        elif response_dict.get('status') != 'OK_STREAM' and command_to_process.upper().startswith("GET"):
                             logging.info(f"Server: GET for {address} resulted in status {response_dict.get('status')}, no file streaming.")


                    except json.JSONDecodeError as je:
                        logging.error(f"Server: Could not parse FileProtocol response as JSON: {je}")
                        file_to_stream_path = None # Safety
            else:
                # No data from client, or client closed connection
                logging.warning(f"Server: No command data received from {address}, closing.")
                return # Exit

        # Stage 2: Stream the raw file data if everything is set for it
        if file_to_stream_path and os.path.exists(file_to_stream_path) and file_to_stream_size >= 0:
            logging.warning(f"Server: Starting stream of {file_to_stream_path} ({file_to_stream_size} bytes) to {address}")
            sent_bytes = 0
            try:
                with open(file_to_stream_path, 'rb') as f:
                    while sent_bytes < file_to_stream_size:
                        # For zero-byte files, this loop won't run, which is correct.
                        # f.read() will return b'' if at EOF.
                        try:
                            chunk = f.read(STREAM_BUFFER_SIZE)
                            if not chunk:
                                # This might happen if file size changed or was read incorrectly
                                logging.warning(f"Server: File ended prematurely while streaming {file_to_stream_path} to {address}. Expected {file_to_stream_size}, sent {sent_bytes}.")
                                break
                            connection.sendall(chunk)
                            sent_bytes += len(chunk)
                        except BrokenPipeError:
                            logging.warning(f"Server: Broken pipe while streaming {file_to_stream_path} to {address}. Client likely disconnected.")
                            break
                        except ConnectionResetError:
                            logging.warning(f"Server: Connection reset while streaming {file_to_stream_path} to {address}. Client likely disconnected.")
                            break
                        except socket.error as se:
                            logging.error(f"Server: Socket error while streaming {file_to_stream_path} to {address}: {se}")
                            break
                    
                if sent_bytes == file_to_stream_size:
                    logging.info(f"Server: Successfully streamed {sent_bytes} bytes for {file_to_stream_path} to {address}.")
                else:
                    logging.warning(f"Server: Streaming finished for {file_to_stream_path}. Sent {sent_bytes}/{file_to_stream_size} bytes to {address}.")
            except Exception as e:
                logging.error(f"Server: Error during file streaming of {file_to_stream_path} to {address}: {e}")
        elif file_to_stream_path: # Path was set, but file not found or size is invalid
            logging.error(f"Server: File {file_to_stream_path} not found or invalid size for streaming to {address}, although metadata was OK_STREAM.")

    except socket.timeout:
        logging.error(f"Socket timeout with {address}")
    except ConnectionResetError:
        logging.warning(f"Connection reset by {address}")
    except BrokenPipeError: # Client disconnected
        logging.warning(f"Broken pipe with {address}, client likely disconnected during streaming.")
    except Exception as e:
        logging.error(f"Error handling client {address}: {str(e)}")
    finally:
        logging.warning(f"Closing connection from {address}")
        connection.close()
class Server(multiprocessing.Process):
    def __init__(self, ipaddress='0.0.0.0', port=8889, max_workers=10):
        self.ipinfo = (ipaddress, port)
        self.max_workers = max_workers
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.running = True  # Local running flag
        multiprocessing.Process.__init__(self)
        
    def stop(self):
        """Method to gracefully stop the server"""
        global running
        running = False
        self.running = False
        # Create a dummy connection to unblock accept()
        try:
            dummy = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            dummy.connect(self.ipinfo)
            dummy.close()
        except Exception as e:
            logging.error(f"Error creating dummy connection: {e}")
    
    def run(self):
        global running
        logging.warning(f"Server starting on {self.ipinfo} with {self.max_workers} workers")
        try:
            self.my_socket.bind(self.ipinfo)
            self.my_socket.listen(64)
            
            # Set a timeout so we can check running flag periodically
            self.my_socket.settimeout(1.0)
            
            # create process pool
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                while running and self.running:
                    try:
                        connection, client_address = self.my_socket.accept()
                        logging.warning(f"connection from {client_address}")
                        
                        # Submit the client connection to the process pool
                        executor.submit(ProcessTheClient, connection, client_address)
                    except socket.timeout:
                        # This allows us to check the running flag periodically
                        continue
                    except Exception as e:
                        logging.error(f"Error accepting connection: {str(e)}")
                        if running and self.running:  # Only log if not due to intentional shutdown
                            logging.error(f"Error in server loop: {e}")
                
                logging.warning("Server loop ended")
        except Exception as e:
            logging.error(f"Error in server: {e}")
        finally:
            self.my_socket.close()
            logging.warning("Server socket closed")

# Signal handler for keyboard interrupt
def signal_handler(sig, frame):
    global running
    running = False
    logging.warning("SIGINT received, shutting down...")

def main():
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    
    # server with user-defined worker count
    process_worker = int(input("Enter number of processes to handle clients (default 10): ") or "10")
    svr = Server(ipaddress='0.0.0.0', port=6666, max_workers=process_worker)
    svr.start()

    try:
        logging.warning("Server is running. Press Ctrl+C to stop.")
        while running:
            time.sleep(1)
        
        # Signal the server to stop
        svr.stop()
        
        # Wait for the server process to finish
        svr.join(timeout=5)
        logging.warning("Server shutdown completed.")
    except Exception as e:
        logging.error(f"Error in main: {e}")
        svr.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format='%(levelname)s - %(message)s')
    main()