from socket import *
import socket
import multiprocessing
import logging
import time
import sys
from multiprocessing import Process

from file_protocol import FileProtocol
fp = FileProtocol()

def ProcessTheClient(connection, address):
    """Function to be run in a separate process for each client connection"""
    logging.warning(f"Process {multiprocessing.current_process().name} handling connection from {address}")
    
    data_received = ""
    try:
        while True:
            data = connection.recv(1024 * 1024)  # 1MB buffer size
            if data:
                d = data.decode()
                data_received += d
                if data_received.endswith("\r\n\r\n"):
                    hasil = fp.proses_string(data_received[:-4])
                    hasil += "\r\n\r\n"
                    connection.sendall(hasil.encode())
                    data_received = ""
            else:
                break
    except Exception as e:
        logging.warning(f"Error in process {multiprocessing.current_process().name}: {str(e)}")
    finally:
        connection.close()
        logging.warning(f"Connection from {address} closed by process {multiprocessing.current_process().name}")

class Server(Process):
    def __init__(self, ipaddress='0.0.0.0', port=8889, max_processes=10):
        self.ipinfo = (ipaddress, port)
        self.max_processes = max_processes
        self.processes = []
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        Process.__init__(self)
        
    def run(self):
        logging.warning(f"Server starting on {self.ipinfo} with max {self.max_processes} processes")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(5)
        
        try:
            while True:
                connection, client_address = self.my_socket.accept()
                logging.warning(f"New connection from {client_address}")
                
                # clean up completed processes
                self.processes = [p for p in self.processes if p.is_alive()]
                
                # check if reached max processes
                if len(self.processes) >= self.max_processes:
                    logging.warning(f"Reached max processes, waiting for a process to complete")
                    # Wait for a process to finish
                    while len(self.processes) >= self.max_processes:
                        time.sleep(0.1)
                        self.processes = [p for p in self.processes if p.is_alive()]
                
                # create process for new connection
                p = Process(target=ProcessTheClient, args=(connection, client_address))
                p.daemon = True # run in background
                p.start()
                self.processes.append(p)
                
                logging.warning(f"Active processes: {len(self.processes)}/{self.max_processes}")
                
        except KeyboardInterrupt:
            logging.warning("Server interrupted, shutting down...")
        finally:
            self.my_socket.close()

def main():
    logging.basicConfig(level=logging.WARNING, format='%(levelname)s - %(message)s')
    process_worker = int(input("Enter number of processes to handle clients (default 10): "))
    svr = Server(ipaddress='0.0.0.0', port=6666, max_processes=process_worker or 20)
    svr.start()
    
    try:
        svr.join()
    except KeyboardInterrupt:
        logging.warning("Server shutting down")

if __name__ == "__main__":
    main()