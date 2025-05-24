from socket import *
import socket
import threading
import logging
import time
import sys
from concurrent.futures import ThreadPoolExecutor

from file_protocol import FileProtocol
fp = FileProtocol()

# Function to handle client connections
def ProcessTheClient(connection, address):
    logging.warning(f"Handling connection from {address}")
    data_received = ""
    try:
        while True:
            data = connection.recv(1024 * 1024) # 1MB buffer size
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
        logging.error(f"Error handling client: {str(e)}")
    finally:
        connection.close()
        logging.warning(f"Connection from {address} closed")

class Server(threading.Thread):
    def __init__(self, ipaddress='0.0.0.0', port=8889, max_workers=10):
        self.ipinfo = (ipaddress, port)
        self.max_workers = max_workers
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        threading.Thread.__init__(self)
        
    def run(self):
        logging.warning(f"Server starting on {self.ipinfo} with {self.max_workers} workers")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(5)
        
        # create thread pool
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            while True:
                try:
                    connection, client_address = self.my_socket.accept()
                    logging.warning(f"connection from {client_address}")
                    
                    # Submit the client connection to the thread pool
                    executor.submit(ProcessTheClient, connection, client_address)
                    
                except Exception as e:
                    logging.error(f"Error accepting connection: {str(e)}")

def main():
    # server with 20 worker
    process_worker = int(input("Enter number of processes to handle clients (default 10): "))
    svr = Server(ipaddress='0.0.0.0', port=6666, max_workers=process_worker)
    svr.start()

    try:
        # join to keep server thread alive because of how client sends FIN (empty data)
        # that accidentally exits the server thread before ending the thread
        svr.join()
    except KeyboardInterrupt:
        logging.warning("Server shutting down")

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format='%(levelname)s - %(message)s')
    main()