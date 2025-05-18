import sys
import socket
import logging

#set basic logging
logging.basicConfig(level=logging.INFO)

try:
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect the socket to the port where the server is listening
    server_address = ('localhost', 45000)
    logging.info(f"connecting to {server_address}")
    sock.connect(server_address)

    # Send data
    message = b'TIME\r\n'
    logging.info(f"sending {message}")
    sock.sendall(message)
    # Look for the response
    data = sock.recv(16)
    logging.info(f"received {data}")
    
    logging.info(f"sending {message}")
    sock.sendall(message)
    # Look for the response
    data = sock.recv(16)
    logging.info(f"received {data}")


    sock.sendall(b'QUIT\r\n')
    data = sock.recv(16)
    if data != b'':
        logging.info(f"received {data}")
        
except Exception as ee:
    logging.info(f"ERROR: {str(ee)}")
    exit(0)
finally:
    logging.info("closing")
    sock.close()