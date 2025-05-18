import sys
import socket
import logging

#set basic logging
logging.basicConfig(level=logging.INFO)

try:
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect the socket to the port where the server is listening
    server_address = ('localhost', 10000)
    logging.info(f"connecting to {server_address}")
    sock.connect(server_address)

    # Create txt file if not exist
    try:
        with open('tugas-1/3-client.txt', 'x') as f:
            logging.info("Creating file tugas-1/3-client.txt")
            f.write('INI ADALAH FILE YANG DIKIRIM QWERTYUIOPASDFGH\n')
            f.write('SECOND LINE YANG DIKIRIM')
    except FileExistsError:
        logging.info("Using existing file tugas-1/3-client.txt")

    # Read the file
    # Send data
    # message = 'INI ADALAH DATA YANG DIKIRIM ABCDEFGHIJKLMNOPQ'
    # logging.info(f"sending {message}")
    message = ''
    with open('tugas-1/3-client.txt', 'r') as f:
        logging.info("Reading file tugas-1/3-client.txt")
        message = f.read()
        logging.info(f"message: getting message from file {message}")
    sock.sendall(message.encode())

    # Look for the response
    amount_received = 0
    amount_expected = len(message)
    while amount_received < amount_expected:
        data = sock.recv(16)
        amount_received += len(data)
        logging.info(f"{data}")
        
except Exception as ee:
    logging.info(f"ERROR: {str(ee)}")
    exit(0)
finally:
    logging.info("closing")
    sock.close()