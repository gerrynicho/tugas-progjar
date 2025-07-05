import sys
import socket
import json
import logging
import ssl
import os
from base64 import b64encode, b64decode

server_address = ('www.its.ac.id', 443)
server_address = ('www.ietf.org',443)
server_address = ('0.0.0.0', 8885) # thread pool
# server_address = ('0.0.0.0', 8889) # process pool

def make_socket(destination_address='localhost', port=12000):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (destination_address, port)
        logging.warning(f"connecting to {server_address}")
        sock.connect(server_address)
        return sock
    except Exception as ee:
        logging.warning(f"error {str(ee)}")


def make_secure_socket(destination_address='localhost', port=10000):
    try:
        # get it from https://curl.se/docs/caextract.html

        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.load_verify_locations(os.getcwd() + '/domain.crt')

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (destination_address, port)
        logging.warning(f"connecting to {server_address}")
        sock.connect(server_address)
        secure_socket = context.wrap_socket(sock, server_hostname=destination_address)
        logging.warning(secure_socket.getpeercert())
        return secure_socket
    except Exception as ee:
        logging.warning(f"error {str(ee)}")

def send_command(command_str, is_secure=False):
    alamat_server = server_address[0]
    port_server = server_address[1]
    #    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # gunakan fungsi diatas
    if is_secure == True:
        sock = make_secure_socket(alamat_server, port_server)
    else:
        sock = make_socket(alamat_server, port_server)

    logging.warning(f"connecting to {server_address}")
    try:
        logging.warning(f"sending message ")
        sock.sendall(command_str.encode())
        # logging.warning(command_str)
        # Look for the response, waiting until socket is done (no more data)
        data_received = ""  # empty string
        while True:
            # socket does not receive all data at once, data comes in part, need to be concatenated at the end of process
            data = sock.recv(2048)
            if data:
                # data is not empty, concat with previous content
                data_received += data.decode()
                if "\r\n\r\n\r\n" in data_received:
                    break
            else:
                # no more data, stop the process by break
                break
        # at this point, data_received (string) will contain all data coming from the socket
        # to be able to use the data_received as a dict, need to load it using json.loads()
        hasil = data_received
        logging.warning("data received from server:")
        return hasil
    except Exception as ee:
        logging.warning(f"error during data receiving {str(ee)}")
        return False

def parse(response):
    """Parse HTTP response and return headers and body separately"""
    try:
        if "\r\n\r\n" in response:
            headers_part, body_part = response.split("\r\n\r\n", 1)
        else:
            headers_part = response
            body_part = ""
        
        lines = headers_part.split("\r\n")
        status_line = lines[0]
        
        status_parts = status_line.split(" ", 2)
        http_version = status_parts[0] if len(status_parts) > 0 else ""
        status_code = status_parts[1] if len(status_parts) > 1 else ""
        status_message = status_parts[2] if len(status_parts) > 2 else ""
        
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                key, value = line.split(":", 1)
                headers[key.strip().lower()] = value.strip()
        
        return {
            'http_version': http_version,
            'status_code': status_code,
            'status_message': status_message,
            'headers': headers,
            'body': body_part.strip()
        }
    except Exception as e:
        logging.warning(f"Error parsing response: {e}")
        return None

def output(data_received):
    if not data_received:
        print("No data received")
        return
    # print(data_received)
    # logging.warning(f"PARSING DATA\n")
    parsed = parse(data_received)
    # print(parsed)
    if parsed['status_code'] != '200' or parsed['status_code'] != '201':
        print(f"HTTP Error {parsed['status_code']}: {parsed['status_message']}")
        return
    headers = parsed['headers']
    body = parsed['body']

    date = headers.get('date', 'Unknown Date')
    connection = headers.get('connection', 'Unknown Connection')
    server = headers.get('server', 'Unknown Server')
    content_length = headers.get('content-length', 'Unknown Content Length')
    object_address = headers.get('object-address', 'Unknown Object Address')
    content_type = headers.get('content-type', 'Unknown Content Type')
    # print(type(body))
    # print(body)
    if 'placeholder' in object_address or 'deleted' in body:
        return 
    # print(f"{body}")
    # print(f"Date: {date}")
    # print(f"Connection: {connection}")
    # print(f"Server: {server}")
    # print(f"Content-Length: {content_length}")
    # print(f"Object-Address: {object_address}")
    # print(f"Content-Type: {content_type}")
    # print(f'Content:\n{body.decode() if body else "No content"}')
    body = b64decode(body) if body else b''
    file = open(f'{object_address}', 'wb')
    file.write(body)
    file.close()


#> GET / HTTP/1.1
#> Host: www.its.ac.id
#> User-Agent: curl/8.7.1
#> Accept: */*
#>

def make_get(filename=None): # get list
    if filename is None:
        filename = ''
    cmd = f"""GET /{filename} HTTP/1.1
Host: {server_address[0]}
User-Agent: myclient/1.1
Accept: */*\r\n\r\n\r\n"""
    return cmd

def readfile(filename):
    try:
        file_path = './files/' + filename
        with open(file_path, 'rb') as file:
            content = file.read()
        file.close()
        return content
    except FileNotFoundError:
        logging.warning(f"File {filename} not found.")
        return None
    except Exception as e:
        logging.warning(f"Error reading file {filename}: {e}")
        return None

def make_post(filename): # post file
    isifile = readfile(filename)
    if isifile:
        isifile = b64encode(isifile).decode() # decode it to utf-8 string first
        cmd = f"""POST /{filename} HTTP/1.1
Host: {server_address[0]}
User-Agent: myclient/1.1
Accept: */*\r\n\r\n"""
        cmd += isifile + "\r\n\r\n\r\n"
        return cmd
    
def make_delete(filename): # delete file
    cmd = f"""DELETE /{filename} HTTP/1.1
Host: {server_address[0]}
User-Agent: myclient/1.1
Accept: */*\r\n\r\n\r\n"""
    return cmd

if __name__ == '__main__':
    cmd = make_get()
    # cmd = make_get('testing.txt')
    # cmd = make_get('donalbebek.jpg')
    # cmd = make_post('testing.txt')
    cmd = make_post('donalbebek.jpg')
    # cmd = make_post('rfc2616.pdf')
    # cmd = make_delete('testing.txt')  
    # cmd = make_delete('donalbebek.jpg')
    print(f"Command to send:\n{cmd}")
    # print(f"type of command: {type(cmd)}")
    hasil = send_command(cmd, is_secure=False)
    print(f"Data received:\n{hasil}")
    output(hasil)