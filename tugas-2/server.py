from socket import *
import socket
import threading
import logging
import time
from datetime import datetime
import sys

class ProcessTheClient(threading.Thread):
	def __init__(self,connection,address):
		self.connection = connection
		self.address = address
		threading.Thread.__init__(self)

	def valid_data(self, data):
		if data == b'':
			logging.warning(f"data is empty")
			return False
		if not data.startswith(b'TIME'):
			logging.warning(f"data does not start with TIME")
			return False
		if not data.endswith(b'\r\n'):
			logging.warning(f"data does not end with \\r\\n")
			return False
		return True

	def prepare_msg(self):
		now = datetime.now()
		time_str = now.strftime("%H:%M:%S")
		return f'JAM {time_str}\r\n'

	def run(self):
		while True:
			data = self.connection.recv(32)
			if self.valid_data(data):
				logging.warning(f"received {data}")
				msg = self.prepare_msg()
				logging.warning(f"sending TIME : {msg}")
				self.connection.sendall(msg.encode())
			elif data == b'QUIT\r\n':
				logging.warning(f"received QUIT")
				break
		
		self.connection.close()

class Server(threading.Thread):
	def __init__(self):
		self.the_clients = []
		self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		threading.Thread.__init__(self)

	def run(self):
		self.my_socket.bind(('0.0.0.0',45000))
		self.my_socket.listen(1)
		logging.warning(f"starting up on {self.my_socket.getsockname()}\n")
		while True:
			self.connection, self.client_address = self.my_socket.accept()
			logging.warning(f"connection from {self.client_address}")
			
			clt = ProcessTheClient(self.connection, self.client_address)
			clt.start()
			self.the_clients.append(clt)
	

def main():
	svr = Server()
	svr.start()

if __name__=="__main__":
	logging.basicConfig(level=logging.WARNING, format='%(message)s')
	main()