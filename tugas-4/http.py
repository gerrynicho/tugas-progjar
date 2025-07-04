import sys
import os.path
import uuid
from base64 import b64encode, b64decode
from glob import glob
from datetime import datetime

class HttpServer:
	def __init__(self):
		self.sessions={}
		self.types={}
		self.types['.pdf']='application/pdf'
		self.types['.jpg']='image/jpeg'
		self.types['.txt']='text/plain'
		self.types['.html']='text/html'
	def response(self,kode=404,message='Not Found',messagebody=bytes(),headers={}, objectaddress='placeholder'):
		tanggal = datetime.now().strftime('%c')
		resp=[]
		resp.append("HTTP/1.0 {} {}\r\n" . format(kode,message))
		resp.append("Date: {}\r\n" . format(tanggal))
		resp.append("Connection: close\r\n")
		resp.append("Server: myserver/1.0\r\n")
		resp.append("Content-Length: {}\r\n" . format(len(messagebody)))
		resp.append("Object-Address: {}\r\n" . format(objectaddress))
		for kk in headers:
			resp.append("{}:{}\r\n" . format(kk,headers[kk]))
		resp.append("\r\n")
		print(f"SEHARUSNYA PRINT INI {objectaddress}\n\n")
		response_headers=''
		for i in resp:
			response_headers="{}{}" . format(response_headers,i)
		#menggabungkan resp menjadi satu string dan menggabungkan dengan messagebody yang berupa bytes
		#response harus berupa bytes
		#message body harus diubah dulu menjadi bytes
		if (type(messagebody) is not bytes):
			messagebody = messagebody.encode()

		response = response_headers.encode() + messagebody
		#response adalah bytes
		return response

	def proses(self,data):
		
		requests = data.split("\r\n")
		print(requests)

		baris = requests[0]
		print(baris)

		all_headers = [n for n in requests[1:] if n!='']

		j = baris.split(" ")
		try:
			method=j[0].upper().strip()
			if (method=='GET'):
				object_address = j[1].strip()
				return self.http_get(object_address, all_headers)
			if (method=='POST'):
				object_address = j[1].strip()
				object_address = object_address[1:] if object_address.startswith('/') else object_address
				body_start = data.find("\r\n\r\n") + 4
				if body_start == -1:
					body = ''
				else:
					body = data[body_start:].strip()
				print(f"body: {body}")
				return self.http_post(object_address, all_headers, body)
			if (method=='DELETE'):
				object_address = j[1].strip()
				return self.http_delete(object_address, all_headers)
			else:
				return self.response(400,'Bad Request','',{}, object_address)
		except IndexError:
			return self.response(400,'Bad Request','',{})
	def http_get(self,object_address,headers):
		#print(files)
		thedir='./uploads/'
		if (object_address == '/'):
			_, _, number_of_files = next(os.walk(thedir))
			number_of_files = len(number_of_files)
			return self.response(200,'OK',f'Jumlah file di dalam server adalah {number_of_files}',dict())

		if (object_address == '/video'):
			return self.response(302,'Found','',dict(location='https://youtu.be/katoxpnTf04'))
		if (object_address == '/santai'):
			return self.response(200,'OK','santai saja',dict())


		object_address=object_address[1:]
		if thedir+object_address not in glob(thedir+'*'):
			print("tidak ada file {}".format(thedir+object_address))
			return self.response(404,'Not Found','',{})
		fp = open(thedir+object_address,'rb') #rb => artinya adalah read dalam bentuk binary
		#harus membaca dalam bentuk byte dan BINARY
		isi = fp.read()
		isi = b64encode(isi)
		fp.close()
		fext = os.path.splitext(thedir+object_address)[1]
		content_type = self.types[fext]
		
		headers={}
		headers['Content-type']=content_type
		return self.response(200,'OK',isi,headers, objectaddress=object_address)
	
	def http_post(self,object_address,headers, body):
		if body is None or body == '':
			return self.response(400, 'Bad Request', 'No Data', {}, objectaddress=object_address)
		try:
			upload_dir = './uploads/'
			if not os.path.exists(upload_dir):
				os.makedirs(upload_dir)
			filename = object_address[1:] if object_address.startswith('/') else object_address

			if not filename:
				return self.response(400, 'Bad Request', 'Filename is required', {}, objectaddress=object_address)
			if not body:
				return self.response(400, 'Bad Request', 'No Data', {}, objectaddress=object_address)
			
			file_path = os.path.join(upload_dir, filename)
			body = b64decode(body)
			print(f"Type of body: {type(body)}\n body: {body[:50]}...")  # Print first 50 bytes for debugging
			with open(file_path, 'wb') as f:
				f.write(body)
			print(f"File uploaded successfully: {file_path}")
			isi = f"File {filename} uploaded successfully.".encode()
			return self.response(201, 'Created', isi, {}, objectaddress=object_address)

		except Exception as e:
			print(f"Error uploading file: {e}")
			return self.response(500, 'Internal Server Error', str(e).encode(), {}, objectaddress=object_address)
		return self.response(200,'OK',isi,headers, objectaddress=object_address)
	
	def http_delete(self,object_address,headers):
		thedir = './uploads/'
		try:
			if not object_address.startswith('/'):
				object_address = '/' + object_address
			file_path = os.path.join(thedir, object_address[1:])
			if not os.path.exists(file_path):
				return self.response(404, 'Not Found', 'File does not exist', {}, objectaddress=object_address)
			os.remove(file_path)
			isi = f"File {object_address} deleted successfully.".encode()
			return self.response(200, 'OK', isi, headers, objectaddress=object_address)
		except Exception as e:
			print(f"Error deleting file: {e}")
			return self.response(500, 'Internal Server Error', str(e).encode(), {}, objectaddress=object_address)

			 	
#>>> import os.path
#>>> ext = os.path.splitext('/ak/52.png')

if __name__=="__main__":
	httpserver = HttpServer()
	d = httpserver.proses('GET /testing.txt HTTP/1.0')
	print(d)
	d = httpserver.proses('GET /donalbebek.jpg HTTP/1.0')
	print(d)
	#d = httpserver.http_get('testing2.txt',{})
	#print(d)
#	d = httpserver.http_get('testing.txt')
#	print(d)















