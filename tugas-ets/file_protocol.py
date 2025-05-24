import json
import logging
import shlex

from file_interface import FileInterface

"""
* class FileProtocol bertugas untuk memproses 
data yang masuk, dan menerjemahkannya apakah sesuai dengan
protokol/aturan yang dibuat

* data yang masuk dari client adalah dalam bentuk bytes yang 
pada akhirnya akan diproses dalam bentuk string

* class FileProtocol akan memproses data yang masuk dalam bentuk
string
"""



class FileProtocol:
    def __init__(self):
        self.file = FileInterface()
    def proses_string(self,string_datamasuk=''):
        logging.warning(f"string diproses: {string_datamasuk}")

        c = ""
        params = []
        parts = string_datamasuk.split(' ', 1)
        c_request = parts[0].strip().lower()

        if c_request != 'upload':
            c = shlex.split(string_datamasuk.lower())
            c_request = c[0].strip()
            params = [x for x in c[1:]]
        else: # upload protocol
            parts = string_datamasuk.split(' ', 2)
            c_request = parts[0].strip().lower()
            filename = parts[1].strip()
            content = parts[2].strip()
            params = [filename, content]
        try:
            logging.warning(f"memproses request: {c_request}")
            cl = getattr(self.file,c_request)(params)
            logging.warning(f"hasil request: {cl}")
            return json.dumps(cl)
        except Exception:
            return json.dumps(dict(status='ERROR',data='request tidak dikenali'))


if __name__=='__main__':
    #contoh pemakaian
    fp = FileProtocol()
    print(fp.proses_string("LIST"))
    print(fp.proses_string("GET pokijan.jpg"))
