import os
import json
import base64
from glob import glob


class FileInterface:
    def __init__(self):
        os.chdir('files/')

    def list(self,params=[]):
        try:
            filelist = glob('*.*')
            return dict(status='OK',data=filelist)
        except Exception as e:
            return dict(status='ERROR',data=str(e))

    def get(self,params=[]):
        try:
            filename = params[0]
            if (filename == ''):
                return None
            fp = open(f"{filename}",'rb')
            isifile = base64.b64encode(fp.read()).decode()
            return dict(status='OK',data_namafile=filename,data_file=isifile)
        except Exception as e:
            return dict(status='ERROR',data=str(e))
        
    def upload(self, params=[]):
        try:
            filelist = glob('*.*')
            before_sum = len(filelist)    
        
            filename = params[0]

            if (filename == ''):
                return dict(status='ERROR',data='nama file tidak boleh kosong')
            elif (filename in filelist):
                return dict(status='ERROR',data='file sudah ada di server/sudah ada file dengan nama yang sama')
            elif len(params) < 2:
                return dict(status='ERROR',data='parameter tidak lengkap')
            elif len(params) > 2:
                return dict(status='ERROR',data='unexpected parameter')
            elif not params[1]:
                return dict(status='ERROR',data='file tidak ada isinya')


            with open(filename, 'wb') as fp:
                fp.write(base64.b64decode(params[1]))
                fp.close()
            
            filelist = glob('*.*')
            after_sum = len(filelist)

            return dict(status='OK',data_namafile=filename, sum=(before_sum, after_sum))
        except Exception as e:
            return dict(status='ERROR',data=str(e))
    
    def delete(self, params=[]):
        try:
            filelist = glob('*.*')
            before_sum = len(filelist)

            filename = params[0]
            if (filename == ''):
                return None
            if not os.path.exists(filename):
                return dict(status='ERROR',data='file tidak ditemukan')
            elif before_sum == 0:
                return dict(status='ERROR',data='tidak ada file di server')
            os.remove(filename)

            filelist = glob('*.*')
            after_sum = len(filelist)

            return dict(status='OK',data_namafile=filename, sum=(before_sum, after_sum))
        except Exception as e:
            return dict(status='ERROR',data=str(e))



if __name__=='__main__':
    f = FileInterface()
    print(f.list())
    print(f.get(['pokijan.jpg']))
