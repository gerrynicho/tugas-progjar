import os
import json
import base64
from glob import glob


class FileInterface:
    def __init__(self):
        self.file_path = 'files/'
        if not os.path.exists(self.file_path):
            os.makedirs(self.file_path)
        # os.chdir('files/')

    def list(self,params=[]):
        try:
            filelist = glob(os.path.join(self.file_path, '*.*'))
            filelist = [os.path.basename(f) for f in filelist]  # Get only filenames
            return dict(status='OK',data=filelist)
        except Exception as e:
            return dict(status='ERROR',data=str(e))

    def get(self,params=[]):
        try:
            filename = params[0]
            if (filename == ''):
                return None
            fp = open(os.path.join(self.file_path, filename),'rb')
            isifile = base64.b64encode(fp.read()).decode()
            return dict(status='OK',data_namafile=filename,data_file=isifile)
        except Exception as e:
            return dict(status='ERROR',data=str(e))
        
    def upload(self, params=[]):
        try:
            filelist = glob(os.path.join(self.file_path, '*.*'))
            filelist = [os.path.basename(f) for f in filelist]  # Get only filenames
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


            with open(os.path.join(self.file_path, filename), 'wb') as fp:
                fp.write(base64.b64decode(params[1]))
                fp.close()
            
            filelist = glob(os.path.join(self.file_path, '*.*'))
            after_sum = len(filelist)

            return dict(status='OK',data_namafile=filename, sum=(before_sum, after_sum))
        except Exception as e:
            return dict(status='ERROR',data=str(e))
    
    def delete(self, params=[]):
        try:
            filelist = glob(os.path.join(self.file_path, '*.*'))
            before_sum = len(filelist)

            filename = params[0]
            if (filename == ''):
                return None
            if not os.path.exists(os.path.join(self.file_path, filename)):
                return dict(status='ERROR',data='file tidak ditemukan')
            elif before_sum == 0:
                return dict(status='ERROR',data='tidak ada file di server')
            os.remove(os.path.join(self.file_path, filename))

            filelist = glob(os.path.join(self.file_path, '*.*'))
            after_sum = len(filelist)

            return dict(status='OK',data_namafile=filename, sum=(before_sum, after_sum))
        except Exception as e:
            return dict(status='ERROR',data=str(e))



if __name__=='__main__':
    f = FileInterface()
    print(f.list())
    print(f.get(['pokijan.jpg']))
