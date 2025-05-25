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
                # Return an error dictionary consistent with other methods
                return dict(status='ERROR', data='Filename cannot be empty')

            file_path_full = os.path.join(self.file_path, os.path.basename(filename)) # Sanitize filename
            
            if not os.path.exists(file_path_full) or not os.path.isfile(file_path_full):
                return dict(status='ERROR',data='File not found or is not a file')

            filesize = os.path.getsize(file_path_full)
            # Return metadata for streaming.
            # 'OK_STREAM' is a new status to indicate that raw file data will follow the JSON response.
            return dict(status='OK_STREAM', data_namafile=filename, data_filesize=filesize)
        except Exception as e:
            return dict(status='ERROR',data=str(e))

        
    def upload(self, params=[]):
        try:
            filename = params[0]
            filename = os.path.join(self.file_path, os.path.basename(filename))  # Sanitize filename
            if (filename == ''):
                return dict(status='ERROR', data='Nama file tidak boleh kosong')
            fp = open(filename, 'wb')
            fp.write(base64.b64decode(params[1]))
            return dict(status='OK', data='File berhasil diupload')
        except Exception as e:
            return dict(status='ERROR', data=str(e))
    
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
