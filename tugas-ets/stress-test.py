import socket
import json
import base64
import logging
import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from statistics import mean, median

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stress_test.log"),
        logging.StreamHandler()
    ]
)

class Client:
    def __init__(self, server_address=('localhost', 6667)):
        self.server_address = server_address
        self.results = {
            'upload' : [],
            'get' : [],
            'list' : []
        }
        self.success_count = {
            'upload' : 0,
            'get' : 0,
            'list' : 0
        }
        self.fail_count = {
            'upload' : 0,
            'get' : 0,
            'list' : 0
        }

        self.server_config = {
            'executor_type': 'thread',  # default executor type
            'worker_pool_size': 20,  # default worker pool size
        }

        if not os.path.exists('results'):
            os.makedirs('results')
        
        if not os.path.exists('downloads'):
            os.makedirs('downloads')
    

    def set_server_config(self):
        self.server_config['executor_type'] = input("Enter Server's executor type (thread/process): ").strip().lower()
        self.server_config['worker_pool_size'] = int(input("Enter Server's worker pool size: ").strip())


    def send_command(self, command_str):
        # base command to be sent to server, not actual interface to send command
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(600) # 10 minutes timeout
        
        try:
            start_connect = time.time()
            sock.connect(self.server_address)
            connect_time = time.time() - start_connect
            logging.debug(f"Connection established in {connect_time:.2f}s")

            chunks = [command_str[i:i+65536] for i in range(0, len(command_str), 65536)]
            for chunk in chunks:
                sock.sendall(chunk.encode())

            sock.sendall('\r\n\r\n'.encode())

            data_received = ""
            while True:
                try:
                    data = sock.recv(1024*1024) # 1MB buffer
                    if data:
                        data_received += data.decode()
                        if "\r\n\r\n" in data_received:
                            break
                    else:
                        break
                except socket.timeout:
                    logging.error("Socket timeout while receiving data")
                    return {'status': 'ERROR', 'data': 'Socket timeout'}
                
            json_response = data_received.split("\r\n\r\n")[0]
            # split the response into header and body
            hasil = json.loads(json_response)
            return hasil
        except socket.timeout as e:
            logging.error(f"Socket timeout: {str(e)}")
            return {'status': 'ERROR', 'data': f'Socket timeout {str(e)}'}
        except ConnectionRefusedError as e:
            logging.error(f"Connection refused: {str(e)}")
            return {'status': 'ERROR', 'data': f'Connection refused {str(e)}'}
        except Exception as e:
            logging.error(f"Error during data receiving: {str(e)}")
            return {'status': 'ERROR', 'data': str(e)}
        finally:
            sock.close()
            logging.debug("Socket closed")
    


    def record_list(self, worker_id):
        start_time = time.time()

        try:
            command_str = "LIST"
            # dont send \r\n\r\n, because send_command will handle technicals
            result = self.send_command(command_str)

            end_time = time.time()
            duration = end_time - start_time

            if result['status'] == 'OK':
                file_count = len(result['data'])
                logging.info(f"Worker {worker_id} - LIST command successful, {file_count} files found")
                self.success_count['list'] += 1
            else:
                logging.error(f"Worker {worker_id} - LIST command failed: {result['data']}")
                self.fail_count['list'] += 1
            return {
                'worker_id': worker_id,
                'operation': 'LIST',
                'duration' : duration,
                'status' : result['status']
            }
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logging.error(f"Worker {worker_id} - LIST command error: {str(e)}")
            self.fail_count['list'] += 1
            
            return {
                'worker_id': worker_id,
                'operation': 'LIST',
                'duration' : duration,
                'status' : 'ERROR',
                'message' : str(e)
            }
    


    def record_get(self, filename, worker_id):
        start_time = time.time()

        try:
            logging.info(f"Worker {worker_id} - GET command for {filename}")
            command_str = f"GET {filename}"
            result = self.send_command(command_str)
            
            if result['status'] == 'OK':
                file_content = base64.b64decode(result['data_file'])
                file_size = len(file_content)

                # put worker_id to avoid race condition between workers
                download_path = os.path.join('downloads', f"worker_{worker_id}_{filename}")

                with open(download_path, 'wb') as f:
                    f.write(file_content)
                
                end_time = time.time()
                duration = end_time - start_time
                throughput = file_size / duration if duration > 0 else 0

                logging.info(f"Worker {worker_id} - GET command successful, downloaded {filename} ({file_size/1024/1024:.2f} MB) in {duration:.2f}s - {throughput/1024/1024:.2f} MB/s")
                self.success_count['get'] += 1

                return {
                    'worker_id': worker_id,
                    'operation': 'GET',
                    'filename': filename,
                    'duration' : duration,
                    'throughput' : throughput,
                    'status' : 'OK'
                }
            else:
                end_time = time.time()
                duration = end_time - start_time
                logging.error(f"Worker {worker_id} - GET command failed: {result['data']}")
                self.fail_count['get'] += 1

                return {
                    'worker_id': worker_id,
                    'operation': 'GET',
                    'filesize' : 0,
                    'duration' : duration,
                    'status' : 'ERROR',
                    'message' : result['data']
                }
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logging.error(f"Worker {worker_id} - GET command error: {str(e)}")
            self.fail_count['get'] += 1
            
            return {
                'worker_id': worker_id,
                'operation': 'GET',
                'filesize' : 0,
                'duration' : duration,
                'status' : 'ERROR',
                'message' : str(e)
            }
        


    def record_upload(self, file_path, worker_id):
        start_time = time.time()
        filename = os.path.basename(file_path)
        filename = f'worker_{worker_id}_{filename}' # change filename to avoid conflict between workers
        file_size = os.path.getsize(file_path)

        try:
            logging.info(f"Worker {worker_id} - upload command for {filename} ({file_size/1024/1024:.2f} MB)")

            with open(file_path, 'rb') as f:
                file_content = f.read()
                encoded_file_content = base64.b64encode(file_content).decode()
            
            command_str = f"UPLOAD {filename} {encoded_file_content}"

            result = self.send_command(command_str)

            end_time = time.time()
            duration = end_time - start_time
            throughput = file_size / duration if duration > 0 else 0

            if result['status'] == 'OK':
                logging.info(f"Worker {worker_id} - upload command successful, uploaded {filename} ({file_size/1024/1024:.2f} MB) in {duration:.2f}s - {throughput/1024/1024:.2f} MB/s")
                self.success_count['upload'] += 1
            else: 
                logging.error(f"Worker {worker_id} - upload command failed: {result['data']}")
                self.fail_count['upload'] += 1
            
            return {
                'worker_id': worker_id,
                'operation': 'UPLOAD',
                'filename': filename,
                'filesize' : file_size,
                'duration' : duration,
                'throughput' : throughput,
                'status' : result['status']
            }
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logging.error(f"Worker {worker_id} - upload command error: {str(e)}")
            self.fail_count['upload'] += 1
            
            return {
                'worker_id': worker_id,
                'operation': 'UPLOAD',
                'filesize' : file_size,
                'duration' : duration,
                'status' : 'ERROR',
                'message' : str(e)
            }
    



    def reset_counters(self):
        self.success_count = {
            'upload' : 0,
            'get' : 0,
            'list' : 0
        }
        self.fail_count = {
            'upload' : 0,
            'get' : 0,
            'list' : 0
        }
        self.results = {
            'upload' : [],
            'get' : [],
            'list' : []
        }



    # we can get sloppy with record_X methods' return values
    # because we are post-processing/filtering the results after the method call
    # as long as the necessary data is saved in the return value
    # the return value can include other data that is not needed
    def run_test(self, operation, file_size_mb, client_pool_size, executor_type='thread'):
        if operation not in ['upload', 'get', 'list']:
            logging.error(f"Invalid operation: {operation}")
            return   

        logging.info(f"Starting {operation} test with file size {file_size_mb}MB, client pool size {client_pool_size}, executor type {executor_type}")

        # upload original file to files
        if operation == 'get':
            filename = f"test_file_{file_size_mb}mb.bin"
            os.chdir('files/')
            if not os.path.exists(filename):
                logging.warning(f"file {filename} not found, uploading to file first")
                upload_info = self.record_upload(filename, 0) # 0 as in main thread and not actual worker
                if upload_info['status'] != 'OK':
                    logging.error(f"Failed to upload file {filename} for GET test")
                    return None
            os.chdir('../')
                
        
        if executor_type == 'thread':
            executor_class = ThreadPoolExecutor
        elif executor_type == 'process':
            executor_class = ProcessPoolExecutor
        
        all_results = []

        with executor_class(max_workers=client_pool_size) as executor:
            futures = []

            for i in range(client_pool_size):
                if operation == 'upload':
                    futures.append(executor.submit(self.record_upload, f"test_file_{file_size_mb}mb.bin", i))
                elif operation == 'get':
                    futures.append(executor.submit(self.record_get, f"test_file_{file_size_mb}mb.bin", i))
                elif operation == 'list':
                    futures.append(executor.submit(self.record_list, i))
                else: # wtf
                    logging.error(f"Invalid operation: {operation}")
                
            for future in as_completed(futures):
                try:
                    result = future.result()
                    all_results.append(result)
                    self.results[operation].append(result)

                except Exception as e:
                    logging.error(f"Error in future: {str(e)}")

        durations = [r['duration'] for r in all_results if r['status'] == 'OK']
        throughputs = [r['throughput'] for r in all_results if r.get('throughput', 0) > 0]
        successful_count = sum(1 for r in all_results if r['status'] == 'OK')
        fail_count = sum(1 for r in all_results if r['status'] != 'OK')

        if not durations:
            logging.warning("No successful operations to calculate statistics")
            return {
                'operation': operation,
                'file_size_mb' : file_size_mb,
                'client_pool_size' : client_pool_size,
                'executor_type' : executor_type,
                'success_count' : successful_count,
                'fail_count' : fail_count,
            }

        stats = {
            'operation': operation,
            'file_size_mb': file_size_mb,
            'client_pool_size': client_pool_size,
            'executor_type': executor_type,
            'avg_duration': mean(durations) if durations else 0,
            'median_duration': median(durations) if durations else 0,
            'min_duration': min(durations) if durations else 0,
            'max_duration': max(durations) if durations else 0,
            'avg_throughput': mean(throughputs) if throughputs else 0,
            'median_throughput': median(throughputs) if throughputs else 0,
            'min_throughput': min(throughputs) if throughputs else 0,
            'max_throughput': max(throughputs) if throughputs else 0,
            'success_count': successful_count,
            'fail_count': fail_count,
        }

        logging.info(f"Test complete: {stats['success_count']} succeeded, {stats['fail_count']} failed")
        logging.info(f"Average duration: {stats['avg_duration']:.2f}s, Average throughput: {stats['avg_throughput']/1024/1024:.2f} MB/s")

        return stats



    def save_results_to_csv(self, all_stats):
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        csv_filename = f"results/stress_test_results_{timestamp}.csv"
        
        with open(csv_filename, 'w', newline='') as csvfile:
            fieldnames = [
                'Operasi', 'Volume File (MB)', 'Jumlah client worker pool', 
                'Server executor type', 'Jumlah server worker pool', 
                'Waktu total per client (s)', 'Throughput per client (MB/s)',
                'Jumlah worker client sukses', 'Jumlah worker client gagal',
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for stats in all_stats:
                operation = stats['operation']
                
                row = {
                    'Operasi': operation.upper(),
                    'Volume File (MB)': stats['file_size_mb'],
                    'Jumlah client worker pool': stats['client_pool_size'],
                    'Server executor type': self.server_config['executor_type'],
                    'Jumlah server worker pool': self.server_config['worker_pool_size'],
                    'Waktu total per client (s)': f"{stats['avg_duration']:.2f}",
                    'Throughput per client (MB/s)': f"{stats['avg_throughput']/1024/1024:.2f}" if stats.get('avg_throughput') else "N/A",
                    'Jumlah worker client sukses': stats['success_count'],
                    'Jumlah worker client gagal': stats['fail_count'],
                }
                writer.writerow(row)
        
        logging.info(f"Results saved to {csv_filename}")
        return csv_filename

    def cleanup(self):
        # Cleanup downloaded files
        for filename in os.listdir('downloads'):
            file_path = os.path.join('downloads', filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                logging.info(f"Removed file: {file_path}")
        
        for filename in os.listdir('files'):
            # remove test files only
            file_path = os.path.join('files', filename)
            if filename.startswith('worker_') and filename.endswith('.bin'):
                # remove test files only
                file_path = os.path.join('files', filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    logging.info(f"Removed file: {file_path}")

    def perform_stress_test(self, operations, file_sizes, client_pool_sizes, executor_types=['thread']):
        all_stats = []
        for operation in operations:
            for file_size in file_sizes:
                for client_pool_size in client_pool_sizes:
                    for executor_type in executor_types:
                        self.reset_counters()
                        stats = self.run_test(operation, file_size, client_pool_size, executor_type)
                        if stats:
                            all_stats.append(stats)
                        self.cleanup()
        
        csv_file = self.save_results_to_csv(all_stats)
        self.cleanup()
        return csv_file
    
    def automate_stress_test(self):
        # operations = ['list', 'get', 'upload']
        operations = ['list']
        file_sizes = [10, 100, 500]
        client_pool_sizes = [1, 5, 10]
        executor_types = ['thread', 'process']
        csv_file = self.perform_stress_test(operations, file_sizes, client_pool_sizes, executor_types)
        logging.info(f"Stress test completed. Results saved to {csv_file}")
        return csv_file



if __name__ == "__main__":
    client = Client(server_address=('localhost', 6666))
    # res = client.run_test('upload', 10, 10, executor_type='thread')
    client.set_server_config()
    res = client.automate_stress_test()
    # res = json.dumps(res, indent=4)
    # print(res)