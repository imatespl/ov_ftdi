import os
import threading
import pyminizip
from datetime import datetime

MAX_FILE_SIZE = 1024 * 1024 * 5  # 5MB
FILE_NAME_TEMPLATE = "/data/capture_current.txt"
ZIP_PASSWORD = b"PeD~L^wDB!Jf5hj"

class FileHandler:
    def __init__(self, file_name, max_file_size, rotation_file_interval):
        self.current_file = None
        self.file_name = file_name or FILE_NAME_TEMPLATE
        self.max_file_size = max_file_size * 1024 * 1024 # MB
        self.rotation_file_interval = rotation_file_interval * 60 # SECONDS
        self.open_file_time = None
        self.save_file_name = None
        self.open_new_file()

    def ensure_directory_exists(self):
        # 获取文件的上级目录路径
        directory = os.path.dirname(self.file_name)
    
        # 如果目录不存在，则创建
        if not os.path.exists(directory):
            os.makedirs(directory)
    def save_with_time(self):
        # 获取当前时间
        now_str = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        self.save_file_name = '/data/capture_' + now_str
        os.rename(self.file_name, self.save_file_name)
        threading.Thread(target=self.compress_file).start()

    def open_new_file(self):
        self.ensure_directory_exists()
        self.current_file = open(self.file_name, 'wb')
        self.open_file_time = datetime.now()

    def close_current_file(self):
        self.current_file.flush()
        os.fsync(self.current_file.fileno())
        if self.current_file and self.current_file.tell() > 0:
            self.current_file.close()
            self.save_with_time()

    def write(self, data):
        self.current_file.write(data)
        now = datetime.now()
        interval = (now - self.open_file_time).seconds
        if (self.current_file.tell() >= self.max_file_size or interval >= self.rotation_file_interval):
            self.handle_file_rotation()

    def handle_file_rotation(self):
        # 关闭当前文件并压缩文件（密码保护和无压缩）
        self.close_current_file()
        self.open_new_file()

    def compress_file(self):
        zip_name = f"{self.save_file_name}.zip"
        pyminizip.compress(self.save_file_name, None, zip_name, ZIP_PASSWORD, 0)
        os.remove(self.save_file_name)  # 删除原始文件

    def close(self):
        self.close_current_file()
