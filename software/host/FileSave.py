import os
import threading
import pyminizip
from datetime import datetime

MAX_FILE_SIZE = 1024 * 1024 * 5  # 5MB
FILE_NAME_TEMPLATE = "/data/capture.txt"
SEQUENCE_FILE = "/data/sequence.txt"
ZIP_PASSWORD = b"PeD~L^wDB!Jf5hj"

class FileHandler:
    def __init__(self, file_name, max_file_size, rotation_file_interval):
        self.current_file_index = self.load_sequence()
        self.current_file = None
        self.file_name = file_name or FILE_NAME_TEMPLATE
        self.max_file_size = max_file_size * 1024 * 1024 # MB
        self.rotation_file_interval = rotation_file_interval * 60 # SECONDS
        self.open_file_time = None
        self.open_new_file()

    def ensure_directory_exists(self):
        # 获取文件的上级目录路径
        directory = os.path.dirname(self.file_name)
    
        # 如果目录不存在，则创建
        if not os.path.exists(directory):
            os.makedirs(directory)

    def load_sequence(self):
        # 加载或初始化文件序列编号
        if os.path.exists(SEQUENCE_FILE):
            with open(SEQUENCE_FILE, 'r') as f:
                return int(f.read().strip())
        else:
            return 0

    def save_sequence(self):
        #保存的当前index+1
        self.current_file_index += 1
        # 保存当前文件序列编号
        with open(SEQUENCE_FILE, 'w') as f:
            f.write(str(self.current_file_index))

    def open_new_file(self):
        self.ensure_directory_exists()
        self.current_file = open(self.file_name + '_' + str(self.current_file_index), 'wb')
        self.save_sequence()
        self.open_file_time = datetime.now()

    def close_current_file(self):
        if self.current_file and self.current_file.tell() > 0:
            self.current_file.close()

    def write(self, data):
        self.current_file.write(data)
        now = datetime.now()
        interval = (now - self.open_file_time).seconds
        if (self.current_file.tell() >= self.max_file_size or interval >= self.rotation_file_interval):
            self.handle_file_rotation()

    def handle_file_rotation(self):
        # 关闭当前文件并压缩文件（密码保护和无压缩）
        self.close_current_file()
        threading.Thread(target=self.compress_file, args=(self.current_file_index,)).start()
        self.open_new_file()

    def compress_file(self, file_index):
        file_name = self.file_name + '_' + str(self.current_file_index)
        zip_name = f"{file_name}.zip"
        pyminizip.compress(file_name, None, zip_name, ZIP_PASSWORD, 0)
        os.remove(file_name)  # 删除原始文件

    def close(self):
        self.close_current_file()
        threading.Thread(target=self.compress_file, args=(self.current_file_index,)).start()
        self.save_sequence()
