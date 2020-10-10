import socket
import os


class Client:
    def __init__(self):
        self.namenode = None
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', 7777))
        self.socket.listen()

    def connect(self, ip='127.0.0.1', port="8888"):
        self.namenode = socket.socket()
        self.namenode.connect((ip, port))

    def __send_msg__(self, msg, recv_label="Status"):
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print(f'{recv_label}: {data}')
        return data

    def init_cluster(self):
        self.__send_msg__('INIT')

    def mkdir(self, path):
        self.__send_msg__(f'MAKEDIR {path}')

    def lsdir(self, path):
        self.__send_msg__(f'READDIR {path}')

    def cd(self, path):
        self.__send_msg__(f'OPENDIR {path}')

    def rmdir(self, path):
        self.__send_msg__(f'REMOVEDIR {path}')

    def touch(self, filepath):
        self.__send_msg__(f'CREATE {filepath}')

    def upload(self, local_path, remote_path):
        data = self.__send_msg__(f"WRITE {remote_path} {os.path.getsize(local_path)}")
        if data.split(' ')[0] == 'ERROR':
            return
        datanode, addr = self.socket.accept()
        send_file(datanode, local_path)
        datanode, _ = self.socket.accept()
        result = datanode.recv(100)
        print(result)

    def download(self, remote_path, local_path):
        pass

    def rm(self, path):
        self.__send_msg__(f'REMOVE {path}')

    def describe_file(self, path):
        self.__send_msg__(f'FILEINFO {path}')

    def cp(self, old_path, dest_path):
        self.__send_msg__(f'COPY {old_path} {dest_path}')

    def mv(self, old_path, dest_path):
        self.__send_msg__(f'MOVE {old_path} {dest_path}')


def send_file(s, filepath):
    f = open(filepath, "rb")
    filesize = os.path.getsize(filepath)
    stat = lambda itr, filesize: int(itr * 1028 / filesize * 100)
    counter = 0
    l = f.read(1024)
    prev_pecent = 0
    while (l):
        s.send(l)
        counter += 1
        progress = stat(counter, filesize)
        progress = progress if progress < 100 else 100
        if progress != prev_pecent:
            prev_pecent = progress
        l = f.read(1024)
    f.close()
    s.close()


if __name__ == '__main__':
    c = Client()
    c.connect()
    c.init_cluster()
