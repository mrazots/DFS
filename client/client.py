import socket

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

    def init_cluster(self):
        msg = b'INIT'
        self.namenode.send(msg)
        data = self.namenode.recv(100)
        print('FS size: ', data)

    def mkdir(self, path):
        msg = f'MAKEDIR {path}'
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return

    def lsdir(self, path):
        msg = f'READDIR {path}'
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return

    def cd(self, path):
        msg = f'OPENDIR {path}'
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return

    def rmdir(self, path):
        msg = f'REMOVEDIR {path}'
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print('Status: {}'.format(data))
        if data.split(' ')[0] == 'ERROR':
            if len(data.split(' ')) > 1: print(data.split(' ')[1])
            return

    def touch(self, filepath):
        pass

    def upload(self, local_path, remote_path):
        pass

    def download(self, remote_path, local_path):
        pass

    def rm(self, path):
        pass

    def get_fileinfo(self, path):
        pass

    def cp(self, curr_path, dest_path):
        pass

    def mv(self, curr_path, dest_path):
        pass

if __name__ == '__main__':
    c = Client()
    c.connect()