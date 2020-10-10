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

    def __send_msg__(self, msg, recv_label="Status"):
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print(f'{recv_label}: {data}')

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
