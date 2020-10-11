import socket
import os


class Client:
    def __init__(self):
        self.namenode = None
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', 7777))
        self.socket.listen()

    def connect(self, ip='127.0.0.1', port=8800):
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
        data = self.__send_msg__(f"READ {remote_path}")
        if data.split(' ')[0] == 'ERROR':
            return
        datanode, addr = self.socket.accept()
        recv_file(datanode, local_path)

    def rm(self, path):
        self.__send_msg__(f'REMOVE {path}')

    def describe_file(self, path):
        self.__send_msg__(f'FILEINFO {path}')

    def cp(self, old_path, dest_path):
        self.__send_msg__(f'COPY {old_path} {dest_path}')

    def mv(self, old_path, dest_path):
        self.__send_msg__(f'MOVE {old_path} {dest_path}')


def send_file(s, filepath):
    def stat(itr, filesize):
        return int(itr * 1028 / filesize * 100)
    f = open(filepath, "rb")
    filesize = os.path.getsize(filepath)
    counter = 0
    l = f.read(1024)
    prev_progr = 0
    while (l):
        s.send(l)
        counter += 1
        progress = stat(counter, filesize)
        progress = progress if progress < 100 else 100
        if progress != prev_progr:
            prev_progr = progress
        l = f.read(1024)
    f.close()
    s.close()


def recv_file(sock, filepath):
    recv = True
    while recv:
        data = sock.recv(1024)
        if data:
            with open(filepath, 'wb') as f:
                f.write(data)
        else:
            sock.close()
            recv = False


def opt(options, input):
    tokens = input().split(' ')
    while tokens[0] != 'exit':
        option_eval(options, tokens)
        tokens = input().split(' ')


def option_eval(options, tokens):
    if tokens[0] in options:
        param = options[tokens[0]]
        if len(tokens) != param[2]:
            print(param[1])
        else:
            if param[2] == 1:
                param[0]()
            if param[2] == 2:
                param[0](tokens[1])
            if param[2] == 3:
                param[0](tokens[1], tokens[2])


if __name__ == '__main__':
    c = Client()
    c.connect()
    c.init_cluster()

    options = {
        'write': (c.upload, 'Usage: write /local_path /DFS_path', 3),
        'read': (c.download, 'Usage: read /DFS_path /local_path', 3),
        'remove': (c.rm, 'Usage: remove /DFS_path', 2),
        'info': (c.describe_file, 'Usage: info /DFS_path', 2),
        'copy': (c.cp, 'Usage: copy /DFS_path /DFS_dest_path', 3),
        'dirread': (c.lsdir, 'Usage: dirread /DFS_path', 2),
        'move': (c.mv, 'Usage: move /DFS_path /DFS_dest_path', 3),
        'dirremove': (c.rmdir, 'Usage: dirremove /DFS_path', 2),
        'dirmake': (c.mkdir, 'Usage: dirmake /DFS_path', 2),
        'diropen': (c.cd, 'Usage: diropen /DFS_path', 2),
        'init': (c.init_cluster, 'Usage: INIT', 1),
        'create': (c.touch, 'Usage: create /DFS_path', 2)
    }

    tokens = input().split(' ')
    while tokens[0] != 'exit':
        opt(options, tokens)
        tokens = input().split(' ')
