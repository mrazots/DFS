import socket
from threading import Thread
import tqdm
import os
from time import sleep
import sys
import psycopg2

HOST = "localhost"
PORT = 8080
BUFFER_SIZE = 1024
datanodes_number = 2
sockets = {}
conn = {}
datanodes = []
current_dir = "/"

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((HOST, PORT))
sock.listen(5)
print('Listening..')
while True:
        connection, addr = sock.accept()
        dn_port = connection.recvfrom(1024)
        print("Node {}: connected". format(addr))
        key = addr[0] + ":" + dn_port[0].decode()
        sockets[key] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sockets[key].connect((addr[0], int(dn_port[0].decode())))
        conn[key] = connection
        datanodes.append(key)


def initialize():
    pass

def check_nodes():
    pass

def send_message(socket, message):
    pass


def is_exists(filename):
    pass

def get_file_path(filename):
    pass

def mkdir(path, dirname):
    pass

def create_file(filename):
    pass

def read(filename):
    pass

def write(filename, str):
    pass

def delete_dir(dirname):
    pass

def delete_file(filename):
    pass

def ls():
    pass

def cd(path):
    pass

def cp(src, dest):
    pass

def mv(src, dest):
    pass

def make_query(query, is_return):
    conn = psycopg2.connect(dbname='postgres', user='postgres', password='postgres', host='localhost', port="5432")
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    if is_return:
        result = cursor.fetchall()
    cursor.close()
    conn.close()
    if is_return:
        return result


def backup(addr):
    pass

def close():
    for i in datanodes:
        sockets[i].close()
        conn[i].close()
    sock.close()
    sock.detach()


if __name__ == "__main__":
    make_query("CREATE TABLE IF NOT EXISTS filesdb (filename Text, path TEXT, datanode1 TEXT, datanode2 TEXT, is_dir BOOLEAN, size TEXT);", False)
    while True:
        try:
            print(current_dir + ">", end=" ")
            inpt = input()
            commands = inpt.split(" ")
            if commands[0] == "init":
                initialize()
            elif commands[0] == "cd":
                cd(commands[1])
            elif commands[0] == "ls":
                ls()
            elif commands[0] == "mkdir":
                mkdir(commands[1], commands[2])
                ls()
            elif commands[0] == 'read':
                read(commands[1])
            elif commands[0] == "delete_dir":
                delete_dir(commands[1])
                ls()
            elif commands[0] == "close":
                close()
                sys.exit(0)
            elif commands[0] == "delete":
                delete_file(commands[1])
            elif commands[0] == 'create_file':
                create_file(commands[1])
            elif commands[0] == 'mv':
                mv(commands[1], commands[2])
            elif commands[0] == 'cp':
                cp(commands[1], commands[2])
            elif commands[0] == 'write':
                write(commands[1], commands[2])
            else:
                print(commands[0] + ": Command not found")
        except SystemExit:
            print("Stop")
            sys.exit(0)
        except KeyboardInterrupt:
            print("Stop")
            sys.exit(0)
        except:
            print("Something went wrong")
            continue


