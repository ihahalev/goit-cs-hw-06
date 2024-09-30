from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from multiprocessing import Process
from concurrent import futures as cf
import asyncio
import mimetypes
import urllib.parse
import pathlib
import socket
import logging

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb://mongodb:27017"
hppt_folder = "src"

HTTPServer_Port = 3000
TCP_IP = '127.0.0.1'
TCP_PORT = 5000

class HttpHandler(BaseHTTPRequestHandler):
    """Creating our own http handler class

    Args:
        BaseHTTPRequestHandler (BaseHTTPRequestHandler): Base HTTP Request Handler
    """
    def do_GET(self):
        """processing HTTP GET request: identifies what route or files are requested
        """
        pr_url = urllib.parse.urlparse(self.path).path
        match pr_url:
            case '/':
                self.send_html_file('index.html')
            case '/message':
                self.send_html_file('message.html')
            case _:
                if pathlib.Path().joinpath(f'./{hppt_folder}/{pr_url[1:]}').exists():
                    self.send_static()
                else:
                    self.send_html_file('error.html', 404)

    def do_POST(self):
        """processing HTTP POST request: sending data to socket
        """
        data = self.rfile.read(int(self.headers['Content-Length']))
        send_data_to_socket(data)
        self.send_response(302)
        self.send_header('Location', '/')
        self.end_headers()

    def send_html_file(self, filename:str, status=200):
        """sends requested html files

        Args:
            filename (str): requested file name
            status (int, optional): response status code. Defaults to 200.
        """
        self.send_response(status)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        with open(f'./{hppt_folder}/{filename}', 'rb') as fd:
            self.wfile.write(fd.read())

    def send_static(self):
        """sends requested static files like images and css
        """
        self.send_response(200)
        mt = mimetypes.guess_type(self.path)
        if mt:
            self.send_header("Content-type", mt[0])
        else:
            self.send_header("Content-type", 'text/plain')
        self.end_headers()
        with open(f'./{hppt_folder}{self.path}', 'rb') as file:
            self.wfile.write(file.read())

def run_http_server(server_class=HTTPServer, handler_class=HttpHandler):
    """configuring and running our http server

    Args:
        server_class (HTTPServer, optional): http server class to create server object. Defaults to HTTPServer.
        handler_class (HttpHandler, optional): http handler class to create handler object. Defaults to HttpHandler.
    """
    try:
        server_address = ('0.0.0.0', HTTPServer_Port)
        http = server_class(server_address, handler_class)
        http.serve_forever()
    except KeyboardInterrupt:
        logging.info('Shutdown server')
    except Exception as e:
        logging.error(f"Unexpected error on server run0: {e}")
    finally:
        http.server_close()

def send_data_to_socket(data:bytes):
    """sends data from http to socket server

    Args:
        data (bytes): encoded data
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server = TCP_IP, TCP_PORT
        sock.connect(server)
        print(f'Connection established {server}')
        sock.send(data)
        response = sock.recv(1024)
        print(f'Response data: {response.decode()}')
        print(f'Data transfer completed')
    except Exception as e:
        logging.error(f"Unexpected error on socket client send")

async def save_data(data:bytes):
    """saves data in db

    Args:
        data (bytes): encoded data
    """
    try:
        client = MongoClient(uri, server_api=ServerApi("1"))
        db = client.socket_db
        data_parse = urllib.parse.unquote_plus(data.decode())
        data_dict = {key: value for key, value in [el.split('=') for el in data_parse.split('&')]}
        data_dict['date'] = str(datetime.now())
        await asyncio.to_thread(db.messages.insert_one(data_dict))
    except ValueError as e:
        logging.error(f"Parsing error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while data saving: {e}")
    finally:
        if client:
            logging.info(f"Database connection closed")
            client.close()

def handle(sock: socket.socket, address: str):
    """receives data from socket connection and calls save to db function

    Args:
        sock (socket.socket): socket client connection
        address (str): socket client address
    """
    try:
        print(f'Connection established {address}')
        while True:
            received = sock.recv(1024)
            if not received:
                break
            asyncio.run(save_data(received))
            print(f'Data received: {received}')
            sock.send(received)
        print(f'Socket connection closed {address}')
    except Exception as e:
        logging.error(f"Unexpected error on socket server receive: {e}")
    finally:
        sock.close()

def run_socket_server(ip:str, port:int):
    """configuring and running our socket server, each client in its thread

    Args:
        ip (str): ip address
        port (int): port
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server = ip, port
    sock.bind(server)

    sock.listen(10)
    print(f'Start socket server {sock.getsockname()}')
    with cf.ThreadPoolExecutor(10) as client_pool:
        try:
            while True:
                new_sock, address = sock.accept()
                client_pool.submit(handle, new_sock, address)
        except KeyboardInterrupt:
            print(f'Destroy server')
        finally:
            sock.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(threadName)s %(message)s')

    http_server_process = Process(target=run_http_server)
    http_server_process.start()

    socket_server_process = Process(target=run_socket_server, args=(TCP_IP, TCP_PORT))
    socket_server_process.start()
