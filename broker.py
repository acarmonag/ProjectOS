import socket
from threading import Thread, Lock
import os

class Broker:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client_connected = False
        self.lock = Lock()
        self.clients = []  # Lista para almacenar clientes conectados
        self.market_consumer_socket = None
        self.cache_directory = "caches"
        if not os.path.exists(self.cache_directory):
            os.makedirs(self.cache_directory)

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(10)  # Permitir hasta 10 conexiones (máximo 9 market consumers + clientes)
        
        while True:
            client_socket, client_address = server_socket.accept()
            Thread(target=self.handle_connection, args=(client_socket,)).start()

    def handle_connection(self, socket):
        initial_message = socket.recv(1024).decode()
        
        if "MARKET_CONSUMER" in initial_message:
            market_name = initial_message.split()[1]  # Extraer el nombre del mercado
            while not self.client_connected:
                pass  # Espera hasta que un cliente esté conectado
            socket.sendall("CLIENT_CONNECTED".encode())  # Notifica al market consumer

            # Recibe datos del market consumer y los envía a todos los clientes conectados
            while True:
                data = socket.recv(1024).decode()
                if not data:
                    break
                with self.lock:  # Ensure synchronization while writing to the CSV
                    with open(os.path.join(self.cache_directory, 'all_data.csv'), 'a') as f_all:  # Adjuntar datos al CSV general
                        f_all.write(data + '\n')  # Store data with market name for clarity
                    with open(os.path.join(self.cache_directory, f'{market_name}.csv'), 'a') as f_market:  # Adjuntar datos al CSV específico del mercado
                        f_market.write(data + '\n')
                for client in self.clients:
                    client.sendall(data.encode())

        elif initial_message == "CLIENT":
            self.client_connected = True
            with self.lock:
                self.clients.append(socket)  # Añade el cliente a la lista de clientes

if __name__ == "__main__":
    broker_host = "localhost"
    broker_port = 54321

    broker = Broker(broker_host, broker_port)
    broker.start()
