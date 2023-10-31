import socket
from threading import Thread, Lock
import os
from datetime import *
import sys
import glob
import subprocess
import signal

class Broker:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client_connected = False
        self.lock = Lock()
        self.clients = []  # Lista para almacenar clientes conectados
        self.all_data = []
        self.market_consumer_socket = None
        self.cache_directory = "caches"
        self.running = True 
        if not os.path.exists(self.cache_directory):
            os.makedirs(self.cache_directory)

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(10)  # Permitir hasta 10 conexiones
        
        def signal_handler(sig, frame):
            print('Deteniendo el servidor...')
            self.running = False  # Cambiar el estado para que el bucle principal se detenga
            server_socket.close()  # Cerrar el socket del servidor
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)  # Configurar el manejador de señales para Ctrl + C
        
        print("Servidor iniciado. Presiona Ctrl + C para detener.")
        while self.running:
            client_socket, client_address = server_socket.accept()
            Thread(target=self.handle_connection, args=(client_socket,)).start()

    def handle_connection(self, socket):
        initial_message = socket.recv(1024).decode()
        
        if "MARKET_CONSUMER" in initial_message:
            market_name = initial_message.split(",")[0]  # Extraer el nombre del mercado
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
                        self.all_data.append(data)  # Añadir datos a la lista en memoria
                        self.all_data.sort(key=lambda x: datetime.strptime(x.split(',')[1], '%Y-%m-%d %H:%M'))  # Ordenar datos por fecha
                    with open(os.path.join(self.cache_directory, 'date_sorted.csv'), 'w') as f_sorted:  # Abrir archivo CSV para escribir datos ordenados
                        for line in self.all_data:
                            f_sorted.write(line + '\n')  # Escribir datos en el archivo
                for client in self.clients:
                    client.sendall(data.encode())

        elif initial_message == "CLIENT":
            self.client_connected = True
            with self.lock:
                self.clients.append(socket)  # Añade el cliente a la lista de clientes

    def start_market_consumers(self, periodo):
        archivos = glob.glob(os.path.join("MonedasCSV", f"*_{periodo}.csv"))
        for archivo in archivos:
            nombre_mercado = os.path.basename(archivo).split('_')[0]
            print(f"Iniciando MarketConsumer para {nombre_mercado} con periodo {periodo}")
            Thread(target=subprocess.run, args=(["python", "market.py", archivo],)).start()

def seleccionar_periodo():
    periodos = ["D1", "H1", "H4", "M1", "M15", "M30", "M5"]
    
    print("Seleccione un periodo:")
    for i, periodo in enumerate(periodos, start=1):
        print(f"{i}. {periodo}")

    while True:
        try:
            seleccion = int(input("Ingrese el número del periodo: "))
            if 1 <= seleccion <= len(periodos):
                return periodos[seleccion - 1]
            else:
                print("Número no válido. Por favor, intente de nuevo.")
        except ValueError:
            print("Entrada no válida. Por favor, ingrese un número.")

if __name__ == "__main__":
    periodo = seleccionar_periodo()

    broker_host = "localhost"
    broker_port = 54321

    broker = Broker(broker_host, broker_port)
    broker.start_market_consumers(periodo)
    broker.start()
