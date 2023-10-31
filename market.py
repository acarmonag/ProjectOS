import socket
import time
import sys
import os

class MarketConsumer:

    def __init__(self, broker_host, broker_port, csv_file):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.csv_file = os.path.join("MonedasCSV", csv_file)  # Utiliza el directorio MonedasCSV como ruta predeterminada
        self.market_name = csv_file.split('.')[0]  # Asumimos que el nombre del mercado es el nombre del archivo sin extensión

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.broker_host, self.broker_port))
            s.sendall(f"MARKET_CONSUMER_{self.market_name}".encode())  # Envía el nombre del mercado al broker

            # Espera la confirmación del broker
            confirmation = s.recv(1024).decode()
            if confirmation == "CLIENT_CONNECTED":
                with open(self.csv_file, 'r') as file:
                    for line in file:
                        s.sendall(f"{self.market_name},{line.strip()}".encode())  # Envía los datos con el nombre del mercado
                        time.sleep(1)  # Envía los datos línea por línea con un intervalo

if __name__ == "__main__":
    broker_host = "localhost"
    broker_port = 54321
    
    if len(sys.argv) < 2:
        print("Por favor, proporcione el nombre del archivo CSV como argumento.")
        sys.exit(1)

    csv_file = sys.argv[1]  # Obtiene el nombre del archivo CSV desde el argumento de la línea de comandos

    mc = MarketConsumer(broker_host, broker_port, csv_file)
    mc.start()
