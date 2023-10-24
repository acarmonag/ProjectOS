import socket

class Client:

    def __init__(self, broker_host, broker_port):
        self.broker_host = broker_host
        self.broker_port = broker_port

    def connect_to_broker(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.broker_host, self.broker_port))
            s.sendall("CLIENT".encode())

            while True:
                data = s.recv(1024).decode()
                if not data:
                    break
                print("Datos recibidos:", data)

if __name__ == "__main__":
    broker_host = "localhost"
    broker_port = 54321

    client = Client(broker_host, broker_port)
    client.connect_to_broker()

