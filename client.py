import socket
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import matplotlib.dates as mdates
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.ticker as ticker

class Client:
    # Asumimos que el máximo de mercados es 9.
    MAX_MARKETS = 9

    def __init__(self, broker_host, broker_port):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.market_data = {}
        self.ax_dict = {}
        self.fig, self.axes = plt.subplots(nrows=3, ncols=3, figsize=(15, 15))
        plt.ion()  # Activa el modo interactivo de matplotlib

    def connect_to_broker(self):
        print("Intentando conectar con el bróker...")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.broker_host, self.broker_port))
            print("Conexión establecida con el bróker.")
            s.sendall("CLIENT".encode())

            incomplete_data = ''  # Almacenará datos incompletos para procesarlos luego

            while True:
                data = s.recv(1024).decode()
                if not data:
                    break
                data = incomplete_data + data  # Añade los datos incompletos al principio del nuevo mensaje
                messages = data.split('\n')
                incomplete_data = messages.pop()  # El último elemento podría estar incompleto

                for message in messages:
                    self.process_data(message)
                plt.pause(0.01)  # Pausa para la actualización de la gráfica

    def process_data(self, message):
        try:
            market, timestamp, open_price, high, low, close, volume = message.split(',')
            # Verifica si ya se alcanzó el máximo de mercados y no intenta crear más subgráficos
            if market not in self.market_data and len(self.market_data) < self.MAX_MARKETS:
                self.init_market(market)
            self.update_market_data(market, timestamp, open_price, high, low, close, volume)
            self.redraw_market(market)
        except ValueError:
            print(ValueError)
            print(f"Mensaje incompleto o con formato incorrecto: {message}")
        except KeyError:
            print(f"No hay subgráfico disponible para el mercado: {market}")

    def init_market(self, market):
        # Inicializa el DataFrame sin la columna 'ax'
        self.market_data[market] = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        row_idx = (len(self.market_data) - 1) // 3
        col_idx = (len(self.market_data) - 1) % 3
        ax = self.axes[row_idx, col_idx]
        ax.set_title(market)
        # Almacena el objeto ax en un diccionario separado si es necesario
        self.ax_dict[market] = ax

    def update_market_data(self, market, timestamp, open_price, high, low, close, volume):
        df = self.market_data[market]
        new_row = [
            mdates.date2num(datetime.strptime(timestamp, '%Y-%m-%d %H:%M')),
            float(open_price),
            float(high),
            float(low),
            float(close),
            int(volume)
        ]
        df.loc[len(df)] = new_row  # Agrega la nueva fila al final del DataFrame

        if len(df) > 20:
            df.drop(df.index[0], inplace=True)  # Elimina la primera fila si el DataFrame tiene más de 20 filas
            df.reset_index(drop=True, inplace=True)  # Restablece el índice del DataFrame después de la eliminación


    def redraw_market(self, market):
        ax = self.ax_dict[market]
        df = self.market_data[market]
        
        ax.clear()
        ax.set_title(market)  # Vuelve a establecer el título después de limpiar el eje.
        
        ax.xaxis_date()
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        candlestick_ohlc(ax, df[['Date', 'Open', 'High', 'Low', 'Close']].values, width=0.6, colorup='g', colordown='r')
        ax.xaxis.set_major_locator(ticker.MaxNLocator(6))


if __name__ == "__main__":
    broker_host = "localhost"
    broker_port = 54321

    client = Client(broker_host, broker_port)
    client.connect_to_broker()
    
