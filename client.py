import socket
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import matplotlib.dates as mdates
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.ticker as ticker

class Client:
    MAX_MARKETS = 9

    def __init__(self, broker_host, broker_port):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.market_data = {}
        self.ax_dict = {}
        self.fig, self.axes = plt.subplots(nrows=3, ncols=3, figsize=(15, 15))
        plt.ion()

    def connect_to_broker(self):
        print("Intentando conectar con el bróker...")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.broker_host, self.broker_port))
            print("Conexión establecida con el bróker.")
            s.sendall("CLIENT".encode())

            incomplete_data = ''
            while True:
                data = s.recv(1024).decode()
                if not data:
                    break
                data = incomplete_data + data
                messages = data.split('\n')
                incomplete_data = messages.pop()

                for message in messages:
                    self.process_data(message)
                plt.pause(0.1) #tiempo de espera para que se actualice el gráfico igual a 0.1 segundos

    def process_data(self, message):
        try:
            parts = message.split(',')
            market, idx, timestamp, open_price, high, low, close, volume = parts[:8]
            if market not in self.market_data and len(self.market_data) < self.MAX_MARKETS:
                self.init_market(market)
            self.update_market_data(market, idx, timestamp, open_price, high, low, close, volume)
            self.redraw_market(market)
        except ValueError:
            print(f"Mensaje incompleto o con formato incorrecto: {message}")

    def init_market(self, market):
        self.market_data[market] = pd.DataFrame(columns=['Index', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'SMA5', 'SMA13'])
        row_idx = (len(self.market_data) - 1) // 3
        col_idx = (len(self.market_data) - 1) % 3
        ax = self.axes[row_idx, col_idx]
        ax.set_title(market)
        self.ax_dict[market] = ax

    def update_market_data(self, market, idx, timestamp, open_price, high, low, close, volume):
        df = self.market_data[market]
        new_row = {
            'Index': int(idx),
            'Date': mdates.date2num(datetime.strptime(timestamp, '%Y-%m-%d %H:%M')),
            'Open': float(open_price),
            'High': float(high),
            'Low': float(low),
            'Close': float(close),
            'Volume': int(volume),
        }
        df = df._append(new_row, ignore_index=True)
        df['SMA5'] = df['Close'].rolling(window=5).mean()
        df['SMA13'] = df['Close'].rolling(window=13).mean()

        if len(df) > 20:
            df.drop(df.index[0], inplace=True)
            df.reset_index(drop=True, inplace=True)

        self.market_data[market] = df  # Asegúrate de actualizar el DataFrame en el diccionario

    def redraw_market(self, market):
        ax = self.ax_dict[market]
        df = self.market_data[market]

        ax.clear()
        ax.set_title(market)
        candlestick_ohlc(ax, df[['Index', 'Open', 'High', 'Low', 'Close']].values, width=0.6, colorup='g', colordown='r')

        if len(df) > 4:
            ax.plot(df['Index'], df['SMA5'], label='SMA 5', color='blue')
        if len(df) > 12:
            ax.plot(df['Index'], df['SMA13'], label='SMA 13', color='orange')

        ax.legend()
        ax.xaxis.set_major_locator(ticker.MaxNLocator(6))
        ax.figure.canvas.draw()  # Esto asegura que el gráfico se actualice

if __name__ == "__main__":
    broker_host = "localhost"
    broker_port = 54321
    client = Client(broker_host, broker_port)
    client.connect_to_broker()
