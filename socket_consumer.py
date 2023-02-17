# cria o websocket em python para dar a resposta em tempo real

import socket
import time

HOST = 'localhost'
PORT = 3000

s = socket.socket()
s.connect((HOST, PORT))

# seu consomer recebe as mensagem até acabar
while True:
  data = s.recv(1024)
  dados = data.decode('utf-8')
  if(data == None or data == ""):
    break
  print(dados)
  time.sleep(2)
