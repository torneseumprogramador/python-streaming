# cria o websocket em python para dar a resposta em tempo real
import socket
import time

HOST = 'localhost'
PORT = 3000

s = socket.socket()
s.bind((HOST, PORT))
print(f'Aguardando conexão na porta: {PORT}')

s.listen(5)
conn, address = s.accept()

print (f'Recebendo solicitação de {address}')

# seu producer manda mensagem para o socket aberto
messages = [
  'Mensagem A',
  'Mensagem B',
  'Mensagem C',
  'Mensagem D',
  'Mensagem E',
  'Mensagem F',
  'Estou animado com o curso'
]

for message in messages:
  message = bytes (message, 'utf-8')
  conn.send(message)
  time.sleep(4)

conn.close()

