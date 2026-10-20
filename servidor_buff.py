import socketTCP

n = 10
servidor = socketTCP.SocketTCP()
servidor.bind(('localhost', 8000))
conexion, _ = servidor.accept()

primera = conexion.recv(n)
segunda = conexion.recv(n)
open("recibido_buff.txt", "wb").write(primera + segunda)

conexion.recv_close()
servidor.sock_udp.close()