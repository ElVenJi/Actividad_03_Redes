import socketTCP

# ==========================================
# 1. CONFIGURACIÓN DEL SERVIDOR
# ==========================================
direccion = ('localhost', 8000)

# Inicialización del socket principal de escucha y enlace de dirección
socket_servidor_tcp = socketTCP.SocketTCP()
socket_servidor_tcp.bind(direccion)

# Espera pasiva y establecimiento de la conexión entrante (3-Way Handshake)
socket_conexion_tcp, nueva_direccion = socket_servidor_tcp.accept()

# ==========================================
# 2. RECEPCIÓN DE DATOS MEDIANTE STOP & WAIT
# ==========================================
# El primer recv procesa el largo total del mensaje y el primer fragmento de datos
contenido = socket_conexion_tcp.recv(1024)

# Continuar leyendo mientras falten bytes del mensaje declarado o queden datos en el buffer sobrante
while socket_conexion_tcp.tam_esperado > 0 or socket_conexion_tcp.bytes_sobrantes:
    trozo = socket_conexion_tcp.recv(1024)
    if not trozo:
        break
    contenido += trozo

# ==========================================
# 3. SALIDA ESTÁNDAR Y CIERRE CONTROLADO
# ==========================================
# Imprimir el contenido íntegro recibido en la salida estándar
print(contenido.decode("utf-8", errors="replace"))

# Cierre ordenado de la conexión desde el extremo receptor (Host B) y liberación del socket
socket_conexion_tcp.recv_close()
socket_servidor_tcp.sock_udp.close()