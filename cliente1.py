import socketTCP

# ==========================================
# 1. CONFIGURACIÓN DEL CLIENTE
# ==========================================
direccion = ('localhost', 8000)

# Lectura binaria completa desde la entrada estándar (python3 cliente1.py < archivo.txt)
datos_archivo = open(0, "rb").read()

# ==========================================
# 2. CONEXIÓN Y TRANSMISIÓN CONFIABLE
# ==========================================
# Establecimiento de la conexión con el servidor mediante el 3-Way Handshake
socket_cliente_tcp = socketTCP.SocketTCP()
socket_cliente_tcp.connect(direccion)

print("Conectado al servidor exitosamente.")

# Transmisión del contenido íntegro fragmentado en trozos de hasta 16 bytes mediante Stop & Wait
socket_cliente_tcp.send(datos_archivo)

# ==========================================
# 3. CIERRE DE CONEXIÓN
# ==========================================
print("Cerrando conexión desde cliente...")
socket_cliente_tcp.close()
print("Cliente cerrado con éxito!")