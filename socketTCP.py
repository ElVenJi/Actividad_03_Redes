import os
import random
import socket
import sys
import time


class SocketTCP:
    """
    Implementación de sockets orientados a conexión sobre datagramas UDP (SOCK_DGRAM).
    Emula el comportamiento esencial del protocolo TCP utilizando Stop & Wait:
    - 3-Way Handshake con tolerancia a pérdidas y resolución de casos borde.
    - Envío y recepción confiable con retransmisión por temporizadores (timeouts).
    - Fragmentación en bloques de hasta 16 bytes y gestión de buffers sobrantes.
    - Cierre de conexión ordenado (FIN / FIN+ACK / ACK) tolerante a pérdidas.
    """

    def __init__(self):
        # Socket UDP subyacente (no orientado a conexión)
        self.sock_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.mi_dir = None
        self.dir_destino = None
        self.seq = 0
        self.tiempo_espera = 1.0

        # Encabezado fijo de 19 bytes ("S|||A|||F|||SSSS|||") + 16 bytes de datos = 35 bytes de buffer UDP
        self.tam_header = 19
        self.max_datos = 16
        self.tam_buff = self.tam_header + self.max_datos
        self.tam_esperado = 0
        self.bytes_sobrantes = b""

        # Modo depuración y pérdidas simuladas a mano mediante variables de entorno:
        #   DEBUG=1 python3 cliente.py ...
        #   PERDIDA_ENVIO=0.2 PERDIDA_RECEPCION=0.2 python3 cliente.py ...
        self.modo_debug = os.environ.get("DEBUG", "0") == "1"
        self.prob_perdida_envio = float(os.environ.get("PERDIDA_ENVIO", "0"))
        self.prob_perdida_recepcion = float(os.environ.get("PERDIDA_RECEPCION", "0"))

    # ==========================================
    # DEPURACIÓN (DEBUG)
    # ==========================================
    def _debug(self, texto: str):
        """Imprime trazas de diagnóstico en la salida de error estándar si el modo debug está activo."""
        if self.modo_debug:
            print(f"[DEBUG] {texto}", file=sys.stderr)

    # ==========================================
    # PROCESAMIENTO DE SEGMENTOS
    # ==========================================
    @staticmethod
    def parse_segment(mensaje_bytes: bytes) -> dict:
        """
        Decodifica un segmento binario delimitado por '|||' y retorna un diccionario
        con los campos de control (syn, ack, fin, seq) y la carga útil en bytes (body).
        """
        trozos = mensaje_bytes.split(b"|||", 4)

        return {
            "syn": int(trozos[0].decode("utf-8", errors="replace")),
            "ack": int(trozos[1].decode("utf-8", errors="replace")),
            "fin": int(trozos[2].decode("utf-8", errors="replace")),
            "seq": int(trozos[3].decode("utf-8", errors="replace")),
            "body": trozos[4] if len(trozos) > 4 else b""
        }

    @staticmethod
    def create_segment(datos_tcp: dict) -> bytes:
        """
        Construye un segmento TCP en bytes con encabezado de longitud fija de 19 bytes
        ('S|||A|||F|||SSSS|||') concatenado con los datos de carga útil.
        """
        syn = datos_tcp.get("syn", 0)
        ack = datos_tcp.get("ack", 0)
        fin = datos_tcp.get("fin", 0)
        seq = datos_tcp.get("seq", 0) % 10000
        cuerpo = datos_tcp.get("body", b"")

        cuerpo_bytes = cuerpo.encode("utf-8") if isinstance(cuerpo, str) else cuerpo

        texto_encabezado = f"{syn}|||{ack}|||{fin}|||{seq:04d}|||"
        return texto_encabezado.encode("utf-8") + cuerpo_bytes

    # ==========================================
    # FUNCIONES DE RED INTERNAS
    # ==========================================
    def _enviar_paquete(self, paquete_bytes: bytes, direccion: tuple):
        """Transmite un datagrama UDP hacia la dirección dada o simula pérdida de envío."""
        if self.prob_perdida_envio > 0 and random.random() < self.prob_perdida_envio:
            self._debug("PÉRDIDA SIMULADA en envío (paquete descartado)")
            return
        self.sock_udp.sendto(paquete_bytes, direccion)

    def _recibir_paquete(self) -> tuple:
        """Recibe un datagrama UDP de tamaño fijo tam_buff; si se simula pérdida entrante, continúa esperando."""
        while True:
            paquete_crudo, dir_remota = self.sock_udp.recvfrom(self.tam_buff)
            if self.prob_perdida_recepcion > 0 and random.random() < self.prob_perdida_recepcion:
                self._debug("PÉRDIDA SIMULADA en recepción (paquete descartado)")
                continue
            return paquete_crudo, dir_remota

    def _enviar_ack(self, seq: int, direccion: tuple):
        """Construye y envía un segmento de confirmación (ACK) puro para la secuencia indicada."""
        datos_ack = {"syn": 0, "ack": 1, "fin": 0, "seq": seq, "body": b""}
        self._enviar_paquete(self.create_segment(datos_ack), direccion)

    # ==========================================
    # FUNCIONES DE CONEXIÓN (3-WAY HANDSHAKE)
    # ==========================================
    def bind(self, direccion: tuple):
        """Vincula el socket UDP interno a una dirección (IP, puerto) de escucha local."""
        self.mi_dir = direccion
        self.sock_udp.bind(direccion)

    def connect(self, direccion: tuple):
        """
        Ejecuta el lado cliente del 3-Way Handshake con tolerancia a pérdidas:
        1. Selecciona número de secuencia inicial aleatorio (0 a 100) y envía SYN.
        2. Aguarda respuesta SYN+ACK retransmitiendo ante timeouts.
        3. Registra la nueva dirección asignada por el servidor y transmite el ACK final.
        """
        self.seq = random.randint(0, 100)
        self.sock_udp.settimeout(self.tiempo_espera)

        datos_syn = {"syn": 1, "ack": 0, "fin": 0, "seq": self.seq, "body": b""}
        paquete_syn = self.create_segment(datos_syn)

        # Fase 1 y 2: Envío de SYN y espera de confirmación SYN+ACK con retransmisión
        while True:
            self._enviar_paquete(paquete_syn, direccion)
            try:
                paquete_crudo, nueva_dir = self._recibir_paquete()
                datos_resp = self.parse_segment(paquete_crudo)

                if datos_resp["syn"] == 1 and datos_resp["ack"] == 1 and datos_resp["seq"] == (self.seq + 1) % 10000:
                    self.dir_destino = nueva_dir
                    self.seq = (datos_resp["seq"] + 1) % 10000
                    self._debug("handshake: SYN+ACK recibido")
                    break
            except socket.timeout:
                self._debug("handshake: timeout esperando SYN+ACK, reenvío SYN")

        # Fase 3: Transmisión del ACK final del establecimiento de conexión
        self._enviar_ack(self.seq, self.dir_destino)
        self._debug("handshake: ACK final enviado")

    def accept(self):
        """
        Ejecuta el lado servidor del 3-Way Handshake:
        1. Espera pasivamente un segmento SYN en el socket principal.
        2. Crea un nuevo SocketTCP enlazado a un puerto efímero asignado por el SO.
        3. Envía SYN+ACK y espera el ACK final con retransmisión por timeout.
        Maneja el caso borde de pérdida del último ACK recibiendo directamente el primer dato.
        Retorna la tupla (nuevo_sock, nueva_dir).
        """
        self.sock_udp.settimeout(None)

        # Fase 1: Recepción del segmento inicial SYN
        while True:
            paquete_crudo, dir_cliente = self._recibir_paquete()
            datos_req = self.parse_segment(paquete_crudo)

            if datos_req["syn"] == 1 and datos_req["ack"] == 0:
                seq_cliente = datos_req["seq"]
                break

        # Crear nuevo socket dedicado para atender exclusivamente la conexión establecida
        nuevo_sock = SocketTCP()
        nuevo_sock.tiempo_espera = self.tiempo_espera

        host_local = self.mi_dir[0] if self.mi_dir else "localhost"
        nuevo_sock.sock_udp.bind((host_local, 0))
        nueva_dir = nuevo_sock.sock_udp.getsockname()

        nuevo_sock.mi_dir = nueva_dir
        nuevo_sock.dir_destino = dir_cliente
        nuevo_sock.seq = (seq_cliente + 1) % 10000
        nuevo_sock.sock_udp.settimeout(nuevo_sock.tiempo_espera)

        datos_syn_ack = {"syn": 1, "ack": 1, "fin": 0, "seq": nuevo_sock.seq, "body": b""}
        paquete_syn_ack = nuevo_sock.create_segment(datos_syn_ack)
        seq_esperado = (seq_cliente + 2) % 10000

        # Fase 2 y 3: Envío de SYN+ACK y espera del ACK final (o primer paquete de datos en caso borde)
        while True:
            nuevo_sock._enviar_paquete(paquete_syn_ack, dir_cliente)
            try:
                paquete_resp, dir_remota = nuevo_sock._recibir_paquete()
                if dir_remota != dir_cliente:
                    continue

                datos_resp = nuevo_sock.parse_segment(paquete_resp)

                # Caso normal: llega el ACK final del handshake
                if datos_resp["syn"] == 0 and datos_resp["ack"] == 1 and datos_resp["seq"] == seq_esperado:
                    nuevo_sock.seq = datos_resp["seq"]
                    nuevo_sock._debug("handshake: ACK final recibido")
                    return nuevo_sock, nueva_dir

                # Caso borde: el ACK final se perdió y el cliente transmitió directamente el primer segmento de datos
                if datos_resp["syn"] == 0 and datos_resp["ack"] == 0 and datos_resp["seq"] == seq_esperado:
                    nuevo_sock.tam_esperado = int(datos_resp["body"].decode("utf-8", errors="replace"))
                    nuevo_sock.seq = (datos_resp["seq"] + len(datos_resp["body"])) % 10000
                    nuevo_sock._enviar_ack(nuevo_sock.seq, dir_cliente)
                    nuevo_sock._debug("handshake: caso borde, primer dato llegó sin ACK final")
                    return nuevo_sock, nueva_dir

            except socket.timeout:
                nuevo_sock._debug("handshake: timeout esperando ACK final, reenvío SYN+ACK")

    # ==========================================
    # ENVÍO Y RECEPCIÓN CON STOP & WAIT
    # ==========================================
    def _enviar_trozo_stop_and_wait(self, pedazo_bytes: bytes):
        """Transmite un bloque unitario de datos y espera su confirmación ACK con retransmisión por timeout."""
        self.sock_udp.settimeout(self.tiempo_espera)
        datos_envio = {"syn": 0, "ack": 0, "fin": 0, "seq": self.seq, "body": pedazo_bytes}
        paquete = self.create_segment(datos_envio)
        ack_esperado = (self.seq + len(pedazo_bytes)) % 10000

        while True:
            self._enviar_paquete(paquete, self.dir_destino)
            try:
                paquete_crudo, _ = self._recibir_paquete()
                datos_resp = self.parse_segment(paquete_crudo)

                # Si llega un SYN+ACK rezagado por pérdida previa, reenviar el ACK del handshake para sincronizar
                if datos_resp["syn"] == 1 and datos_resp["ack"] == 1:
                    self._enviar_ack(self.seq, self.dir_destino)
                    continue

                if datos_resp["ack"] == 1 and datos_resp["seq"] == ack_esperado:
                    self.seq = ack_esperado
                    return
                self._debug(f"ACK inesperado (esperaba {ack_esperado}), sigo esperando")
            except socket.timeout:
                self._debug(f"timeout en seq={self.seq}, retransmito segmento")

    def send(self, mensaje: bytes):
        """
        Transmite inicialmente la longitud total del mensaje en bytes y luego
        el contenido dividido en fragmentos de a lo más 16 bytes mediante Stop & Wait.
        """
        mensaje_bytes = mensaje.encode("utf-8") if isinstance(mensaje, str) else mensaje
        largo_str = str(len(mensaje_bytes)).encode("utf-8")
        self._enviar_trozo_stop_and_wait(largo_str)

        for i in range(0, len(mensaje_bytes), self.max_datos):
            pedazo = mensaje_bytes[i:i + self.max_datos]
            self._enviar_trozo_stop_and_wait(pedazo)

    def recv(self, tam_buffer: int) -> bytes:
        """
        Recibe datos mediante Stop & Wait. El primer segmento determina el largo total del mensaje.
        Retorna a lo más min(tam_esperado, tam_buffer) bytes, preservando cualquier remanente en
        el buffer auxiliar interno para entregas en llamadas sucesivas sin pérdida de información.
        """
        self.sock_udp.settimeout(None)

        if self.tam_esperado == 0 and len(self.bytes_sobrantes) == 0:
            # Recepción del primer segmento que declara el largo total del mensaje
            while True:
                paquete_crudo, _ = self._recibir_paquete()
                datos_req = self.parse_segment(paquete_crudo)

                if datos_req["fin"] == 1:
                    self._completar_cierre_receptor(datos_req)
                    return b""

                if datos_req["syn"] == 0 and datos_req["ack"] == 0:
                    if datos_req["seq"] == self.seq:
                        self.tam_esperado = int(datos_req["body"].decode("utf-8", errors="replace"))
                        self.seq = (self.seq + len(datos_req["body"])) % 10000
                        self._enviar_ack(self.seq, self.dir_destino)
                        break
                    # Segmento duplicado: reenviar ACK para permitir que el emisor continúe
                    self._enviar_ack(self.seq, self.dir_destino)

        meta_bytes = min(self.tam_esperado, tam_buffer)
        acumulado = self.bytes_sobrantes
        self.bytes_sobrantes = b""

        while len(acumulado) < meta_bytes:
            paquete_crudo, _ = self._recibir_paquete()
            datos_req = self.parse_segment(paquete_crudo)

            if datos_req["fin"] == 1:
                self._completar_cierre_receptor(datos_req)
                break

            if datos_req["syn"] == 0 and datos_req["ack"] == 0:
                if datos_req["seq"] == self.seq:
                    pedazo = datos_req["body"]
                    acumulado += pedazo
                    self.seq = (self.seq + len(pedazo)) % 10000
                # Se confirma siempre con el ACK del número de secuencia esperado
                self._enviar_ack(self.seq, self.dir_destino)

        bytes_retorno = acumulado[:meta_bytes]
        self.bytes_sobrantes = acumulado[meta_bytes:]
        self.tam_esperado -= len(bytes_retorno)

        return bytes_retorno

    # ==========================================
    # CIERRE DE CONEXIÓN TOLERANTE A PÉRDIDAS
    # ==========================================
    def close(self):
        """
        Inicia el fin de conexión desde el lado emisor (Host A):
        Envía FIN y espera FIN+ACK hasta por 3 timeouts (retransmitiendo FIN).
        Al recibir la confirmación, envía 3 veces el último ACK espaciado por un timeout.
        """
        self.sock_udp.settimeout(self.tiempo_espera)
        datos_fin = {"syn": 0, "ack": 0, "fin": 1, "seq": self.seq, "body": b""}
        paquete_fin = self.create_segment(datos_fin)
        seq_esperado = (self.seq + 1) % 10000

        llego_fin_ack = False
        for intento in range(3):
            self._enviar_paquete(paquete_fin, self.dir_destino)
            try:
                paquete_crudo, _ = self._recibir_paquete()
                datos_resp = self.parse_segment(paquete_crudo)

                if datos_resp["fin"] == 1 and datos_resp["ack"] == 1 and datos_resp["seq"] == seq_esperado:
                    self.seq = (datos_resp["seq"] + 1) % 10000
                    llego_fin_ack = True
                    break
            except socket.timeout:
                self._debug(f"cierre: timeout {intento + 1}/3 esperando FIN+ACK")

        if llego_fin_ack:
            paquete_ultimo_ack = self.create_segment(
                {"syn": 0, "ack": 1, "fin": 0, "seq": self.seq, "body": b""})
            for _ in range(3):
                self._enviar_paquete(paquete_ultimo_ack, self.dir_destino)
                time.sleep(self.tiempo_espera)
        else:
            self._debug("cierre: no llegó FIN+ACK, asumo que la contraparte cerró")

        self.sock_udp.close()

    def recv_close(self):
        """
        Atiende el cierre de conexión desde el lado receptor (Host B):
        Espera la llegada del segmento FIN y coordina la liberación ordenada de recursos.
        """
        self.sock_udp.settimeout(None)
        while True:
            paquete_crudo, _ = self._recibir_paquete()
            datos_req = self.parse_segment(paquete_crudo)
            if datos_req["fin"] == 1:
                self._completar_cierre_receptor(datos_req)
                break
            # Datos duplicados (el ACK previo se perdió en la red): reenviar ACK para no bloquear al emisor
            if datos_req["syn"] == 0 and datos_req["ack"] == 0:
                self._enviar_ack(self.seq, self.dir_destino)

    def _completar_cierre_receptor(self, datos_fin: dict):
        """
        Secuencia interna del Host B tras recibir FIN:
        Transmite FIN+ACK y espera el último ACK hasta por un máximo de 3 timeouts.
        """
        self.seq = (datos_fin["seq"] + 1) % 10000
        datos_fin_ack = {"syn": 0, "ack": 1, "fin": 1, "seq": self.seq, "body": b""}
        paquete_fin_ack = self.create_segment(datos_fin_ack)
        seq_esperado = (self.seq + 1) % 10000

        self.sock_udp.settimeout(self.tiempo_espera)
        for intento in range(3):
            self._enviar_paquete(paquete_fin_ack, self.dir_destino)
            try:
                paquete_crudo, _ = self._recibir_paquete()
                datos_resp = self.parse_segment(paquete_crudo)

                if datos_resp["ack"] == 1 and datos_resp["fin"] == 0 and datos_resp["seq"] == seq_esperado:
                    break
            except socket.timeout:
                self._debug(f"cierre receptor: timeout {intento + 1}/3 esperando ACK final")

        self.sock_udp.close()