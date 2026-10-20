Actividad 3: Sockets orientados a conexión con Stop & Wait


































Benjamín Alvarado Guerra
github: https://github.com/ElVenJi/Actividad_03_Redes
DECLARACIÓN DE USO DE IA: Durante la realización de esta actividad, se utilizó GEMINIAI en su versión flash como apoyo pedagógico para entender de mejor manera los conceptos además de ayuda a la hora de entender y corregir errores. También sirvió como “compañero” a la hora de idear código. También ayudó a documentar el código y mejorar redacción.


ARCHIVOS:
socketTCP.py: la clase SocketTCP, que tiene toda la lógica.
cliente1.py: lee un archivo desde la entrada estándar y lo envía.
servidor1.py: recibe el mensaje y lo guarda en recibido.txt
test.txt: archivo prueba para enviar mensaje.
servidor_buff: para test
recibido_buff: para test

FLUJO DE FUNCIONAMIENTO DE CÓDIGO:
La idea general es esta: el servidor se queda escuchando, el cliente se conecta, manda el archivo en pedazos y cierra. Cada paso tiene su función:

bind() y accept() (servidor): el servidor queda escuchando en localhost:8000. Cuando llega un SYN, accept() crea un socket nuevo en otro puerto, porque el puerto original tiene que quedar libre para otras conexiones. Ese socket nuevo es el que se usa para toda la conversación.

connect() (cliente): manda un SYN con un número de secuencia aleatorio entre 0 y 100, espera el SYN+ACK y responde con un ACK. Si algo se pierde, reintenta por timeout.

send() (cliente): primero manda el largo del mensaje en un segmento aparte, para que el servidor sepa cuánto esperar. Después parte el contenido en trozos de 16 bytes y envía cada uno con Stop & Wait: manda, espera el ACK, y si no llega a tiempo, reenvía.

recv() (servidor): el primer segmento trae el largo, así que el servidor sabe cuántos bytes faltan. Va acumulando datos y confirmando cada uno. Si el archivo es más grande que el buff_size que pidió la aplicación, devuelve solo un pedazo y guarda el resto para la siguiente llamada.

close() y recv_close(): el cliente manda FIN, el servidor responde FIN+ACK y el cliente confirma. Si alguien no responde después de tres timeouts, se asume que la otra parte ya cerró, para no quedar colgados.


PASOS PARA EJECUTAR:
Abre dos terminales en la carpeta del proyecto. 
Terminal 1, servidor: python3 servidor.py 
Terminal 2, cliente: python3 cliente.py < archivo.txt
Cuando termine, el archivo recibido queda en recibido.txt. 

Para comprobar que llegó igual: 
diff archivo.txt recibido.txt 

Con pérdidas simuladas a mano: agrega variables de entorno al cliente y, si quieres ver lo que pasa por dentro, también 
DEBUG=1: DEBUG=1 PERDIDA_ENVIO=0.2 PERDIDA_RECEPCION=0.2 python3 cliente.py < archivo.txt 

Con netem: antes de probar, activa la pérdida en localhost: 
sudo tc qdisc add dev lo root netem loss 20.0% delay 0.5s 

Y cuando termines, sácala: 
sudo tc qdisc del dev lo root netem



DIAGRAMAS:
PASO 4



Paso 7

Para la parte 5, lo que sobra se guarda en bytes_sobrantes. Si llegan 16 bytes y buff_size es 10, devolvemos 10 y guardamos 6. Así no se pierde nada. Lo más importante es que el sobrante no se descarta.


TEST1 > 16 bytes

TEST n no multiplo de 16

Test > 16 bytes con pérdida


TEST n no mult 16 con pérdida

