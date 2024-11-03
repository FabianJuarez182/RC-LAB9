import time
from confluent_kafka import Consumer, KafkaError
import matplotlib.pyplot as plt

# Configuración del servidor Kafka
KAFKA_SERVER = 'lab9.alumchat.lol:9092'
TOPIC_NAME = '21440'

consumer = Consumer({
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': 'grupo_consumidor',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([TOPIC_NAME])

# Listas para datos históricos
all_temp = []
all_hume = []
all_wind = []

plt.ion()
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8))
ax1.set_title('Temperatura')
ax2.set_title('Humedad')
ax3.set_title('Dirección del Viento')
direcciones = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']

# Función para decodificar los datos del payload de 3 bytes
def decode_data(payload):
    # Convertir de bytes a un entero de 24 bits
    encoded_value = int.from_bytes(payload, 'big')
    
    # Extraer los 3 campos de los bits correspondientes
    temp_encoded = (encoded_value >> 10) & 0x3FFF
    hum_encoded = (encoded_value >> 3) & 0x7F
    dir_encoded = encoded_value & 0x7

    # Decodificar cada campo
    temperatura = temp_encoded / 100.0
    humedad = hum_encoded
    direccion_viento = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE'][dir_encoded]

    return temperatura, humedad, direccion_viento

def actualizar_grafica():
    ax1.cla()
    ax2.cla()
    ax3.cla()
    
    ax1.plot(all_temp, color='tab:red')
    ax2.plot(all_hume, color='tab:blue')
    ax3.plot([direcciones.index(d) for d in all_wind], color='tab:green')
    
    ax1.set_title('Temperatura')
    ax2.set_title('Humedad')
    ax3.set_title('Dirección del Viento')
    ax3.set_yticks(range(len(direcciones)))
    ax3.set_yticklabels(direcciones)

    plt.draw()
    plt.pause(0.1)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Fin de la partición alcanzado {msg.topic()}/{msg.partition()}")
            else:
                print(f"Error: {msg.error()}")
            continue

        # Decodificar el mensaje recibido
        temperatura, humedad, direccion_viento = decode_data(msg.value())
        print(f"Datos recibidos - Temperatura: {temperatura}, Humedad: {humedad}, Dirección: {direccion_viento}")

        # Almacenar los datos
        all_temp.append(temperatura)
        all_hume.append(humedad)
        all_wind.append(direccion_viento)

        # Actualizar la gráfica
        actualizar_grafica()

except KeyboardInterrupt:
    print("Deteniendo el consumidor.")
finally:
    consumer.close()
