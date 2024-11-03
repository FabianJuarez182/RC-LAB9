import json
import time
from confluent_kafka import Consumer, KafkaError
import matplotlib.pyplot as plt

# Configuración del servidor Kafka y del consumidor
KAFKA_SERVER = 'lab9.alumchat.lol:9092'
TOPIC_NAME = '21440'  

consumer = Consumer({
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': 'grupo_consumidor',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([TOPIC_NAME])

# Listas para almacenar datos históricos
all_temp = []
all_hume = []
all_wind = []

# Configuración de la gráfica en vivo
plt.ion()
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8))
ax1.set_title('Temperatura')
ax2.set_title('Humedad')
ax3.set_title('Dirección del Viento')
ax1.set_ylim(0, 110)  # Rango de temperatura
ax2.set_ylim(0, 100)  # Rango de humedad
ax3.set_yticks(range(0, 8))  # Rango de categorías de viento (N, NO, O, SO, S, SE, E, NE)
direcciones = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']

def actualizar_grafica():
    # Limpiar ejes anteriores
    ax1.cla()
    ax2.cla()
    ax3.cla()
    
    # Actualizar cada gráfico
    ax1.plot(all_temp, label='Temperatura (°C)', color='tab:red')
    ax2.plot(all_hume, label='Humedad (%)', color='tab:blue')
    ax3.plot([direcciones.index(d) for d in all_wind], label='Dirección del Viento', color='tab:green')
    
    # Añadir leyendas y títulos
    ax1.set_title('Temperatura')
    ax1.set_ylim(0, 110)
    ax2.set_title('Humedad')
    ax2.set_ylim(0, 100)
    ax3.set_title('Dirección del Viento')
    ax3.set_yticks(range(len(direcciones)))
    ax3.set_yticklabels(direcciones)

    # Dibujar gráficos actualizados
    plt.draw()
    plt.pause(0.1)

# Procesar y graficar datos en vivo
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Fin de la partición alcanzado {0}/{1}'.format(msg.topic(), msg.partition()))
            else:
                print('Error: {}'.format(msg.error()))
            continue

        # Decodificar el mensaje JSON recibido
        payload = json.loads(msg.value().decode('utf-8'))
        print(f"Mensaje recibido: {payload}")

        # Almacenar datos en las listas correspondientes
        all_temp.append(payload['temperatura'])
        all_hume.append(payload['humedad'])
        all_wind.append(payload['direccion_viento'])

        # Graficar todos los datos
        actualizar_grafica()

except KeyboardInterrupt:
    print("Deteniendo el consumidor...")

finally:
    # Cerrar el consumidor
    consumer.close()
