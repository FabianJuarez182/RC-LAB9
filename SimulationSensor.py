import json
import random
import time
from confluent_kafka import Producer

# Configuración del servidor Kafka
KAFKA_SERVER = 'lab9.alumchat.lol:9092'
TOPIC_NAME = '21440_21469'

# Configuración del productor de Kafka
producer = Producer({
    'bootstrap.servers': KAFKA_SERVER
})

# Función para generar datos de temperatura (distribución normal)
def generar_temperatura():
    return round(random.gauss(55, 10), 2)

# Función para generar datos de humedad (distribución normal)
def generar_humedad():
    return max(0, min(int(random.gauss(55, 10)), 100))

# Función para generar datos de dirección del viento
def generar_direccion_viento():
    direcciones = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']
    return random.choice(direcciones)

# Función para simular la generación de datos de los sensores
def generar_datos_sensores():
    return {
        "temperatura": generar_temperatura(),
        "humedad": generar_humedad(),
        "direccion_viento": generar_direccion_viento()
    }

# Función de callback para verificar la entrega de mensajes
def delivery_report(err, msg):
    if err is not None:
        print(f"Error al enviar mensaje: {err}")
    else:
        print(f"Mensaje enviado: {msg.value().decode('utf-8')} a {msg.topic()} [partición: {msg.partition()}]")

# Enviar datos al servidor Kafka de manera periódica
def enviar_datos():
    while True:
        datos = generar_datos_sensores()
        print(f"Enviando datos: {datos}")
        # Enviar datos al servidor Kafka
        producer.produce(TOPIC_NAME, json.dumps(datos).encode('utf-8'), callback=delivery_report)
        # Llamar a poll() para asegurar la entrega de los mensajes
        producer.poll(0)
        # Esperar entre 15 y 30 segundos antes de enviar la siguiente lectura
        time.sleep(random.randint(15, 30))

if __name__ == "__main__":
    try:
        enviar_datos()
    except KeyboardInterrupt:
        print("Deteniendo la simulación de sensores.")
    finally:
        # Esperar a que todos los mensajes en el buffer sean enviados
        producer.flush()
