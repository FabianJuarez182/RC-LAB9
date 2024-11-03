import random
import time
from confluent_kafka import Producer

# Configuración del servidor Kafka
KAFKA_SERVER = 'lab9.alumchat.lol:9092'
TOPIC_NAME = '21440'

producer = Producer({'bootstrap.servers': KAFKA_SERVER})

# Función para codificar los datos en 3 bytes (24 bits)
def encode_data(temperatura, humedad, direccion_viento):
    # Convertir la temperatura en un entero de 14 bits (multiplicando por 100)
    temp_encoded = int(temperatura * 100) & 0x3FFF  # limitar a 14 bits
    hum_encoded = int(humedad) & 0x7F  # limitar a 7 bits
    dir_encoded = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE'].index(direccion_viento) & 0x7  # limitar a 3 bits

    # Combinar en un solo valor de 24 bits
    encoded_value = (temp_encoded << 10) | (hum_encoded << 3) | dir_encoded

    # Dividir en 3 bytes
    return encoded_value.to_bytes(3, 'big')



# Función para generar datos de los sensores
def generar_datos_sensores():
    temperatura = round(random.uniform(0, 110), 2)  # Rango [0, 110] para temperatura
    humedad = random.randint(0, 100)  # Rango [0, 100] para humedad
    direccion_viento = random.choice(['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE'])
    return temperatura, humedad, direccion_viento

def enviar_datos():
    while True:
        # Generar datos y codificarlos
        temperatura, humedad, direccion_viento = generar_datos_sensores()
        payload = encode_data(temperatura, humedad, direccion_viento)
        print(f"Enviando payload codificado: {payload}")

        # Enviar los datos codificados al servidor Kafka
        producer.produce(TOPIC_NAME, payload)
        producer.poll(0)
        
        # Esperar entre 15 y 30 segundos
        time.sleep(random.randint(15, 30))

if __name__ == "__main__":
    try:
        enviar_datos()
    except KeyboardInterrupt:
        print("Deteniendo el productor.")
    finally:
        producer.flush()
