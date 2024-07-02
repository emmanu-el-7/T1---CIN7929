import pandas as pd
from kafka import KafkaProducer
import json

# Configuração do Kafka
KAFKA_TOPIC = 'logs_topic'
KAFKA_SERVER = 'localhost:9092'

def csv_to_kafka(csv_file):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka Producer criado com sucesso.")
    except Exception as e:
        print(f"Erro ao criar Kafka Producer: {e}")
        return

    # Ler o arquivo CSV
    try:
        df = pd.read_csv(csv_file)
        print(f"Arquivo {csv_file} lido com sucesso.")
    except FileNotFoundError:
        print(f"Arquivo {csv_file} não encontrado.")
        return
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
        return
    
    # Enviar cada linha do CSV para o Kafka
    for index, row in df.iterrows():
        try:
            producer.send(KAFKA_TOPIC, row.to_dict())
            print(f"Linha {index} enviada com sucesso: {row.to_dict()}")
        except Exception as e:
            print(f"Erro ao enviar mensagem para o Kafka: {e}")

    try:
        producer.flush()
        producer.close()
        print("Envio de mensagens concluído e Kafka Producer fechado.")
    except Exception as e:
        print(f"Erro ao finalizar o Kafka Producer: {e}")

if __name__ == "__main__":
    csv_file = 'euro2024_players.csv'
    csv_to_kafka(csv_file)
