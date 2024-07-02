from kafka import KafkaConsumer
import json

KAFKA_TOPIC = 'logs_topic'
KAFKA_SERVER = 'localhost:9092'

def consume_from_kafka():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='logs_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("Kafka Consumer criado com sucesso.")
    except Exception as e:
        print(f"Erro ao criar Kafka Consumer: {e}")
        return

    print(f"Consumindo mensagens do tópico: {KAFKA_TOPIC}")
    for message in consumer:
        log = message.value
        try:
            process_log(log)
        except Exception as e:
            print(f"Erro ao processar a mensagem: {e}")

def process_log(log):
    print(f"Log recebido: {log}")

if __name__ == "__main__":
    consume_from_kafka()
    process_log()
