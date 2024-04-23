from kafka import KafkaProducer

from config import KAFKA_TOPIC, KAFKA_HOST, KAFKA_PORT

producer = KafkaProducer(bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}")

if __name__ == '__main__':
    producer.send(KAFKA_TOPIC,
                  b'{"id":3,"status":"pending","repo_url":"git@github.com:USST-CS-2024-01/USST-CS-2024-Backend.git","group_id":4,"commit_stats":null,"code_line_stats":null,"create_time":1713854897,"stat_time":null,"user_repo_mapping":null}')
    producer.flush()
