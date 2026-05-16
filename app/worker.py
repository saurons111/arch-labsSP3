import json
import os
import time

import pika


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

TASK_EXCHANGE = "task.exchange"
TASK_QUEUE = "task.events"
TASK_ROUTING_KEY = "task.*"


def callback(ch, method, properties, body):
    event = json.loads(body)

    print("Received task event:", event, flush=True)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)

            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials
            )

            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            channel.exchange_declare(
                exchange=TASK_EXCHANGE,
                exchange_type="topic",
                durable=True
            )

            channel.queue_declare(
                queue=TASK_QUEUE,
                durable=True
            )

            channel.queue_bind(
                exchange=TASK_EXCHANGE,
                queue=TASK_QUEUE,
                routing_key=TASK_ROUTING_KEY
            )

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=TASK_QUEUE,
                on_message_callback=callback
            )

            print("Notification worker started. Waiting for task events...", flush=True)
            channel.start_consuming()

        except Exception as error:
            print(f"RabbitMQ connection failed: {error}. Retrying in 5 seconds...", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()