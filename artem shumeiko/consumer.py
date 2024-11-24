from pika import ConnectionParameters, BlockingConnection

connection_params = ConnectionParameters(
    host="localhost",
    port=5672,
)


def process_message(ch, method, properties, body):
    print(f"Took message: {body.decode()}")
    try:
        x = 1/0
    except:
        pass
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    with BlockingConnection(connection_params) as conn:
        with conn.channel() as ch:
            ch.queue_declare(queue="messages")

            ch.basic_consume(
                queue="messages",
                on_message_callback=process_message,
            )
            print("Wait message!")
            ch.start_consuming()


if __name__ == '__main__':
    main()
