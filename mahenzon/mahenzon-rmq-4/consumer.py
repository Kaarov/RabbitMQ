import time
import random
from typing import TYPE_CHECKING
import logging

from config import (
    configure_logging,
    MQ_ROUTING_KEY,
)

from rabbit.common import SimpleRabbit

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import Basic, BasicProperties

log = logging.getLogger(__name__)


def process_new_message(
        channel: "BlockingChannel",
        method: "Basic",
        properties: "BasicProperties",
        body: bytes,
):
    log.debug("channel: %s", channel)
    log.debug("method: %s", method)
    log.debug("properties: %s", properties)
    log.debug("body: %s", body)

    log.warning("[ ] Start processing message (expensive task!) %r", body)
    start_time = time.time()

    number = int(body[-2:])
    is_odd = number % 2
    ...
    time.sleep(1 + is_odd * 2)
    ...
    end_time = time.time()
    if random.random() > .5:
        # log.info("--- Could not process processing message %r, sending nack!", body)
        # channel.basic_nack(delivery_tag=method.delivery_tag)
        log.info("--- Could not process processing message %r, sending nack (no requeue)!", body)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        # log.info("--- Could not process processing message %r, sending reject!", body)
        # channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        # log.info("--- Could not process processing message %r, sending reject (requeue)!", body)
        # channel.basic_reject(delivery_tag=method.delivery_tag)
    else:
        log.info("+++ Finished processing message %r, sending ack!", body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    log.warning(
        "[X] Finished in %.2fs processing message %r",
        end_time - start_time,
        body,
    )


def consume_messages(channel: "BlockingChannel") -> None:
    channel.basic_qos(prefetch_count=3)
    channel.queue_declare(MQ_ROUTING_KEY)
    channel.basic_consume(
        queue=MQ_ROUTING_KEY,
        on_message_callback=process_new_message,
        # auto_ack=True,
    )
    log.warning("Waiting for messages...")
    channel.start_consuming()


def main():
    configure_logging(level=logging.INFO)
    with SimpleRabbit() as rabbit:
        rabbit.consume_messages(
            message_callback=process_new_message,
        )


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")
