import pika
from pika.adapters.blocking_connection import BlockingConnection
import config
from .exc import RabbitException


class RabbitBase:
    def __init__(
        self,
        connection_params: pika.ConnectionParameters = config.connection_params,
    ):
        self.connection_params = connection_params
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingConnection | None = None

    def get_connection(self) -> BlockingConnection | None:
        return pika.BlockingConnection(self.connection_params)

    @property
    def channel(self) -> BlockingConnection | None:
        if self._channel is None:
            raise RabbitException("Please use context manager for Rabbit helper.")
        return self._channel

    def __enter__(self):
        self._connection = self.get_connection()
        self._channel = self._connection.channel()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._channel.is_open:
            self._channel.close()
        if self._connection.is_open:
            self._connection.close()
