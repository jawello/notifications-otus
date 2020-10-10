import asyncio
from aio_pika import connect, IncomingMessage
from config import load_config

import logging

log = logging.getLogger(__name__)


async def on_message(message: IncomingMessage):
    """
    on_message doesn't necessarily have to be defined as async.
    Here it is to show that it's possible.
    """
    print(" [x] Received message %r" % message)
    print("Message body is: %r" % message.body)
    print("Before sleep!")
    await asyncio.sleep(5)  # Represents async I/O operations
    print("After sleep!")


async def main(loop, config_path):

    config = load_config(config_path)
    logging.basicConfig(level=logging.DEBUG)
    log.debug(config)

    # Perform connection
    rabbitmq_config = config['rabbitmq']
    connection = await connect(
        host=rabbitmq_config['host'],
        login=rabbitmq_config.get('login', 'guest'),
        password=rabbitmq_config.get('password', 'guest'),
        port=int(rabbitmq_config.get('port', 5672)),
        loop=loop
    )

    # Creating a channel
    channel = await connection.channel()

    # Declaring queue
    queue = await channel.declare_queue(rabbitmq_config[''])

    # Start listening the queue with name 'hello'
    await queue.consume(on_message)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Provide path to config file")
    parser.add_argument("-cm", "--config-migration", help="Provide path to config migration file")
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))

    # we enter a never-ending loop that waits for data and
    # runs callbacks whenever necessary.
    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()
