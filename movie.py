import pika
from pika.exchange_type import ExchangeType

connection_parameters = pika.ConnectionParameters('localhost')

connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel()

channel.exchange_declare(exchange="pubsub", exchange_type= ExchangeType.fanout)

meesage = "this is testing movie"

channel.basic_publish(exchange='pubsub', routing_key='', body=meesage)

print(f"send message: {meesage}")

connection.close()