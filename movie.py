import json
import pika
from pika.exchange_type import ExchangeType

# Connection parameters
connection_parameters = pika.ConnectionParameters('localhost')

try:
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()

    # Define the exchange
    exchange_name = 'payment_requests'
    channel.exchange_declare(exchange=exchange_name, exchange_type=ExchangeType.fanout)

    # Construct the payment request message
    payment_data = {
        "user_id": 123,
        "amount": 100.00,
        "currency": "USD",
        "description": "Movie purchase"
    }

    # Validate JSON data (optional)
    try:
        json.dumps(payment_data)  # This will raise an exception if there's an error
    except json.JSONDecodeError as e:
        print(f"Error encoding JSON data: {e}")
        connection.close()
        exit(1)

    # Encode to bytes
    message = json.dumps(payment_data).encode('utf-8')

    # Publish the message
    channel.basic_publish(exchange=exchange_name, routing_key='', body=message)
    print(f"Sent payment request message: {payment_data}")

except pika.exceptions.ConnectionClosed as e:
    print(f"Connection error: {e}")
except Exception as e:  # Catch any other unexpected errors
    print(f"Unexpected error: {e}")
finally:
    # Close the connection (always recommended)
    connection.close()
