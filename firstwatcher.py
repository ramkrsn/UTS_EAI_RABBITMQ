import json
import pika
from pika.exchange_type import ExchangeType

def on_message_received(ch, method, properties, body):
    """Callback function to handle received messages from the RabbitMQ queue."""

    # Decode the message body (assuming JSON format)
    try:
        payment_data = json.loads(body)
    except json.JSONDecodeError as e:
        print(f"Error decoding message: {e}")
        # Consider handling invalid messages (e.g., logging, retrying)
        return  # Exit the callback function if decoding fails

    # Extract relevant payment information from the data
    user_id = payment_data.get('user_id')
    amount = payment_data.get('amount')
    currency = payment_data.get('currency')
    description = payment_data.get('description')

    if None in (user_id, amount, currency, description):
        print(f"Missing required payment data: {payment_data}")
        # Consider handling missing data (e.g., logging, retrying)
        return  # Exit the callback function if required data is missing

    # Call your payment service using a suitable library or API
    # (Replace this placeholder with your actual implementation)
    payment_response = call_payment_service(user_id, amount, currency, description)

    # Process the payment response (e.g., handle success/failure)
    if payment_response['status'] == 'success':
        print(f"Payment successful for user {user_id}")
        # Handle successful payment (e.g., update order status)
    else:
        print(f"Payment failed for user {user_id}: {payment_response['error_message']}")
        # Handle payment failure (e.g., notify user, retry)

def call_payment_service(user_id, amount, currency, description):
    """Placeholder function to simulate calling your payment service.

    Replace this with your actual payment service integration code.
    This function should return a dictionary containing status and error_message (optional).
    """
    # Replace with your actual API call, data processing, and error handling
    return {'status': 'success'}  # Simulate successful payment

# RabbitMQ connection details
connection_parameters = pika.ConnectionParameters('localhost')

try:
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()

    # Define the exchange (can be named differently based on your application)
    exchange_name = 'payment_requests'
    channel.exchange_declare(exchange=exchange_name, exchange_type=ExchangeType.fanout)

    # Create a temporary, exclusive queue for the consumer
    queue = channel.queue_declare(queue='')

    # Bind the queue to the exchange
    channel.queue_bind(exchange=exchange_name, queue=queue.method.queue)

    # Consume messages from the queue and call the callback function
    channel.basic_consume(
        queue=queue.method.queue, auto_ack=True, on_message_callback=on_message_received
    )

    print("Starting consuming messages from 'payment_requests' exchange...")
    channel.start_consuming()

except pika.exceptions.ConnectionClosed as e:
    print(f"Connection error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    # Close the connection (always recommended)
    connection.close()
