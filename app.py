from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json

app = Flask(__name__)

# Kafka Configuration
KAFKA_TOPIC = 'your_topic_name'
KAFKA_BROKER = 'localhost:9092'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/')
def index():
    """
    Root endpoint to check if the service is running.
    """
    return "Welcome to the Flask Kafka producer!"


@app.route('/produce', methods=['POST'])
def produce_message():
    """
    Endpoint to produce a message to Kafka.
    Expects JSON data in the request body.
    """
    try:
        # Get JSON data from request
        data = request.json
        if not data:
            return jsonify({'error': 'No data provided'}), 400

        # Send message to Kafka
        producer.send(KAFKA_TOPIC, value=data)
        producer.flush()

        return jsonify({'status': 'Message sent successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


