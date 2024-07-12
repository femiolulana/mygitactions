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
    return "Welcome to the Flask Kafka producer!"

@app.route('/produce', methods=['POST'])
def produce_message():
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
