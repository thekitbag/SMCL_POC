from flask import Flask, request, jsonify
from processing import process_webhook_data

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    """Handles the Zendesk webhook for new user uploads."""
    try:
        data = request.get_json()
        app.logger.info(f"Webhook received: {data}")

        process_webhook_data(data)

        return jsonify({"message": "Webhook processed successfully"}), 200

    except Exception as e:
        app.logger.error(f"Error processing webhook: {e}")
        return jsonify({"message": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)  # Use debug=True only during development