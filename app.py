import pandas as pd
from flask import Flask, request, jsonify
from processing import process_webhook_data
import logging
import os

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('error.log'),  # Update with your path
        logging.StreamHandler()  # Outputs to console
    ]
)

# Determine if running locally or on PythonAnywhere
IS_LOCAL = not os.path.exists('/home/thekitbag/SMCL_POC')

@app.route('/webhook', methods=['POST', 'GET']) # Allow GET requests for local testing
def handle_webhook():
    try:
        if IS_LOCAL:  # Local testing
            app.logger.info("Running locally, using test CSV")
            try:
                df = pd.read_csv('Canada_example_SMCL.csv', skipinitialspace=True, engine="python")
                result = process_webhook_data(df, testing=True)
                if result:
                    return jsonify({"message": "Test CSV processed successfully"}), 200
                else:
                    return jsonify({"message": "Error processing CSV"}), 500
            except Exception as e:
                logging.exception("Error reading or processing CSV")
                return jsonify({"message": "Error reading or processing CSV"}), 500

        else:  # Running on PythonAnywhere
            data = request.get_json()
            app.logger.info(f"Webhook received: {data}")
            process_webhook_data(data)
            return jsonify({"message": "Webhook processed successfully"}), 200

    except Exception as e:
        logging.error(f"Error processing webhook: {e}", exc_info=True)
        return jsonify({"message": "Internal server error"}), 500


if __name__ == '__main__':
    app.run(debug=True, port=8010)