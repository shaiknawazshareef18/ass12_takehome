from flask import Flask, request, jsonify, url_for, send_file
from flask_socketio import SocketIO
import os
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
from celery import Celery

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY')

# Configure directories
UPLOAD_FOLDER = 'uploads'
RESULT_FOLDER = 'results'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULT_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['RESULT_FOLDER'] = RESULT_FOLDER

# Configure Celery
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

# Import tasks
from tasks import process_csv

# Initialize SocketIO
socketio = SocketIO(app, async_mode='eventlet')

@app.route('/upload', methods=['POST'])
def upload_file():
    """
    API endpoint to upload a CSV file.
    """
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    if file and file.filename.endswith('.csv'):
        # Save the file to the upload folder
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        # Start the Celery task
        task = process_csv.apply_async(args=[filepath])
        return jsonify({
            'message': 'File uploaded successfully',
            'task_id': task.id,
            'status_url': url_for('get_status', task_id=task.id, _external=True)
        }), 202
    else:
        return jsonify({'error': 'Only CSV files are allowed'}), 400

@app.route('/status/<task_id>', methods=['GET'])
def get_status(task_id):
    """
    API endpoint to check the status of a background task.
    """
    task = process_csv.AsyncResult(task_id)
    if task.state == 'PENDING':
        response = {'state': task.state, 'status': 'Pending...'}
    elif task.state != 'FAILURE':
        response = {'state': task.state, 'status': task.info or 'Processing...'}
        if task.state == 'SUCCESS':
            response['result_file'] = url_for('download_result', task_id=task_id, _external=True)
    else:
        response = {'state': task.state, 'status': str(task.info)}  # Error details
    return jsonify(response)

@app.route('/result/<task_id>', methods=['GET'])
def download_result(task_id):
    """
    API endpoint to download the processed result file.
    """
    task = process_csv.AsyncResult(task_id)
    if task.state == 'SUCCESS':
        result_file = task.result
        if os.path.exists(result_file):
            return send_file(result_file, as_attachment=True)
        else:
            return jsonify({'error': 'Result file not found'}), 404
    else:
        return jsonify({'error': 'Task is not completed yet'}), 400

# Run the Flask app
if __name__ == '__main__':
    socketio.run(app)
