from celery import Celery
import csv
import os
import time

celery = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

@celery.task
def process_csv(filepath):
    """
    Process the uploaded CSV file to calculate the sum of a column and save the result.
    """
    result_folder = 'results'
    os.makedirs(result_folder, exist_ok=True)
    result_file = os.path.join(result_folder, os.path.basename(filepath).replace('.csv', '_result.csv'))
    time.sleep(5)  # Simulate processing delay

    try:
        with open(filepath, 'r') as file:
            reader = csv.DictReader(file)
            total = 0

            for row in reader:
                try:
                    # Assuming the CSV has a column named 'value'
                    total += float(row.get('value', 0))
                except ValueError:
                    continue

        # Save the result to a new CSV file
        with open(result_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Sum'])
            writer.writerow([total])

        return result_file
    except Exception as e:
        return str(e)
