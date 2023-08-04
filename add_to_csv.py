import csv
file_path = 'orphaned.csv'

def add_to_csv(data):   

    # Open the CSV file in write mode
    with open(file_path, mode='a', newline='') as file:
        # Create a CSV writer object
        writer = csv.writer(file)
        writer.writerows(data)