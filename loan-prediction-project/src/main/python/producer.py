from kafka import KafkaProducer
import json
import time
import csv

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

CSV_FILE = 'src/main/python/loan_data.csv'

def parse_row(row):
    """Convert CSV row to clean dict"""
    return {
        'application_id': row['Loan_ID'],
        'gender':          row['Gender'] if row['Gender'] else 'Male',
        'married':         row['Married'] if row['Married'] else 'Yes',
        'dependents':      int(row['Dependents'].replace('3+', '3')) if row['Dependents'] else 0,
        'education':       row['Education'] if row['Education'] else 'Graduate',
        'self_employed':   row['Self_Employed'] if row['Self_Employed'] else 'No',
        'applicant_income':    float(row['ApplicantIncome']) if row['ApplicantIncome'] else 0.0,
        'coapplicant_income':  float(row['CoapplicantIncome']) if row['CoapplicantIncome'] else 0.0,
        'loan_amount':         float(row['LoanAmount']) if row['LoanAmount'] else 0.0,
        'loan_term':           float(row['Loan_Amount_Term']) if row['Loan_Amount_Term'] else 360.0,
        'credit_history':      float(row['Credit_History']) if row['Credit_History'] else 1.0,
        'property_area':       row['Property_Area'] if row['Property_Area'] else 'Urban',
        'loan_status':         1 if row['Loan_Status'] == 'Y' else 0,
        'timestamp':           time.time()
    }

print("Reading from loan_data.csv and sending to Kafka...")
print("Press Ctrl+C to stop\n")

count = 0
with open(CSV_FILE, newline='') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

# Keep looping through CSV to simulate continuous stream
while True:
    for row in rows:
        application = parse_row(row)
        producer.send('loan_applications', value=application)
        count += 1
        print(f"Sent #{count} | ID: {application['application_id']} | "
              f"Income: {application['applicant_income']} | "
              f"Status: {'APPROVED' if application['loan_status'] == 1 else 'DENIED'}")
        time.sleep(0.5)  # send every 0.5 seconds

    print("\n--- Completed one pass through CSV, restarting ---\n")
    time.sleep(2)
