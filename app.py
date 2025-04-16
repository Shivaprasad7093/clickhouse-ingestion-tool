from flask import Flask, request, render_template, send_file
from clickhouse_driver import Client
import pandas as pd
import os

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# ClickHouse connector
def get_ch_client(host, user, token, db):
    return Client(host=host, user=user, password=token, database=db)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return "No file part", 400
    file = request.files['file']
    if file.filename == '':
        return "No selected file", 400

    filepath = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(filepath)

    try:
        df = pd.read_csv(filepath)
        columns = list(df.columns)
        rows = df.head(10).values.tolist()  # 👈 Show first 10 rows
        return render_template('preview.html', columns=columns, rows=rows, filename=file.filename)
    except Exception as e:
        return f"Failed to process file: {e}", 500



@app.route('/ingest_flat_to_ch', methods=['POST'])
def flat_to_ch():
    file = request.form['filename']
    host = request.form['host']
    db = request.form['database']
    user = request.form['user']
    token = request.form['token']
    table = request.form['table']

    df = pd.read_csv(os.path.join(UPLOAD_FOLDER, file))
    client = get_ch_client(host, user, token, db)
    
    # Auto-create table (optional)
    cols = ', '.join([f"{col} String" for col in df.columns])
    client.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols}) ENGINE = MergeTree() ORDER BY tuple()")

    # Insert data
    client.execute(f"INSERT INTO {table} VALUES", df.values.tolist())

    return f"Inserted {len(df)} records into {table}."

@app.route('/ch_to_flat', methods=['POST'])
def ch_to_flat():
    host = request.form['host']
    db = request.form['database']
    user = request.form['user']
    token = request.form['token']
    table = request.form['table']

    client = get_ch_client(host, user, token, db)
    result = client.execute(f"SELECT * FROM {table}")
    cols = [desc[0] for desc in client.execute(f"DESCRIBE TABLE {table}")]
    df = pd.DataFrame(result, columns=cols)
    
    filename = f"{table}_export.csv"
    df.to_csv(filename, index=False)
    return send_file(filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)

