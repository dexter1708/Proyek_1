import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from flask import Flask, jsonify
from dotenv import load_dotenv
from flask_cors import CORS  

load_dotenv()

app = Flask(__name__)


CORS(app)

db_config = {
    'app_db3': {
        'host': 'localhost',
        'user': os.getenv('MYSQL_USER3'),
        'password': os.getenv('MYSQL_PASSWORD3'),
        'database': os.getenv('MYSQL_DATABASE3'),
        'port': 3309
    },
    'app_db2': {
        'host': 'localhost',
        'user': os.getenv('MYSQL_USER2'),
        'password': os.getenv('MYSQL_PASSWORD2'),
        'database': os.getenv('MYSQL_DATABASE2'),
        'port': 3308
    }
}

def fetch_data_from_db3():
    config = db_config['app_db3']
    try:
        connection = mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            port=config['port']
        )
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT * FROM sampel") 
            data = cursor.fetchall()
            return pd.DataFrame(data)
    except Error as e:
        print(f"Error fetching data from app_db3: {str(e)}")
        return pd.DataFrame()

def fetch_data_from_db2():
    config = db_config['app_db2']
    try:
        connection = mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            port=config['port']
        )
        if connection.is_connected():
            print("Successfully connected to app_db3")
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT * FROM sampel2")  
            data = cursor.fetchall()
            return pd.DataFrame(data)
    except Error as e:
        print(f"Error fetching data from app_db2: {str(e)}")
        return pd.DataFrame()

def fetch_combined_data():
    df_db3 = fetch_data_from_db3()
    df_db2 = fetch_data_from_db2()
    
    if df_db3.empty or df_db2.empty:
        print("One of the DataFrames is empty. Cannot combine data.")
        return pd.DataFrame()
    
    combined_df = pd.merge(df_db3, df_db2, on='no_pokok', how='inner')
    result_df = combined_df[['nama', 'no_pokok', 'nama_jabatan', 'kode_unit', 'kontrak_kerja','status']]
    
    return result_df

@app.route('/api/app_db3', methods=['GET'])
def get_data_db1():
    data = fetch_data_from_db3()
    if data.empty:
        return jsonify({"error": "Tidak ada data dari app_db1"}), 404
    return jsonify(data.to_dict(orient='records'))

@app.route('/api/app_db2', methods=['GET'])
def get_data_db2():
    data = fetch_data_from_db2()
    if data.empty:
        return jsonify({"error": "Tidak ada data dari app_db2"}), 404
    return jsonify(data.to_dict(orient='records'))

@app.route('/api/combined_data', methods=['GET'])
def get_combined_data():
    result = fetch_combined_data()
    
    if not result.empty:
        return jsonify(result.to_dict(orient='records'))
    else:
        return jsonify({"error": "Tidak ada data yang cocok"}), 404

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=3000)
   
