import os
import threading
import logging
import io
import pandas as pd
from flask import Flask, request, jsonify, render_template
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
#from azure.identity import DefaultAzureCredential
from azure.identity import ManagedIdentityCredential
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

app = Flask(__name__)
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.DEBUG)

# Rate limiter setup
limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    default_limits=["100 per hour"]
)

@app.route('/health')
def health_check():
    return jsonify({'status': 'ok'}), 200

@app.route('/')
def index():
    return render_template('index.html')

# MongoDB Config
mongo_uri = os.getenv('MONGO_URI')
mongo_db_name = os.getenv('MONGO_DB_NAME')
mongo_collection_name = os.getenv('MONGO_COLLECTION_NAME')
mongo_generic_collection = os.getenv('GENERIC_COLLECTION_NAME')
mongo_generic_db_name = os.getenv('MONGO_GENERIC_DB_NAME')

def get_mongo_collection(uri, db_name, collection_name):
    client = MongoClient(uri)
    db = client[db_name]
    return db[collection_name]

# Uses User Delegation SAS via Managed Identity
def generate_sas_url(blob_service_client, account_name, container_name, blob_name, expiry_minutes=15):
    expiry = datetime.utcnow() + timedelta(minutes=expiry_minutes)
    delegation_key = blob_service_client.get_user_delegation_key(datetime.utcnow(), expiry)

    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=container_name,
        blob_name=blob_name,
        user_delegation_key=delegation_key,
        permission=BlobSasPermissions(read=True),
        expiry=expiry
    )

    return f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"

app_container_mapping = {
    'ATnA': {
        'containers': [os.getenv('CONTAINER_ATnA')],
        'account_name': os.getenv('AZURE_STORAGE_ACCOUNT_NAME_ATnA')
    },
    'PALOALTO': {
        'containers': [os.getenv('CONTAINER_PALOALTO_1'), os.getenv('CONTAINER_PALOALTO_2')],
        'account_name': os.getenv('AZURE_STORAGE_ACCOUNT_NAME_PALOALTO')
    },
    'EAROI': {
        'containers': [os.getenv('CONTAINER_EAROI')],
        'account_name': os.getenv('AZURE_STORAGE_ACCOUNT_NAME_EAROI')
    },
    'F5': {
        'containers': [os.getenv('CONTAINER_F5')],
        'account_name': os.getenv('AZURE_STORAGE_ACCOUNT_NAME_F5')
    },
    'CHECKPOINT': {
        'containers': [os.getenv('CONTAINER_CHECKPOINT')],
        'account_name': os.getenv('AZURE_STORAGE_ACCOUNT_NAME_CHECKPOINT')
    },
    'MCE': {
        'containers': [os.getenv('CONTAINER_MCE')],
        'account_name': os.getenv('AZURE_STORAGE_ACCOUNT_NAME_MCE')
    },
    'CISCOIBR': {
        'containers': [os.getenv('CONTAINER_CISCO_IBR')],
        'account_name': os.getenv('AZURE_STORAGE_ACCOUNT_NAME_CISCO_IBR')
    },
    'BYOW': {
        'containers': [os.getenv('CONTAINER_BYOW')],
        'account_name': os.getenv('AZURE_STORAGE_ACCOUNT_NAME_BYOW')
    }
}

@app.route('/download', methods=['POST'])
def download_latest_files():
    app_name = request.json.get('application_name')
    company_id = request.json.get('company_id')

    if not app_name or not company_id:
        return jsonify({'error': 'Missing application_name or company_id'}), 400

    if app_name not in app_container_mapping:
        return jsonify({'error': 'Invalid application name'}), 400

    blob_service_client = get_blob_service_client(app_name)
    if app_name == 'ATnA':
        return handle_atna_download(blob_service_client, app_name, company_id)
    elif app_name in ['F5', 'CHECKPOINT']:
        return handle_f5_checkpoint_download(blob_service_client, app_name, company_id)
    elif app_name == 'PALOALTO':
        return handle_paloalto_download(blob_service_client, app_name, company_id)
    else:
        return handle_latest_blob_download(blob_service_client, app_name, company_id)

def handle_atna_download(blob_service_client, app_name, company_id):
    collection = get_mongo_collection(mongo_uri, mongo_db_name, mongo_collection_name)
    latest_record = collection.find_one({'company_id': company_id}, sort=[('created_on', -1)])

    if not latest_record or 'asset_add_uploads' not in latest_record or not latest_record['asset_add_uploads']:
        return jsonify({'error': 'No upload_id found for the given company_id'}), 404

    upload_id = latest_record['asset_add_uploads'][0]['upload_id']
    container_names = app_container_mapping[app_name]['containers']
    account_name = app_container_mapping[app_name]['account_name']

    for container_name in container_names:
        container_client = blob_service_client.get_container_client(container_name)
        for blob in container_client.list_blobs():
            if upload_id in blob.name:
                download_url = generate_sas_url(blob_service_client, account_name, container_name, blob.name)
                return jsonify({'message': f'SAS link generated for {blob.name}', 'download_url': download_url}), 200

    return jsonify({'error': 'No file found matching upload_id'}), 404

def handle_f5_checkpoint_download(blob_service_client, app_name, company_id):
    collection = get_mongo_collection(mongo_uri, mongo_generic_db_name, mongo_generic_collection)
    record = collection.find_one({"company_id": company_id})

    if not record:
        return jsonify({'error': f'No record found for company_id: {company_id}'}), 404

    files_to_download = []
    if app_name == 'F5':
        files_to_download = [
            record.get("license_asset_summary_workbook_processed"),
            record.get("pricing_retired_workbook_processed"),
            record.get("pricing_active_workbook_processed")
        ]
    elif app_name == 'CHECKPOINT':
        files_to_download = [
            record.get("orders_workbook_processed"),
            record.get("product_list_workbook_processed")
        ]

    files_to_download = [f for f in files_to_download if f]
    if not files_to_download:
        return jsonify({'error': 'No workbook filenames found in MongoDB record'}), 404

    container_names = app_container_mapping[app_name]['containers']
    account_name = app_container_mapping[app_name]['account_name']
    sas_links = []

    for blob_name in files_to_download:
        for container_name in container_names:
            container_client = blob_service_client.get_container_client(container_name)
            blob_list = list(container_client.list_blobs(name_starts_with=blob_name))
            for blob in blob_list:
                if blob.name == blob_name:
                    sas_links.append({
                        'file': blob_name,
                        'download_url': generate_sas_url(blob_service_client, account_name, container_name, blob_name)
                    })
                    break

    if sas_links:
        return jsonify({'message': 'SAS links generated', 'files': sas_links}), 200
    else:
        return jsonify({'error': 'No matching blobs found in containers'}), 404

def handle_paloalto_download(blob_service_client, app_name, company_id):
    collection = get_mongo_collection(mongo_uri, mongo_generic_db_name, mongo_generic_collection)
    record = collection.find_one({"company_id": company_id})

    if not record:
        return jsonify({'error': f'No record found for company_id: {company_id}'}), 404

    connection_details = record.get("connection_details", [])
    if not connection_details or not connection_details[0].get("csp_acct_name"):
        return jsonify({'error': 'No csp_acct_name found'}), 404

    csp_acct_name = connection_details[0].get("csp_acct_name").strip().lower()
    container_names = app_container_mapping[app_name]['containers']
    account_name = app_container_mapping[app_name]['account_name']
    matching_files = []

    for container_name in container_names:
        container_client = blob_service_client.get_container_client(container_name)
        for blob in container_client.list_blobs():
            blob_client = container_client.get_blob_client(blob.name)
            try:
                blob_data = blob_client.download_blob(max_concurrency=1).readall()
                df = pd.read_csv(io.BytesIO(blob_data), dtype=str)
                if 'CSP Acct Name' in df.columns:
                    df['CSP Acct Name'] = df['CSP Acct Name'].astype(str).str.strip().str.lower()
                    if csp_acct_name in df['CSP Acct Name'].values:
                        matching_files.append({
                            'file': blob.name,
                            'download_url': generate_sas_url(blob_service_client, account_name, container_name, blob.name)
                        })
            except Exception as e:
                logging.warning(f"Failed to process blob {blob.name}: {e}")

    if matching_files:
        return jsonify({'message': 'SAS links generated', 'files': matching_files}), 200
    else:
        return jsonify({'message': 'No matching data found'}), 200

def handle_latest_blob_download(blob_service_client, app_name, company_id):
    container_names = app_container_mapping[app_name]['containers']
    account_name = app_container_mapping[app_name]['account_name']

    latest_blob = None
    latest_container = None

    for container_name in container_names:
        container_client = blob_service_client.get_container_client(container_name)
        blobs = list(container_client.list_blobs())
        matching = [b for b in blobs if company_id in b.name]
        if matching:
            most_recent = max(matching, key=lambda b: b.last_modified)
            if not latest_blob or most_recent.last_modified > latest_blob.last_modified:
                latest_blob = most_recent
                latest_container = container_name

    if not latest_blob:
        return jsonify({'message': 'No files found for company_id'}), 200

    try:
        download_url = generate_sas_url(blob_service_client, account_name, latest_container, latest_blob.name)
        return jsonify({'message': f'Download link for {latest_blob.name} generated', 'download_url': download_url}), 200
    except Exception as e:
        logging.error(f"Failed to generate SAS URL: {e}")
        return jsonify({'error': 'Failed to generate download link'}), 500

def get_blob_service_client(app_name):
    info = app_container_mapping[app_name]
    client_id = "9add19d3-89e3-4363-8124-8f9b60da9a4e"
    credential = ManagedIdentityCredential(client_id=client_id)
    return BlobServiceClient(
        account_url=f"https://{info['account_name']}.blob.core.windows.net",
        credential=credential
    )



if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
