import pandas as pd
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def process_raw_to_trusted():
    raw_path = os.getenv("RAW_DATA_PATH", "data/raw")
    trusted_path = os.getenv("TRUSTED_DATA_PATH", "data/trusted")
    
    # Garantir que o diretório trusted existe
    if not os.path.exists(trusted_path):
        os.makedirs(trusted_path)

    # Listar arquivos CSV pendentes
    for root, dirs, files in os.walk(raw_path):
        processed_log = os.path.join(root, ".processed")
        processed_files = set()
        if os.path.exists(processed_log):
            with open(processed_log, 'r') as f:
                processed_files = {line.strip() for line in f}

        csv_files = [f for f in files if f.endswith(".csv") and f not in processed_files]
        
        for file_name in csv_files:
            file_path = os.path.join(root, file_name)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Processando: {file_name}")
            
            try:
                # Carregar CSV
                df = pd.read_csv(file_path, sep=';', encoding='utf-8-sig')
                
                # Deduplicação por ID
                df = df.drop_duplicates(subset=['ID'])
                
                # Tratamento de nulos e arredondamento (1 casa decimal)
                numeric_cols = ['ANNUAL', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
                for col in numeric_cols:
                    df[col] = df[col].fillna(0.0).round(1)
                
                # Padronização de nomes (Inglês)
                df = df.rename(columns={
                    'ID': 'id',
                    'UF': 'state',
                    'LON': 'lon',
                    'LAT': 'lat',
                    'ANNUAL': 'annual'
                })
                
                # Estruturar monthly_data como dicionário
                records = []
                for _, row in df.iterrows():
                    record = {
                        "id": int(row['id']),
                        "state": row['state'],
                        "lon": float(row['lon']),
                        "lat": float(row['lat']),
                        "annual": float(row['annual']),
                        "monthly_data": {m: float(row[m]) for m in ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']}
                    }
                    records.append(record)
                
                # Salvar em JSON com timestamp e pasta MM-YYYY
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                date_folder = datetime.now().strftime("%m-%Y")
                target_dir = os.path.join(trusted_path, date_folder)
                
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir)
                
                target_file = os.path.join(target_dir, f"solar_data_trusted_{timestamp}.json")
                with open(target_file, 'w', encoding='utf-8') as f:
                    json.dump(records, f, indent=4, ensure_ascii=False)
                
                # Marcar como processado
                with open(processed_log, 'a') as f:
                    f.write(f"{file_name}\n")
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Salvo em: {target_file}")
                
            except Exception as e:
                print(f"Erro ao processar {file_name}: {e}")

if __name__ == "__main__":
    process_raw_to_trusted()
