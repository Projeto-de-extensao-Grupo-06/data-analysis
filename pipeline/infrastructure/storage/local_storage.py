import pandas as pd
import json
import os
import time
import fnmatch
from typing import List
from pipeline.domain.interfaces import StorageProvider
from pipeline.domain.entities import SolarReading, EnrichedSolarReading, Address

class LocalStorageProvider(StorageProvider):
    """
    Provedor de armazenamento que utiliza o pandas para manipulação de arquivos locais.
    Seguindo regras de Data Governance: MM-YYYY e Timestamps.
    """

    def list_pending_files(self, source_path: str, pattern: str) -> List[str]:
        """
        Lista arquivos recursivamente que dão match no padrão e não estão no .processed local.
        """
        pending = []
        for root, dirs, files in os.walk(source_path):
            processed_log = os.path.join(root, ".processed")
            processed_files = set()
            if os.path.exists(processed_log):
                with open(processed_log, 'r') as f:
                    processed_files = {line.strip() for line in f}

            matches = [f for f in files if fnmatch.fnmatch(f, pattern) and f not in processed_files]
            for f in matches:
                pending.append(os.path.join(root, f))
        
        return pending

    def load_raw(self, source_path: str) -> List[SolarReading]:
        """
        Carrega dados de arquivos CSV. 
        Suporta tanto caminho de arquivo quanto diretório (usando list_pending_files).
        """
        if os.path.isfile(source_path):
             return self._load_csv_file(source_path)
        
        if not os.path.exists(source_path):
            return []

        pending = self.list_pending_files(source_path, "*.csv")
        all_readings = []
        for f in pending:
            all_readings.extend(self._load_csv_file(f))
        return all_readings

    def _load_csv_file(self, file_path: str) -> List[SolarReading]:
        """Helper para carregar um CSV individual."""
        df = pd.read_csv(file_path, sep=';', encoding='utf-8-sig')
        readings = []
        for _, row in df.iterrows():
            monthly = {month: float(row[month]) for month in ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']}
            readings.append(SolarReading(
                id=int(row['ID']), uf=row['UF'], lon=float(row['LON']), lat=float(row['LAT']),
                annual=float(row['ANNUAL']), monthly_data=monthly
            ))
        return readings

    def save_trusted(self, readings: List[SolarReading], target_path: str):
        """
        Salva os dados limpos em formato JSON dentro de uma pasta MM-YYYY com timestamp.
        """
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        date_folder = time.strftime("%m-%Y")
        full_target_path = os.path.join(target_path, date_folder)
        
        if not os.path.exists(full_target_path):
            os.makedirs(full_target_path)

        df = pd.DataFrame([self._reading_to_dict(r) for r in readings])
        file_path = os.path.join(full_target_path, f"solar_data_trusted_{timestamp}.json")
        df.to_json(file_path, orient='records', indent=4, force_ascii=False)
        print(f"[{time.strftime('%H:%M:%S')}] Dados salvos na TRUSTED: {date_folder}/solar_data_trusted_{timestamp}.json")

    def load_trusted(self, source_path: str) -> List[SolarReading]:
        """
        Carrega dados JSON da camada TRUSTED. 
        Suporta arquivo único ou varredura de diretório (respeitando .processed).
        """
        if not os.path.exists(source_path):
            return []

        if os.path.isfile(source_path):
            return self._load_json_file(source_path)

        all_readings = []
        for root, dirs, files in os.walk(source_path):
            processed_log = os.path.join(root, ".processed")
            processed_files = set()
            if os.path.exists(processed_log):
                with open(processed_log, 'r') as f:
                    processed_files = {line.strip() for line in f}

            trusted_files = [f for f in files if f.startswith("solar_data_trusted_") and f.endswith(".json") and f not in processed_files]
            
            for f in trusted_files:
                all_readings.extend(self._load_json_file(os.path.join(root, f)))

        return all_readings

    def _load_json_file(self, file_path: str) -> List[SolarReading]:
        """Helper para carregar um arquivo JSON da Trusted."""
        df = pd.read_json(file_path)
        return [SolarReading(
            id=int(d['id']), uf=d['uf'], lon=float(d['lon']), lat=float(d['lat']),
            annual=float(d['annual']), monthly_data=d['monthly_data']
        ) for d in df.to_dict('records')]

    def save_refined(self, enriched_data: List[EnrichedSolarReading], target_path: str):
        """
        Salva os dados enriquecidos na camada REFINED com MM-YYYY e timestamp.
        """
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        date_folder = time.strftime("%m-%Y")
        full_target_path = os.path.join(target_path, date_folder)

        if not os.path.exists(full_target_path):
            os.makedirs(full_target_path)

        flat_data = []
        for e in enriched_data:
            flat_data.append({
                "id": e.reading.id,
                "lat": e.reading.lat,
                "lon": e.reading.lon,
                "cidade": e.address.city,
                "bairro": e.address.suburb,
                "rua": e.address.road,
                "codigo_postal": e.address.postcode,
                "endereco_completo": e.address.full_address,
                "anual": e.reading.annual,
                **e.reading.monthly_data,
                **{f"media_{k}": v for k, v in e.seasons_avg.items()}
            })

        df = pd.DataFrame(flat_data)
        file_name = f"solar_data_refined_{timestamp}.json"
        file_path = os.path.join(full_target_path, file_name)
        df.to_json(file_path, orient='records', indent=4, force_ascii=False)
        print(f"[{time.strftime('%H:%M:%S')}] Dados salvos na REFINED: {date_folder}/{file_name}")

    def mark_as_processed(self, file_path: str):
        """
        Registra o nome do arquivo no log oculto de processados no diretório do arquivo.
        """
        directory = os.path.dirname(file_path)
        file_name = os.path.basename(source_path)
        processed_log = os.path.join(raw_dir, ".processed")
        
        with open(processed_log, 'a') as f:
            f.write(f"{file_name}\n")

    def _reading_to_dict(self, r: SolarReading) -> dict:
        """Converte uma entidade para dicionário."""
        return {
            "id": r.id,
            "uf": r.uf,
            "lon": r.lon,
            "lat": r.lat,
            "annual": r.annual,
            "monthly_data": r.monthly_data
        }
