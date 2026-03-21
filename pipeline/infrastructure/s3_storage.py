import os
import boto3
from typing import List
from pipeline.domain.interfaces import StorageProvider
from pipeline.domain.entities import SolarReading, EnrichedSolarReading

class S3StorageProvider(StorageProvider):
    """
    Implementação de Storage para ambiente AWS (S3).
    Utilizada quando PIPELINE_PROFILE=prod.
    """

    def __init__(self, bucket_name: str = None):
        self.bucket = bucket_name or os.getenv("AWS_S3_BUCKET")
        self.s3 = boto3.client('s3')

    def load_raw(self, source_path: str) -> List[SolarReading]:
        """
        Simula o carregamento de dados do S3 RAW.
        """
        print(f"[AWS S3] Buscando arquivos em s3://{self.bucket}/{source_path}")
        # Na vida real, usaríamos boto3 para ler o CSV e pandas.read_csv(obj['Body'])
        # Por enquanto, retornamos vazio para demonstrar a troca de classe.
        return []

    def save_trusted(self, readings: List[SolarReading], target_path: str):
        """
        Simula o upload para o S3 TRUSTED.
        """
        print(f"[AWS S3] Uploading {len(readings)} registros para s3://{self.bucket}/{target_path}")

    def load_trusted(self, source_path: str) -> List[SolarReading]:
        """
        Simula a carga do S3 TRUSTED.
        """
        return []

    def mark_as_processed(self, source_path: str):
        """
        Simula a marcação de metadados no S3 (Ex: Tagging ou prefixo).
        """
        print(f"[AWS S3] Marcando s3://{self.bucket}/{source_path} como processado.")

    def _reading_to_dict(self, r: SolarReading) -> dict:
        return {}
