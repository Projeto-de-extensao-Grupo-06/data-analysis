from typing import List
from pipeline.domain.interfaces import StorageProvider
from pipeline.domain.entities import SolarReading

class CleanDataUseCase:
    """
    Caso de Uso responsável por limpar e padronizar os dados da camada RAW para TRUSTED.
    """

    def __init__(self, storage: StorageProvider):
        """
        Inicializa o caso de uso com um provedor de armazenamento.
        """
        self.storage = storage

    def execute(self, source_path: str, target_path: str):
        """
        Executa o processo de limpeza: leitura, deduplicação, tratamento de nulos e persistência.
        """
        # Carregamento
        readings = self.storage.load_raw(source_path)
        
        # Deduplicação por ID
        seen_ids = set()
        clean_readings = []
        for r in readings:
            if r.id not in seen_ids:
                # Tratamento de nulos
                r.annual = r.annual if r.annual is not None else 0.0
                for month in r.monthly_data:
                    if r.monthly_data[month] is None:
                        r.monthly_data[month] = 0.0
                
                clean_readings.append(r)
                seen_ids.add(r.id)
        
        # Persistência na camada TRUSTED
        self.storage.save_trusted(clean_readings, target_path)
