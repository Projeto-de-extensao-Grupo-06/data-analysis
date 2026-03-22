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
        Executa o processo de limpeza completo para um caminho.
        """
        readings = self.storage.load_raw(source_path)
        self.execute_readings(readings, target_path)

    def execute_readings(self, readings: List[SolarReading], target_path: str):
        """
        Executa o processo de limpeza em uma lista de entidades.
        """
        seen_ids = set()
        clean_readings = []
        for r in readings:
            if r.id not in seen_ids:
                r.annual = round(r.annual, 1) if r.annual is not None else 0.0
                for month in r.monthly_data:
                    if r.monthly_data[month] is None:
                        r.monthly_data[month] = 0.0
                    else:
                        r.monthly_data[month] = round(r.monthly_data[month], 1)
                
                clean_readings.append(r)
                seen_ids.add(r.id)
        
        if clean_readings:
            self.storage.save_trusted(clean_readings, target_path)
