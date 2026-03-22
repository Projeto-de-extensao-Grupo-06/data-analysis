from abc import ABC, abstractmethod
from typing import List, Optional
from pipeline.domain.entities import SolarReading, EnrichedSolarReading

class StorageProvider(ABC):
    """
    Interface (Contrato) para provedores de armazenamento de dados.
    """

    @abstractmethod
    def load_raw(self, source_path: str) -> List[SolarReading]:
        """Carrega dados brutos da camada RAW."""
        pass

    @abstractmethod
    def save_trusted(self, readings: List[SolarReading], target_path: str):
        """Salva dados limpos na camada TRUSTED (formato JSON)."""
        pass

    @abstractmethod
    def load_trusted(self, source_path: str) -> List[SolarReading]:
        """Carrega dados da camada TRUSTED."""
        pass

    @abstractmethod
    def save_refined(self, enriched_data: List[EnrichedSolarReading], target_path: str):
        """Salva dados enriquecidos na camada REFINED."""
        pass

    @abstractmethod
    def list_pending_files(self, source_path: str, pattern: str) -> List[str]:
        """Lista arquivos que ainda não foram marcados como processados."""
        pass

    @abstractmethod
    def mark_as_processed(self, file_path: str):
        """Marca o arquivo como processado (ex: move ou log)."""
        pass

class ExecutionStrategy(ABC):
    """
    Interface para estratégias de execução da pipeline.
    Define COMO a pipeline é disparada (Loop vs One-shot).
    """

    @abstractmethod
    def run(self, orchestrator: 'PipelineOrchestrator', job_name: str):
        """Executa o orchestrator seguindo a estratégia específica."""
        pass

class PipelineOrchestrator(ABC):
    """
    Interface para o orquestrador que coordena os Casos de Uso.
    """
    @abstractmethod
    def run_raw_to_trusted(self):
        pass

    @abstractmethod
    def run_trusted_to_refined(self):
        pass

class GeocodingService(ABC):
    """
    Interface para serviços de geocodificação reversa.
    """

    @abstractmethod
    def reverse_geocode(self, lat: float, lon: float) -> Optional[dict]:
        """Obtém informações de endereço a partir de coordenadas."""
        pass
