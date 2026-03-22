import os
from pipeline.domain.interfaces import StorageProvider, ExecutionStrategy, GeocodingService
from pipeline.infrastructure.storage.local_storage import LocalStorageProvider
from pipeline.infrastructure.storage.s3_storage import S3StorageProvider
from pipeline.infrastructure.geocoding.nominatim_service import NominatimService
from pipeline.infrastructure.execution.strategies import PollingExecutionStrategy, OneShotExecutionStrategy

class InfrastructureFactory:
    """
    Fábrica para instanciar as implementações de infraestrutura e estratégias.
    """

    @staticmethod
    def get_storage() -> StorageProvider:
        """
        Retorna a implementação de Storage dependendo do PIPELINE_PROFILE.
        """
        profile = os.getenv("PIPELINE_PROFILE", "dev").lower()
        if profile == "prod":
            return S3StorageProvider()
        return LocalStorageProvider()

    @staticmethod
    def get_execution_strategy() -> ExecutionStrategy:
        """
        Retorna a estratégia de execução (Polling vs One-shot).
        """
        profile = os.getenv("PIPELINE_PROFILE", "dev").lower()
        if profile == "prod":
            return OneShotExecutionStrategy()
        return PollingExecutionStrategy()

    @staticmethod
    def get_geocoder() -> GeocodingService:
        """
        Retorna o serviço de geocodificação.
        """
        user_agent = os.getenv("NOMINATIM_USER_AGENT", "SolarIrradiationPipeline/1.0")
        return NominatimService(user_agent=user_agent)
