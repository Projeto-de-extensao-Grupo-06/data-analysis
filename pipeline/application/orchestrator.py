import os
import time
from pipeline.domain.interfaces import PipelineOrchestrator, StorageProvider, GeocodingService
from pipeline.application.clean_data_use_case import CleanDataUseCase
from pipeline.application.refine_data_use_case import RefineDataUseCase

class SolarPipelineOrchestrator(PipelineOrchestrator):
    """
    Orquestrador que coordena os Casos de Uso sem se preocupar com a forma de execução (Loop/Event).
    """

    def __init__(self, storage: StorageProvider, geocoder: GeocodingService):
        self.storage = storage
        self.geocoder = geocoder
        self.clean_use_case = CleanDataUseCase(storage)
        self.refine_use_case = RefineDataUseCase(storage, geocoder)
        
        self.raw_dir = os.getenv("RAW_DATA_PATH", "data/raw")
        self.trusted_dir = os.getenv("TRUSTED_DATA_PATH", "data/trusted")
        self.refined_dir = os.getenv("REFINED_DATA_PATH", "data/refined")

    def run_raw_to_trusted(self):
        """
        Coordena a limpeza de arquivos RAW pendentes.
        """
        try:
            pending_files = self.storage.list_pending_files(self.raw_dir, "*.csv")
            
            for file_path in pending_files:
                print(f"[{time.strftime('%H:%M:%S')}] [Orchestrator] Processando RAW: {os.path.basename(file_path)}")
                readings = self.storage.load_raw(file_path)
                self.clean_use_case.execute_readings(readings, self.trusted_dir)
                self.storage.mark_as_processed(file_path)
                
            if pending_files:
                print(f"[{time.strftime('%H:%M:%S')}] [Orchestrator] RAW->TRUSTED concluído.")
        except Exception as e:
            print(f"Erro no Orchestrator (RAW->TRUSTED): {e}")

    def run_trusted_to_refined(self):
        """
        Coordena o refinamento de arquivos TRUSTED pendentes.
        """
        try:
            pending_files = self.storage.list_pending_files(self.trusted_dir, "solar_data_trusted_*.json")
            
            for file_path in pending_files:
                print(f"[{time.strftime('%H:%M:%S')}] [Orchestrator] Refinando: {os.path.basename(file_path)}")
                self.refine_use_case.execute(file_path, self.refined_dir)
                self.storage.mark_as_processed(file_path)

            if pending_files:
                print(f"[{time.strftime('%H:%M:%S')}] [Orchestrator] TRUSTED->REFINED concluído.")
        except Exception as e:
            print(f"Erro no Orchestrator (TRUSTED->REFINED): {e}")
