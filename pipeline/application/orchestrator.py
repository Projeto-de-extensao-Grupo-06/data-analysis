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
        Executa uma rodada de limpeza de novos arquivos na pasta RAW.
        """
        try:
            readings = self.storage.load_raw(self.raw_dir)
            if readings:
                self.clean_use_case.execute(self.raw_dir, self.trusted_dir)
                
                new_files = [f for f in os.listdir(self.raw_dir) if f.endswith(".csv")]
                for f in new_files:
                    self.storage.mark_as_processed(os.path.join(self.raw_dir, f))
                
                print(f"[{time.strftime('%H:%M:%S')}] [Orchestrator] RAW->TRUSTED concluído.")
        except Exception as e:
            print(f"Erro no Orchestrator (RAW->TRUSTED): {e}")

    def run_trusted_to_refined(self):
        """
        Executa o refinamento. Varre a TRUSTED, processa cada arquivo novo e marca como processado.
        """
        try:
            profile = os.getenv("PIPELINE_PROFILE", "dev").lower()
            limit = 10 if profile == "dev" else None
            
            count = 0
            for root, dirs, files in os.walk(self.trusted_dir):
                processed_log = os.path.join(root, ".processed")
                processed_files = set()
                if os.path.exists(processed_log):
                    with open(processed_log, 'r') as f:
                        processed_files = {line.strip() for line in f}

                new_trusted = [f for f in files if f.startswith("solar_data_trusted_") and f.endswith(".json") and f not in processed_files]
                
                for f in new_trusted:
                    file_path = os.path.join(root, f)
                    print(f"[{time.strftime('%H:%M:%S')}] [Orchestrator] Refinando arquivo: {f}")
                    
                    self.refine_use_case.execute(file_path, self.refined_dir, limit=limit)
                    
                    self.storage.mark_as_processed(file_path)
                    count += 1

            if count > 0:
                print(f"[{time.strftime('%H:%M:%S')}] [Orchestrator] TRUSTED->REFINED concluído ({count} arquivos).")
                
        except Exception as e:
            print(f"Erro no Orchestrator (TRUSTED->REFINED): {e}")
