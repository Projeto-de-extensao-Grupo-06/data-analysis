import time
import threading
import os
from pipeline.domain.interfaces import ExecutionStrategy, PipelineOrchestrator

class PollingExecutionStrategy(ExecutionStrategy):
    """
    Estratégia de execução contínua com Polling e Threads.
    Ideal para ambiente Local (Dev).
    """

    def run(self, orchestrator: PipelineOrchestrator, job_name: str):
        interval = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
        
        if job_name == "raw-to-trusted":
            self._loop(orchestrator.run_raw_to_trusted, interval)
            
        elif job_name == "trusted-to-refined":
            self._loop(orchestrator.run_trusted_to_refined, interval)
            
        elif job_name == "all":
            print(f"[{time.strftime('%H:%M:%S')}] Iniciando Polling Mode (Threading)")
            t1 = threading.Thread(target=self._loop, args=(orchestrator.run_raw_to_trusted, interval), daemon=True)
            t2 = threading.Thread(target=self._loop, args=(orchestrator.run_trusted_to_refined, interval), daemon=True)
            t1.start()
            t2.start()
            
            # Manter a thread principal viva
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass

    def _loop(self, func, interval):
        while True:
            func()
            time.sleep(interval)

class OneShotExecutionStrategy(ExecutionStrategy):
    """
    Estratégia de execução única (On-demand).
    Ideal para ambiente Cloud (Lambda/Glue/Events).
    """

    def run(self, orchestrator: PipelineOrchestrator, job_name: str):
        print(f"[{time.strftime('%H:%M:%S')}] Iniciando One-Shot Execution (Cloud Style)")
        
        if job_name == "raw-to-trusted":
            orchestrator.run_raw_to_trusted()
            
        elif job_name == "trusted-to-refined":
            orchestrator.run_trusted_to_refined()
            
        elif job_name == "all":
            orchestrator.run_raw_to_trusted()
            orchestrator.run_trusted_to_refined()
