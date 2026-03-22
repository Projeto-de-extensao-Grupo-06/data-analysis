import os
import argparse
from dotenv import load_dotenv
from pipeline.infrastructure.factory import InfrastructureFactory
from pipeline.application.orchestrator import SolarPipelineOrchestrator

load_dotenv()

def main():
    """
    Ponto de entrada simplificado (Thin Entrypoint).
    Delegamos COMO rodar para a Strategy e O QUE rodar para o Orchestrator.
    """
    parser = argparse.ArgumentParser(description="Solar Irradiation Pipeline")
    parser.add_argument("--job", choices=["raw-to-trusted", "trusted-to-refined", "all"], 
                        default="all", help="Job a ser executado.")
    args = parser.parse_args()

    storage = InfrastructureFactory.get_storage()
    geocoder = InfrastructureFactory.get_geocoder()
    execution_strategy = InfrastructureFactory.get_execution_strategy()
    orchestrator = SolarPipelineOrchestrator(storage, geocoder)

    try:
        execution_strategy.run(orchestrator, args.job)
    except KeyboardInterrupt:
        print("\nPipeline encerrada pelo usuário.")

if __name__ == "__main__":
    main()
