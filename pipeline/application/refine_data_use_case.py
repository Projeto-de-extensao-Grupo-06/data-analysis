import time
from typing import List, Dict
from pipeline.domain.interfaces import StorageProvider, GeocodingService
from pipeline.domain.entities import SolarReading, Address, EnrichedSolarReading

class RefineDataUseCase:
    """
    Caso de Uso responsável por enriquecer os dados da camada TRUSTED para REFINED.
    """

    def __init__(self, storage: StorageProvider, geocoder: GeocodingService):
        """
        Inicializa o caso de uso com provedores de armazenamento e geocodificação.
        """
        self.storage = storage
        self.geocoder = geocoder

    def execute(self, source_path: str, target_path: str, limit: int = None):
        """
        Executa o processo de refinamento: geocodificação e cálculo de médias sazonais.
        """
        readings = self.storage.load_trusted(source_path)
        
        if limit:
            readings = readings[:limit]
            print(f"[{time.strftime('%H:%M:%S')}] [Refine] Limitando a {limit} registros para teste.")

        enriched_list = []

        for r in readings:
            # Geocodificação Reversa
            geo_data = self.geocoder.reverse_geocode(r.lat, r.lon)
            address = Address()
            if geo_data and 'address' in geo_data:
                addr = geo_data['address']
                address = Address(
                    road=addr.get('road') or addr.get('street'),
                    suburb=addr.get('suburb') or addr.get('neighbourhood'),
                    city=addr.get('city') or addr.get('town') or addr.get('village') or addr.get('municipality'),
                    postcode=addr.get('postcode'),
                    country=addr.get('country')
                )
            
            # Cálculo de Médias por Estação
            seasons = self._calculate_seasons(r.monthly_data)
            
            enriched = EnrichedSolarReading(
                reading=r,
                address=address,
                seasons_avg=seasons
            )
            enriched_list.append(enriched)

        self.storage.save_refined(enriched_list, target_path)

    def _calculate_seasons(self, monthly_data: Dict[str, float]) -> Dict[str, float]:
        """
        Calcula as médias de irradiação por estação do ano.
        """
        return {
            "verao": (monthly_data.get("JAN", 0) + monthly_data.get("FEB", 0) + monthly_data.get("DEC", 0)) / 3,
            "outono": (monthly_data.get("MAR", 0) + monthly_data.get("APR", 0) + monthly_data.get("MAY", 0)) / 3,
            "inverno": (monthly_data.get("JUN", 0) + monthly_data.get("JUL", 0) + monthly_data.get("AUG", 0)) / 3,
            "primavera": (monthly_data.get("SEP", 0) + monthly_data.get("OCT", 0) + monthly_data.get("NOV", 0)) / 3
        }
