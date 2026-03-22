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

    def execute(self, source_path: str, target_path: str):
        """
        Executa o processo de refinamento: geocodificação e cálculo de médias sazonais.
        """
        readings = self.storage.load_trusted(source_path)
        enriched_list = []

        for r in readings:
            # Geocodificação Reversa
            geo_data = self.geocoder.reverse_geocode(r.lat, r.lon)
            address = Address()
            if geo_data:
                addr = geo_data.get('address', {})
                address = Address(
                    road=addr.get('road') or addr.get('street') or addr.get('pedestrian'),
                    suburb=addr.get('suburb') or addr.get('neighbourhood') or addr.get('hamlet'),
                    city=addr.get('city') or addr.get('city_district') or addr.get('town') or addr.get('village') or addr.get('municipality'),
                    postcode=addr.get('postcode'),
                    country=addr.get('country'),
                    full_address=geo_data.get('display_name')
                )

            if not address.postcode:
                print(f"[{time.strftime('%H:%M:%S')}] [Refine] Pulando {r.id}: CEP (postcode) não encontrado.")
                continue

            if not address.suburb:
                print(f"[{time.strftime('%H:%M:%S')}] [Refine] Pulando {r.id}: Bairro (suburb) não encontrado.")
                continue
            
            # Cálculo de médias por Estação
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
            "summer": round((monthly_data.get("JAN", 0) + monthly_data.get("FEB", 0) + monthly_data.get("DEC", 0)) / 3, 1),
            "autumn": round((monthly_data.get("MAR", 0) + monthly_data.get("APR", 0) + monthly_data.get("MAY", 0)) / 3, 1),
            "winter": round((monthly_data.get("JUN", 0) + monthly_data.get("JUL", 0) + monthly_data.get("AUG", 0)) / 3, 1),
            "spring": round((monthly_data.get("SEP", 0) + monthly_data.get("OCT", 0) + monthly_data.get("NOV", 0)) / 3, 1)
        }
