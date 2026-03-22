import urllib.request
import json
import time
from typing import Optional
from pipeline.domain.interfaces import GeocodingService

class OpenStreetService(GeocodingService):
    """
    Serviço de geocodificação reversa utilizando a API OpenStreetMap.
    """

    def __init__(self, user_agent: str = "SolarIrradiationPipeline/1.0"):
        """
        Inicializa o serviço com um User-Agent.
        """
        self.user_agent = user_agent
        self.base_url = "https://nominatim.openstreetmap.org/reverse?format=json"

    def reverse_geocode(self, lat: float, lon: float) -> Optional[dict]:
        """
        Solicita informações de endereço para o OpenStreetMap.
        Nota: Inclui um delay simples para respeitar os limites de taxa (1 req/sec).
        """
        url = f"{self.base_url}&lat={lat}&lon={lon}"
        headers = {'User-Agent': self.user_agent}

        try:
            time.sleep(1.1) 
            
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req) as response:
                if response.getcode() == 200:
                    data = json.loads(response.read().decode('utf-8'))
                    if data and 'display_name' in data:
                        print(f"[{time.strftime('%H:%M:%S')}] [Geo] Sucesso: {data['display_name'][:50]}...")
                    return data
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] [Geo] Erro na requisição: {e}")
            return None
        return None
