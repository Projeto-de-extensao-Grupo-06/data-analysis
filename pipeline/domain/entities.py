from dataclasses import dataclass, field
from typing import Dict, Optional

@dataclass
class SolarReading:
    """
    Entidade que representa uma medição de irradiação solar em um ponto geográfico.
    
    Atributos:
        id: Identificador único da medição.
        uf: Estado (Unidade da Federação).
        lon: Longitude.
        lat: Latitude.
        annual: Média anual de irradiação.
        monthly_data: Dicionário contendo as médias mensais (JAN-DEC).
    """
    id: int
    uf: str
    lon: float
    lat: float
    annual: float
    monthly_data: Dict[str, float] = field(default_factory=dict)

@dataclass
class Address:
    """
    Entidade que representa o endereço enriquecido via geocodificação reversa.
    """
    road: Optional[str] = None
    suburb: Optional[str] = None
    city: Optional[str] = None
    postcode: Optional[str] = None
    country: Optional[str] = None
    full_address: Optional[str] = None

@dataclass
class EnrichedSolarReading:
    """
    Entidade que combina os dados de irradiação com informações de endereço.
    """
    reading: SolarReading
    address: Address
    seasons_avg: Dict[str, float] = field(default_factory=dict)
