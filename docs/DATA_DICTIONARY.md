# Dicionário de Dados - Pipeline de Irradiação Solar

Este documento descreve os campos e transformações em cada camada da pipeline.

## 1. Camada RAW (Entrada)
**Fonte:** `docs/labren-data/tilted_latitude_means_SP.csv`
**Formato:** CSV (Delimitador `;`)

| Campo | Descrição | Tipo |
| :--- | :--- | :--- |
| ID | Identificador único do ponto | Inteiro |
| UF | Unidade da Federação (Estado) | Texto |
| LON | Longitude | Decimal |
| LAT | Latitude | Decimal |
| ANNUAL | Média anual de irradiação | Inteiro |
| JAN...DEC | Médias mensais de irradiação | Inteiro |

## 2. Camada TRUSTED (Limpeza)
**Formato:** JSON

**Transformações:**
- Conversão de CSV para JSON.
- Deduplicação baseada no campo `ID`.
- Tratamento de valores nulos (preenchimento com 0.0 para campos numéricos).
- Padronização de tipos (float para coordenadas e valores de irradiação).

## 3. Camada REFINED (Enriquecimento)
**Formato:** JSON

**Campos Adicionais:**
| Campo | Descrição | Origem |
| :--- | :--- | :--- |
| location.address | Endereço detalhado (Rua, Bairro, Cidade, CEP) | Nominatim API |
| irradiation.seasons | Médias calculadas por estação do ano | Cálculo Interno |

**Cálculo das Estações (Hemisfério Sul):**
- **Summer (Verão):** Dezembro, Janeiro, Fevereiro.
- **Autumn (Outono):** Março, Abril, Maio.
- **Winter (Inverno):** Junho, Julho, Agosto.
- **Spring (Primavera):** Setembro, Outubro, Novembro.
