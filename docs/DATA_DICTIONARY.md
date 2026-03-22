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

**Campos Oficiais:**
| Campo | Descrição | Origem |
| :--- | :--- | :--- |
| id | Identificador original | RAW |
| lat / lon | Coordenadas Geográficas | RAW |
| cidade | Cidade / Município / Vila | OpenStreet API |
| bairro | Bairro / Distrito / Vilarejo | OpenStreet API |
| rua | Nome da via / Estrada | OpenStreet API |
| codigo_postal | CEP (Obrigatório) | OpenStreet API |
| endereco_completo| String completa do endereço | OpenStreet API |
| anual | Média anual de irradiação | RAW |
| media_verao | Média (Jan, Fev, Dez) | Cálculo |
| media_outono | Média (Mar, Abr, Mai) | Cálculo |
| media_inverno | Média (Jun, Jul, Ago) | Cálculo |
| media_primavera| Média (Set, Out, Nov) | Cálculo |

**Regra de Ouro:** Registros sem `codigo_postal` são descartados desta camada.
