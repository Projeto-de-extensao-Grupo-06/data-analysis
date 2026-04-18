# Implementation Notes

Este documento registra as evolucoes da pipeline de dados do projeto Solarway, com foco em explicar de forma sucinta o objetivo de cada etapa e o motivo das novas implementacoes.

## 1. ETL Base

### `raw_to_trusted.py`
Responsavel por transformar a base bruta (`raw`) em uma camada confiavel (`trusted`).

O que faz:
- le arquivos CSV da camada `raw`
- remove duplicidades por `ID`
- trata valores nulos em campos numericos
- padroniza nomes de colunas
- converte os dados para JSON
- salva os arquivos na camada `trusted`

Objetivo:
- garantir que os dados brutos fiquem limpos e estruturados para a proxima etapa

### `trusted_to_refined.py`
Responsavel por enriquecer os dados da camada `trusted` e gerar a camada `refined`.

O que faz:
- le os arquivos JSON da camada `trusted`
- realiza geocodificacao reversa com OpenStreetMap/Nominatim
- obtem dados territoriais como cidade, bairro e CEP
- calcula medias sazonais da irradiacao solar
- salva a saida na camada `refined`

Objetivo:
- transformar coordenadas geograficas em contexto territorial util para analise

## 2. Controle de Processamento

### Arquivo `.processed`
Arquivo de controle usado para registrar quais arquivos ja foram processados em cada etapa.

Como funciona:
- cada etapa da pipeline le o `.processed`
- se o nome do arquivo ja estiver registrado, ele nao e processado novamente
- ao final do processamento com sucesso, o nome do arquivo e adicionado ao `.processed`

Objetivo:
- evitar reprocessamento desnecessario
- impedir geracao duplicada de arquivos
- permitir execucao incremental da pipeline

Observacao para ambiente `dev`:
- para testar novamente um mesmo arquivo, pode ser necessario limpar ou editar manualmente o `.processed`

## 3. Nova Camada Analitica

### `refined_to_business.py`
Nova etapa proposta para transformar a camada `refined` em uma visao de negocio.

O que faz:
- le os arquivos da camada `refined`
- agrupa os registros por:
  - estado
  - cidade
  - bairro
  - CEP
- calcula medias de irradiacao anual, mensal e sazonal
- conta quantos pontos existem em cada agrupamento
- salva a saida na camada `business`

Objetivo:
- criar uma base mais util para decisoes comerciais
- preparar os dados para ranking de bairros com potencial de compra de energia solar

## 4. Camada `business`

### Finalidade
A camada `business` nao substitui a ETL original.

Ela existe para:
- agregar os dados da camada `refined`
- transformar dados tecnicos em indicadores territoriais
- facilitar cruzamentos futuros com renda, imoveis, IPTU ou outros dados socioeconomicos

Exemplo de uso:
- identificar bairros com maior incidencia solar e maior potencial de compra

## 5. Fluxo Atual da Pipeline

### Ordem das etapas
1. `raw_to_trusted.py`
2. `trusted_to_refined.py`
3. `refined_to_business.py`

Resumo:
- `raw`: dado bruto
- `trusted`: dado limpo
- `refined`: dado enriquecido geograficamente
- `business`: dado agregado para analise comercial

## 6. Proximo Objetivo de Negocio

### Identificar bairros com potencial de compra de energia solar

Estrategia atual:
- consolidar a base por bairro + cidade + CEP
- gerar indicadores medios por territorio
- preparar a base para enriquecimentos futuros com:
  - renda
  - perfil imobiliario
  - vulnerabilidade social
  - dados municipais
  - eventualmente IPTU, quando disponivel

Objetivo final:
- criar um ranking territorial de bairros com melhor potencial comercial para energia solar
