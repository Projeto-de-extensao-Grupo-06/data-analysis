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
Nova etapa implementada para transformar a camada `refined` em uma visao de negocio.

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

Status:
- implementado
- gera arquivos na camada `data/business`

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

## 6. Diferenca Entre as Camadas

### `refined`
Camada de enriquecimento tecnico e geografico.

O que representa:
- cada registro ainda e um ponto geografico individual
- cada ponto possui coordenadas e informacoes territoriais associadas

Objetivo:
- responder onde esta o ponto
- informar cidade, bairro, CEP e irradiacao daquele registro

Uso principal:
- mapas
- geolocalizacao
- analise territorial ponto a ponto

### `business`
Camada de agregacao comercial e analitica.

O que representa:
- varios pontos da camada `refined` sao agrupados por `state`, `city`, `suburb` e `postcode`
- o foco deixa de ser o ponto individual e passa a ser o territorio

Objetivo:
- responder qual e o comportamento medio de irradiacao de um bairro/regiao
- indicar quantos pontos sustentam aquele agrupamento

Uso principal:
- leitura comercial
- ranking de bairros
- preparacao para score de potencial

### `ibge`
Camada de enriquecimento institucional e socioeconomico.

O que representa:
- a base `business` passa a receber uma chave oficial de integracao com dados publicos
- normalmente essa chave sera o codigo IBGE do municipio

Objetivo:
- permitir cruzamento com bases oficiais de renda, vulnerabilidade e indicadores censitarios
- preparar a base para o calculo futuro do `solar_lead_score`

Uso principal:
- integracao com dados do IBGE
- enriquecimento socioeconomico
- etapa preparatoria para scoring

Observacao de implementacao:
- o enriquecimento IBGE deve buscar os municipios por UF e fazer o match localmente pelo nome da cidade
- essa abordagem e mais estavel do que consultar a API diretamente por nome do municipio

### Por que o dado fica melhor apos `business_to_ibge.py`
Antes da camada `ibge`, o registro da camada `business` ainda depende principalmente de campos textuais como cidade e estado.

Isso funciona bem para leitura humana, mas nao e o ideal para integracao com bases publicas, pois nomes podem variar, conter acentos, abreviacoes ou diferencas de padronizacao.

Depois do enriquecimento com IBGE, o registro passa a ter uma chave oficial de identificacao do municipio.

Principal ganho:
- o dado deixa de ser apenas um agrupamento comercial interno
- passa a ser um registro com referencia oficial para cruzamento com outras bases publicas

Beneficios praticos:
- melhora a confiabilidade de joins com bases externas
- reduz ambiguidade no nome da cidade
- prepara a base para renda, vulnerabilidade social e futuros indicadores
- facilita a criacao do `solar_lead_score`

### Novos campos adicionados pela camada `ibge`

`ibge_city_code`
- codigo oficial do municipio no IBGE
- principal chave de integracao com outras bases oficiais

`ibge_city_name`
- nome oficial do municipio encontrado na base do IBGE
- usado para confirmar se o match foi feito corretamente

`ibge_state_name`
- nome do estado associado ao municipio identificado
- melhora legibilidade e validacao do enriquecimento

`ibge_state_acronym`
- sigla oficial da unidade federativa
- util para padronizacao e integracao futura

`ibge_match_status`
- informa se o cruzamento com o IBGE foi realizado com sucesso
- exemplos:
  - `matched`
  - `not_found`

## 7. Proximo Objetivo de Negocio

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

## 8. Proximo Passo Tecnico

### Cruzamento com base territorial do IBGE
Agora que a camada `business` foi criada, o proximo passo e enriquecer os bairros com informacoes territoriais e socioeconomicas.

Objetivo:
- associar cada bairro/cidade/CEP a uma unidade geografica que permita cruzamento com dados do IBGE

Direcao recomendada:
- usar malha territorial ou setor censitario do IBGE como base de referencia
- preparar a camada `business` para receber indicadores como renda, perfil domiciliar e vulnerabilidade

Resultado esperado:
- base pronta para criacao futura do `solar_lead_score`

## 9. Proximos Passos

### 1. Enriquecer com indicadores socioeconomicos
Depois da camada `ibge`, o passo seguinte e usar o `ibge_city_code` para cruzar a base com indicadores externos.

Indicadores sugeridos:
- renda media ou proxy de renda
- vulnerabilidade social
- perfil domiciliar
- caracteristicas urbanas e residenciais

Objetivo:
- adicionar contexto socioeconomico ao potencial solar ja calculado

Implementacao recomendada:
- criar uma nova etapa chamada `ibge_to_socioeconomic.py`
- usar o `ibge_city_code` como chave principal de integracao
- priorizar, em um primeiro momento, indicadores no nivel municipal

Fontes sugeridas:
- IBGE SIDRA para indicadores de renda e domicilios
- IBGE Censo 2022 para caracteristicas urbanas e residenciais
- Ipea/Atlas da Vulnerabilidade Social para vulnerabilidade social

Resultado esperado:
- nova camada com indicadores socioeconomicos prontos para composicao do score comercial

Status atual:
- etapa `ibge_to_socioeconomic.py` criada
- estrutura pronta para ler `data/ibge` e gerar `data/socioeconomic`
- integracoes SIDRA ainda devem ser refinadas com a definicao final das tabelas/variaveis

### 2. Criar uma camada de scoring
Depois do enriquecimento socioeconomico, deve ser criada uma nova etapa para gerar um indice de potencial comercial por territorio.

Sugestao de nome:
- `ibge_to_scoring.py`
- ou `business_scoring.py`

Objetivo:
- gerar um `solar_lead_score` para cada bairro/regiao

Sugestao inicial de composicao do score:
- 40% irradiacao solar
- 30% renda ou proxy socioeconomica
- 20% perfil residencial horizontal
- 10% baixa vulnerabilidade social

### 3. Produzir uma saida pronta para negocio
Apos o score, a pipeline deve gerar uma camada final pronta para uso comercial.

Possibilidades:
- ranking de bairros
- lista de territorios prioritarios
- base pronta para dashboard
- insumo para campanhas de marketing ou prospeccao

Objetivo:
- transformar a pipeline em uma ferramenta de apoio real para decisao comercial

### 4. Evoluir a granularidade territorial
No curto prazo, o municipio e uma boa chave oficial para integracao.

No medio prazo, a base pode evoluir para:
- setores censitarios
- malhas territoriais mais detalhadas
- cruzamentos mais proximos da realidade do bairro

Objetivo:
- aumentar a precisao do score
- reduzir generalizacao excessiva no nivel municipal
