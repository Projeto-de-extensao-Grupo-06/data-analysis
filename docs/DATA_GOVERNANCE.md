# Data Governance Rules

Este documento define as regras de organização e nomenclatura de dados para as camadas da pipeline.

## 1. Organização de Diretórios

Todas as camadas subsequentes à RAW (TRUSTED e REFINED) devem seguir a organização por **Mês e Ano** do processamento.

- **Padrão de pasta:** `MM-YYYY` (ex: `03-2026`)
- **Motivo:** Facilita o particionamento de dados em buckets S3 e evita que uma única pasta contenha milhares de arquivos, degradando a performance de listagem.

## 2. Nomenclatura de Arquivos (Timestamps)

Para garantir rastreabilidade e permitir múltiplas execuções no mesmo período sem sobreposição destrutiva, os arquivos devem conter um timestamp.

- **Padrão Trusted:** `solar_data_trusted_YYYYMMDD_HHMMSS.json`
- **Padrão Refined:** `solar_data_refined_YYYYMMDD_HHMMSS.json`

## 3. Controle de Processamento (Layer RAW)

A camada RAW deve permanecer **imutável** e **bruta**. 
- Não é permitido mover ou renomear arquivos originais.
- O controle de redundância é feito via arquivo metadata oculto (`.processed`) que registra quais arquivos já foram processados.

## 4. Esquema de Dados

- **TRUSTED:** Dados limpos, tipados e salvos em JSON puro.
- **REFINED:** Dados enriquecidos com geocodificação reversa (OpenStreetMap) e médias sazonais calculadas. Idioma oficial dos campos: **Português**.

## 5. Qualidade dos Dados (Data Quality)

Para garantir que a camada REFINED contenha apenas informações úteis para análise geoespacial:

- **CEP Obrigatório:** Somente registros que possuam `codigo_postal` validado pela geocodificação são persistidos na REFINED.
- **Rastreabilidade:** Cada execução gera um arquivo único (Timestamp), permitindo auditoria de quando e como o dado foi gerado.
- **Processamento Individual:** Diferente da abordagem em lote (batch), cada arquivo na TRUSTED é processado e marcado individualmente, garantindo que falhas em um arquivo não impeçam o processamento de outros.
