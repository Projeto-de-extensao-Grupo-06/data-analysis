# Solar Irradiation Pipeline (Clean Architecture)

Este projeto implementa uma pipeline de processamento de dados de irradiação solar no estado de São Paulo, seguindo os princípios de **Clean Architecture** e o padrão **Strategy**.

## Arquitetura

A aplicação está dividida em camadas para garantir separação de preocupações e testabilidade:

- **Domain**: Entidades de negócio (`SolarReading`, `Address`) e interfaces (contratos).
- **Application**: Casos de uso (`CleanDataUseCase`, `RefineDataUseCase`) que contêm a lógica de transformação.
- **Infrastructure**: Implementações específicas de armazenamento (`LocalStorageProvider`) e serviços externos (`OpenStreetService`).
- **Entrypoints**: Pontos de entrada da aplicação (`main.py`).
- **Data Governance**: Regras de qualidade e organização (`docs/`).

### Estrutura do Projeto
```text
pipeline/
├── domain/             # Entidades e Interfaces (Independente)
├── application/        # Casos de Uso (Regra de Negócio)
├── infrastructure/     # Implementações Técnicas (Desacoplado)
│   ├── storage/        # Provedores (Local, S3)
│   ├── geocoding/      # Serviços (OpenStreetMap/OpenStreet)
│   └── execution/      # Estratégias (Polling, One-Shot)
│   └── factory.py      # Injeção de Dependências
└── main.py             # Entrypoint
```

## Camadas de Dados (Data Lake)

1.  **RAW**: Recebe arquivos CSV brutos (ex: `tilted_latitude_means_SP.csv`).
2.  **TRUSTED**: Dados limpos e convertidos para **JSON**, organizados por `MM-YYYY` e rastreados por `.processed`.
3.  **REFINED**: Dados enriquecidos com endereço (OpenStreet) e métricas sazonais. **Regra mandatória:** Apenas registros com CEP (Postal Code) são persistidos.

Arquivos em TRUSTED e REFINED seguem o padrão de nomenclatura com **Timestamps** para evitar sobreposição.

## Como Executar

### Pré-requisitos
- Python 3.10+

### Instalação

1.  **Criar ambiente virtual:**
    ```bash
    python -m venv venv
    ```

2.  **Ativar o ambiente virtual:**
    -   **Windows (PowerShell):** `.\venv\Scripts\Activate.ps1`
    -   **Linux/macOS:** `source venv/bin/activate`

3.  **Instalar dependências:**
    ```bash
    pip install -r requirements.txt
    ```

### Execução Local (Poller)
O programa monitora a pasta `data/raw` a cada 1 minuto por novos arquivos `.csv`.

```bash
# Iniciar todos os jobs simultaneamente
python -m pipeline.main --job all

# Executar apenas o tratamento RAW -> TRUSTED
python -m pipeline.main --job raw-to-trusted

# Executar apenas o refinamento TRUSTED -> REFINED
python -m pipeline.main --job trusted-to-refined
```

## Estratégia Dev vs Prod
A escolha da arquitetura permite que o `LocalStorageProvider` seja substituído por um `S3StorageProvider` (AWS Glue/S3) apenas alterando a variável de ambiente `PIPELINE_PROFILE`, sem tocar na lógica de negócio.
