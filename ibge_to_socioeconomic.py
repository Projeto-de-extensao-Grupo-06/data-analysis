import gzip
import json
import os
import time
import urllib.request
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

UNKNOWN_TEXT = "desconhecido"


def ensure_directory(path):
    os.makedirs(path, exist_ok=True)


def load_processed_files(processed_log):
    if not os.path.exists(processed_log):
        return set()

    with open(processed_log, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def append_processed_file(processed_log, file_name):
    with open(processed_log, "a", encoding="utf-8") as f:
        f.write(f"{file_name}\n")


def safe_text(value, default=UNKNOWN_TEXT):
    if value is None:
        return default

    text = str(value).strip()
    return text if text else default


def safe_float(value, default=None):
    if value in (None, "", "...", "..", "X", "-"):
        return default

    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return default


def read_json_response(response):
    raw_content = response.read()
    if not raw_content:
        return None

    content_encoding = response.headers.get("Content-Encoding", "").lower()
    if content_encoding == "gzip":
        raw_content = gzip.decompress(raw_content)

    return json.loads(raw_content.decode("utf-8"))


def fetch_sidra_values(query_path):
    base_url = "https://apisidra.ibge.gov.br/values"
    url = f"{base_url}{query_path}"

    try:
        time.sleep(0.3)
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "SolarwaySocioeconomic/1.0",
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
            },
        )

        with urllib.request.urlopen(req) as response:
            if response.getcode() != 200:
                return []

            data = read_json_response(response)
            return data if isinstance(data, list) else []

    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Erro SIDRA na consulta {query_path}: {e}")
        return []


def get_last_value(records):
    if not records or len(records) <= 1:
        return None

    for item in reversed(records[1:]):
        value = safe_float(item.get("V"))
        if value is not None:
            return value

    return None


def fetch_income_indicator(ibge_city_code):
    """
    Consulta placeholder de indicador de renda no SIDRA.
    O caminho deve ser ajustado conforme a tabela final escolhida.
    """
    query_path = f"/t/5938/n6/{ibge_city_code}/v/37/p/last?formato=json"
    records = fetch_sidra_values(query_path)
    return {
        "income_avg": get_last_value(records),
        "income_source": "sidra",
        "income_query": query_path,
    }


def fetch_household_indicator(ibge_city_code):
    """
    Consulta placeholder de indicador domiciliar no SIDRA.
    O caminho deve ser ajustado conforme a tabela final escolhida.
    """
    query_path = f"/t/5938/n6/{ibge_city_code}/v/37/p/last?formato=json"
    records = fetch_sidra_values(query_path)
    return {
        "household_indicator_value": get_last_value(records),
        "household_indicator_source": "sidra",
        "household_indicator_query": query_path,
    }


def fetch_urban_indicator(ibge_city_code):
    """
    Consulta placeholder de indicador urbanistico no SIDRA.
    O caminho deve ser ajustado conforme a tabela final escolhida.
    """
    query_path = f"/t/5938/n6/{ibge_city_code}/v/37/p/last?formato=json"
    records = fetch_sidra_values(query_path)
    return {
        "urban_indicator_value": get_last_value(records),
        "urban_indicator_source": "sidra",
        "urban_indicator_query": query_path,
    }


def build_socioeconomic_enrichment(record, indicator_cache):
    ibge_city_code = record.get("ibge_city_code")

    if not ibge_city_code:
        return {
            "income_avg": None,
            "income_source": UNKNOWN_TEXT,
            "income_query": UNKNOWN_TEXT,
            "household_indicator_value": None,
            "household_indicator_source": UNKNOWN_TEXT,
            "household_indicator_query": UNKNOWN_TEXT,
            "urban_indicator_value": None,
            "urban_indicator_source": UNKNOWN_TEXT,
            "urban_indicator_query": UNKNOWN_TEXT,
            "vulnerability_index": None,
            "vulnerability_source": "pendente",
        }

    if ibge_city_code not in indicator_cache:
        indicator_cache[ibge_city_code] = {
            **fetch_income_indicator(ibge_city_code),
            **fetch_household_indicator(ibge_city_code),
            **fetch_urban_indicator(ibge_city_code),
            "vulnerability_index": None,
            "vulnerability_source": "pendente",
        }

    return indicator_cache[ibge_city_code]


def process_ibge_to_socioeconomic():
    ibge_path = os.getenv("IBGE_DATA_PATH", "data/ibge")
    socioeconomic_path = os.getenv("SOCIOECONOMIC_DATA_PATH", "data/socioeconomic")

    ensure_directory(socioeconomic_path)

    for root, _, files in os.walk(ibge_path):
        processed_log = os.path.join(root, ".processed_socioeconomic")
        processed_files = load_processed_files(processed_log)

        ibge_files = [
            f for f in files
            if f.startswith("solar_data_ibge_") and f.endswith(".json") and f not in processed_files
        ]

        if not ibge_files:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Nenhum arquivo IBGE pendente em: {root}")
            continue

        for file_name in ibge_files:
            file_path = os.path.join(root, file_name)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Enriquecendo socioeconomic: {file_name}")

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    records = json.load(f)

                if not records:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Arquivo vazio: {file_name}")
                    append_processed_file(processed_log, file_name)
                    continue

                indicator_cache = {}
                enriched_records = []

                for record in records:
                    socioeconomic_data = build_socioeconomic_enrichment(record, indicator_cache)

                    enriched_record = {
                        **record,
                        **socioeconomic_data,
                        "socioeconomic_processed_at": datetime.now().isoformat(),
                        "socioeconomic_source_file": file_name,
                    }
                    enriched_records.append(enriched_record)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                date_folder = os.path.basename(root)
                target_dir = os.path.join(socioeconomic_path, date_folder)
                ensure_directory(target_dir)

                target_file = os.path.join(target_dir, f"solar_data_socioeconomic_{timestamp}.json")
                with open(target_file, "w", encoding="utf-8") as f:
                    json.dump(enriched_records, f, indent=4, ensure_ascii=False)

                append_processed_file(processed_log, file_name)

                print(f"[{datetime.now().strftime('%H:%M:%S')}] Registros socioeconomic gerados: {len(enriched_records)}")
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Salvo em: {target_file}")

            except Exception as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Erro ao processar {file_name}: {e}")


if __name__ == "__main__":
    process_ibge_to_socioeconomic()
