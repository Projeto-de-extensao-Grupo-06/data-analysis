import json
import os
import time
import urllib.request
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def reverse_geocode(lat, lon, user_agent):
    """Solicita informações de endereço para o OpenStreetMap."""
    base_url = "https://nominatim.openstreetmap.org/reverse?format=json"
    url = f"{base_url}&lat={lat}&lon={lon}"
    headers = {'User-Agent': user_agent}

    try:
        time.sleep(1.1)  # Respeitar limite de taxa (1 req/sec)
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as response:
            if response.getcode() == 200:
                data = json.loads(response.read().decode('utf-8'))
                if data and 'display_name' in data:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Geo] Sucesso: {data['display_name'][:50]}...")
                return data
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Geo] Erro: {e}")
        return None
    return None

def calculate_seasons(monthly_data):
    """Calcula as médias sazonais e arredonda para 1 casa decimal."""
    return {
        "summer": round((monthly_data.get("JAN", 0) + monthly_data.get("FEB", 0) + monthly_data.get("DEC", 0)) / 3, 1),
        "autumn": round((monthly_data.get("MAR", 0) + monthly_data.get("APR", 0) + monthly_data.get("MAY", 0)) / 3, 1),
        "winter": round((monthly_data.get("JUN", 0) + monthly_data.get("JUL", 0) + monthly_data.get("AUG", 0)) / 3, 1),
        "spring": round((monthly_data.get("SEP", 0) + monthly_data.get("OCT", 0) + monthly_data.get("NOV", 0)) / 3, 1)
    }

def process_trusted_to_refined():
    trusted_path = os.getenv("TRUSTED_DATA_PATH", "data/trusted")
    refined_path = os.getenv("REFINED_DATA_PATH", "data/refined")
    user_agent = os.getenv("NOMINATIM_USER_AGENT", "SolarIrradiationPipeline/1.0")

    if not os.path.exists(refined_path):
        os.makedirs(refined_path)

    # Listar arquivos JSON pendentes na Trusted
    for root, dirs, files in os.walk(trusted_path):
        processed_log = os.path.join(root, ".processed")
        processed_files = set()
        if os.path.exists(processed_log):
            with open(processed_log, 'r') as f:
                processed_files = {line.strip() for line in f}

        json_files = [f for f in files if f.startswith("solar_data_trusted_") and f.endswith(".json") and f not in processed_files]

        for file_name in json_files:
            file_path = os.path.join(root, file_name)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Refinando: {file_name}")

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    readings = json.load(f)

                enriched_list = []
                for r in readings:
                    # Geocodificação Reversa
                    geo_data = reverse_geocode(r['lat'], r['lon'], user_agent)
                    
                    if not geo_data:
                        continue

                    addr = geo_data.get('address', {})
                    
                    # Extração de campos (Idioma Inglês)
                    city = addr.get('city') or addr.get('city_district') or addr.get('town') or addr.get('village') or addr.get('municipality')
                    suburb = addr.get('suburb') or addr.get('neighbourhood') or addr.get('hamlet')
                    postcode = addr.get('postcode')
                    
                    # Regras de Negócio: CEP e Bairro obrigatórios
                    if not postcode or not suburb:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Pulando ID {r['id']}: CEP ou Bairro não encontrados.")
                        continue

                    # Cálculo de médias sazonais (Arredondadas)
                    seasons = calculate_seasons(r['monthly_data'])

                    enriched = {
                        "id": r['id'],
                        "lat": r['lat'],
                        "lon": r['lon'],
                        "state": r.get('state') or r.get('uf'),
                        "city": city,
                        "suburb": suburb,
                        "road": addr.get('road') or addr.get('street') or addr.get('pedestrian'),
                        "postcode": postcode,
                        "full_address": geo_data.get('display_name'),
                        "annual": round(r['annual'], 1),
                        **{m: round(v, 1) for m, v in r['monthly_data'].items()},
                        **{f"{k}_avg": v for k, v in seasons.items()}
                    }
                    enriched_list.append(enriched)

                if enriched_list:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    date_folder = os.path.basename(root)  # Usa MM-YYYY da pasta de origem
                    target_dir = os.path.join(refined_path, date_folder)
                    
                    if not os.path.exists(target_dir):
                        os.makedirs(target_dir)

                    target_file = os.path.join(target_dir, f"solar_data_refined_{timestamp}.json")
                    with open(target_file, 'w', encoding='utf-8') as f:
                        json.dump(enriched_list, f, indent=4, ensure_ascii=False)

                    # Marcar como processado
                    with open(processed_log, 'a') as f:
                        f.write(f"{file_name}\n")
                    
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Salvo em: {target_file}")

            except Exception as e:
                print(f"Erro ao refinar {file_name}: {e}")

if __name__ == "__main__":
    process_trusted_to_refined()
