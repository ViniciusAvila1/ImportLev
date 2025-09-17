# app/services/ibge_api.py
import requests
import logging
from datetime import datetime, timedelta

# Cache em memória simples para a lista de estados
cache_estados = {
    'data': None,
    'timestamp': datetime.min
}

def get_estados_from_api():
    """Busca a lista de estados da API do IBGE, com cache de 24 horas."""
    global cache_estados
    if cache_estados['data'] and (datetime.now() - cache_estados['timestamp']) < timedelta(hours=24):
        logging.info("Servindo lista de estados do CACHE.")
        return cache_estados['data']

    logging.info("Buscando lista de estados da API do IBGE...")
    try:
        url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados?orderBy=nome"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        estados = response.json()
        cache_estados.update({'data': estados, 'timestamp': datetime.now()})
        return estados
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar estados da API do IBGE: {e}")
        return []

def get_cidades_from_api(uf):
    """Busca as cidades de um estado específico na API do IBGE."""
    try:
        url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf.upper()}/municipios"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        cidades_data = response.json()
        return [cidade['nome'] for cidade in cidades_data]
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar cidades para {uf}: {e}")
        return []