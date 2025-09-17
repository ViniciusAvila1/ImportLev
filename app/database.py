# app/database.py
import psycopg2
import os

# Pega as configurações do banco do ambiente
db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'client_encoding': 'UTF8'
}

def get_db_connection():
    """Cria e retorna uma nova conexão com o banco de dados."""
    if not db_config['password']:
        raise ValueError("A senha do banco (DB_PASSWORD) não foi definida no arquivo .env")
    return psycopg2.connect(**db_config)