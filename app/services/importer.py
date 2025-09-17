# app/services/importer.py
import pandas as pd
import psycopg2
from psycopg2 import sql
import logging
import io
import csv
from datetime import datetime
from app.database import get_db_connection # Importa a função de conexão

def importar_planilha_pessoas(caminho_planilha, nome_convenio):
    #
    conn = None
    try:
        # --- 1. LEITURA DO CSV ---
        logging.info(f"Iniciando leitura do CSV: {caminho_planilha}")
        col_names = [f'col_{i}' for i in range(50)]
        df = pd.read_csv(
            caminho_planilha, encoding='latin1', sep=';', header=None,
            names=col_names, engine='python', on_bad_lines='warn'
        )
        
        col_mapping = {
            'col_0': 'cpf', 'col_1': 'nome', 'col_2': 'data_nascimento', 'col_3': 'numero_conta_corrente',
            'col_4': 'numero_agencia', 'col_5': 'salario', 'col_6': 'idade', 'col_7': 'cbo',
            'col_8': 'uf_endereco', 'col_9': 'municipio_endereco', 'col_10': 'endereco',
            'col_11': 'numero_endereco', 'col_12': 'cep', 'col_13': 'tipo_orgao',
            'col_14': 'uf_orgao', 'col_15': 'municipio_orgao', 'col_16': 'nao_perturbe'
        }
        df.rename(columns=col_mapping, inplace=True)
        df = df.iloc[1:].reset_index(drop=True)
        logging.info(f"CSV lido com {len(df)} linhas de dados.")

        # --- 2. PRÉ-PROCESSAMENTO (PESSOAS) ---
        logging.info("Iniciando pré-processamento dos dados das pessoas.")
        df['cpf'] = df['cpf'].astype(str).str.replace(r'\D', '', regex=True).str.strip()
        df['data_nascimento'] = pd.to_datetime(df['data_nascimento'], dayfirst=True, errors='coerce')
        df['salario'] = pd.to_numeric(df['salario'].astype(str).str.replace(',', '.', regex=False), errors='coerce')
        df['idade'] = pd.to_numeric(df['idade'], errors='coerce').astype('Int64')
        bool_map = {'sim': True, 's': True, 'true': True, '1': True, 'não': False, 'n': False, 'false': False, '0': False}
        df['nao_perturbe'] = df['nao_perturbe'].astype(str).str.lower().map(bool_map)
        df['convenio'] = nome_convenio
        df.dropna(subset=['cpf'], inplace=True)

        # --- 3. INSERÇÃO DOS DADOS DE PESSOAS ---
        conn = get_db_connection()
        cursor = conn.cursor()

        people_cols = [
            'cpf', 'nome', 'data_nascimento', 'numero_conta_corrente', 'numero_agencia', 'salario', 'idade',
            'cbo', 'uf_endereco', 'municipio_endereco', 'endereco', 'numero_endereco', 'cep',
            'tipo_orgao', 'uf_orgao', 'municipio_orgao', 'nao_perturbe', 'convenio'
        ]
        df_pessoas = df[people_cols].drop_duplicates(subset=['cpf'])
        
        buffer_pessoas = io.StringIO()
        df_pessoas.to_csv(buffer_pessoas, sep=',', header=False, index=False, na_rep=r'\N', quoting=csv.QUOTE_MINIMAL)
        buffer_pessoas.seek(0)
        
        temp_pessoas_table = f"pessoas_temp_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
        # A tabela temporária não precisa da chave primária, apenas da estrutura de colunas
        create_temp_sql = "CREATE TEMPORARY TABLE {table} (LIKE pessoas EXCLUDING CONSTRAINTS) ON COMMIT DROP;"
        cursor.execute(sql.SQL(create_temp_sql).format(table=sql.Identifier(temp_pessoas_table)))
        
        copy_sql_pessoas = sql.SQL("COPY {table} FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL E'\\\\N', QUOTE '\"')").format(table=sql.Identifier(temp_pessoas_table))
        cursor.copy_expert(sql=copy_sql_pessoas, file=buffer_pessoas)
        
        insert_sql_pessoas = sql.SQL("INSERT INTO pessoas SELECT * FROM {table} ON CONFLICT (cpf) DO NOTHING;").format(table=sql.Identifier(temp_pessoas_table))
        cursor.execute(insert_sql_pessoas)
        inserted_rows = cursor.rowcount
        logging.info(f"{inserted_rows} registros de pessoas inseridos.")

        # --- 4. TRANSFORMAÇÃO E INSERÇÃO DOS TELEFONES ---
        logging.info("Iniciando processamento dos telefones.")
        
        phone_cols_start_index = 18 
        phone_cols = [col for col in df.columns if col.startswith('col_') and int(col.split('_')[1]) >= phone_cols_start_index]
        
        df_telefones_long = pd.melt(df, id_vars=['cpf'], value_vars=phone_cols, value_name='numero')
        
        df_telefones_long.dropna(subset=['numero'], inplace=True)
        df_telefones_long['numero'] = df_telefones_long['numero'].astype(str).str.replace(r'\D', '', regex=True).str.strip()
        df_telefones_long = df_telefones_long[df_telefones_long['numero'].str.len() >= 8]
        df_telefones_long.drop_duplicates(inplace=True)

        if not df_telefones_long.empty:
            buffer_telefones = io.StringIO()
            df_telefones_long[['cpf', 'numero']].to_csv(buffer_telefones, sep=',', header=False, index=False, na_rep=r'\N')
            buffer_telefones.seek(0)
            
            temp_telefones_table = f"telefones_temp_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
            cursor.execute(sql.SQL("CREATE TEMPORARY TABLE {table} (cpf VARCHAR(11), numero VARCHAR(25)) ON COMMIT DROP;").format(table=sql.Identifier(temp_telefones_table)))

            copy_sql_telefones = sql.SQL("COPY {table} FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL E'\\\\N')").format(table=sql.Identifier(temp_telefones_table))
            cursor.copy_expert(sql=copy_sql_telefones, file=buffer_telefones)
            
            # --- INÍCIO DA MUDANÇA NA LÓGICA DE INSERÇÃO ---
            # Agora inserimos o CPF diretamente, sem precisar do JOIN para buscar o ID.
            # A lógica para evitar duplicatas também foi ajustada.
            insert_sql_telefones = sql.SQL("""
                INSERT INTO telefones (pessoa_cpf, numero)
                SELECT t_temp.cpf, t_temp.numero
                FROM {temp_telefones_table} t_temp
                WHERE EXISTS ( -- Garante que a pessoa com este CPF existe na tabela principal
                    SELECT 1 FROM pessoas p WHERE p.cpf = t_temp.cpf
                ) AND NOT EXISTS ( -- Garante que este telefone não existe para esta pessoa
                    SELECT 1 FROM telefones t_exist
                    WHERE t_exist.pessoa_cpf = t_temp.cpf AND t_exist.numero = t_temp.numero
                );
            """).format(temp_telefones_table=sql.Identifier(temp_telefones_table))
            # --- FIM DA MUDANÇA NA LÓGICA DE INSERÇÃO ---
            
            cursor.execute(insert_sql_telefones)
            inserted_phones = cursor.rowcount
            logging.info(f"{inserted_phones} registros de telefones inseridos.")
        else:
            inserted_phones = 0
            logging.info("Nenhum telefone válido encontrado para importar.")

        conn.commit()
        cursor.close()

        summary = (
            f"Importação da planilha concluída para convênio '{nome_convenio}'.<br>"
            f"Total de pessoas inseridas: {inserted_rows}.<br>"
            f"Total de telefones inseridos: {inserted_phones}."
        )
        return True, summary, []

    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Erro geral durante a importação: {e}"
        logging.error(error_msg, exc_info=True)
        return False, error_msg, []
    finally:
        if conn:
            conn.close()