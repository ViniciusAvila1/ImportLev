import os
import io
import csv
import logging
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import pandas as pd
import psycopg2
from psycopg2 import sql
from urllib.parse import urlencode 
from dotenv import load_dotenv

# --- Carregar variáveis de ambiente do arquivo .env ---
load_dotenv()

# --- Configuração do Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configurações do Flask ---
app = Flask(__name__)
#Puxar a chave secreta do ambiente (do .env)
app.secret_key = os.getenv('FLASK_SECRET_KEY')
if not app.secret_key:
    raise ValueError("A chave secreta (FLASK_SECRET_KEY) não foi definida no arquivo .env")

app.config['UPLOAD_FOLDER'] = 'uploads/'
app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024

if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

# Configurações de Conexão com o PostgreSQL (lidas do .env)
# Puxar as configurações do banco do ambiente (do .env)
db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Validação para garantir que a senha do banco foi carregada
if not db_config['password']:
    raise ValueError("A senha do banco (DB_PASSWORD) não foi definida no arquivo .env")


ESTADOS_BRASILEIROS = [
    ('AC', 'Acre'), ('AL', 'Alagoas'), ('AP', 'Amapá'), ('AM', 'Amazonas'),
    ('BA', 'Bahia'), ('CE', 'Ceará'), ('DF', 'Distrito Federal'), ('ES', 'Espírito Santo'),
    ('GO', 'Goiás'), ('MA', 'Maranhão'), ('MT', 'Mato Grosso'), ('MS', 'Mato Grosso do Sul'),
    ('MG', 'Minas Gerais'), ('PA', 'Pará'), ('PB', 'Paraíba'), ('PR', 'Paraná'),
    ('PE', 'Pernambuco'), ('PI', 'Piauí'), ('RJ', 'Rio de Janeiro'), ('RN', 'Rio Grande do Norte'),
    ('RS', 'Rio Grande do Sul'), ('RO', 'Rondônia'), ('RR', 'Roraima'), ('SC', 'Santa Catarina'),
    ('SP', 'São Paulo'), ('SE', 'Sergipe'), ('TO', 'Tocantins')
]

CIDADES_POR_ESTADO = {
    'AC': ['Rio Branco', 'Cruzeiro do Sul'],
    'AL': ['Maceió', 'Arapiraca'],
    'AP': ['Macapá', 'Santana'],
    'AM': ['Manaus', 'Parintins'],
    'BA': ['Salvador', 'Feira de Santana', 'Vitória da Conquista'],
    'CE': ['Fortaleza', 'Caucaia', 'Juazeiro do Norte'],
    'DF': ['Brasília'],
    'ES': ['Vitória', 'Vila Velha', 'Serra'],
    'GO': ['Goiânia', 'Aparecida de Goiânia', 'Anápolis'],
    'MA': ['São Luís', 'Imperatriz'],
    'MT': ['Cuiabá', 'Várzea Grande'],
    'MS': ['Campo Grande', 'Dourados'],
    'MG': ['Belo Horizonte', 'Uberlândia', 'Contagem', 'Juiz de Fora'],
    'PA': ['Belém', 'Ananindeua'],
    'PB': ['João Pessoa', 'Campina Grande'],
    'PR': ['Curitiba', 'Londrina', 'Maringá'],
    'PE': ['Recife', 'Jaboatão dos Guararapes', 'Olinda'],
    'PI': ['Teresina', 'Parnaíba'],
    'RJ': ['Rio de Janeiro', 'São Gonçalo', 'Duque de Caxias'],
    'RN': ['Natal', 'Mossoró'],
    'RS': ['Porto Alegre', 'Caxias do Sul'],
    'RO': ['Porto Velho', 'Ji-Paraná'],
    'RR': ['Boa Vista', 'Rorainópolis'],
    'SC': ['Florianópolis', 'Joinville', 'Blumenau'],
    'SP': ['São Paulo', 'Guarulhos', 'Campinas', 'São Bernardo do Campo', 'Santo André'],
    'SE': ['Aracaju', 'Nossa Senhora do Socorro'],
    'TO': ['Palmas', 'Araguaína']
}

def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados."""
    return psycopg2.connect(**db_config)

def get_convenios_from_db():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT convenio FROM pessoas WHERE convenio IS NOT NULL ORDER BY convenio;")
        convenios = [row[0] for row in cur.fetchall()]
        cur.close()
        return convenios
    except Exception as e:
        logging.error(f"Erro ao buscar convênios: {e}")
        return []
    finally:
        if conn:
            conn.close()

def importar_planilha_pessoas(caminho_planilha, nome_convenio):
    conn = None
    try:
        logging.info(f"Iniciando leitura do CSV: {caminho_planilha}")
        
        expected_csv_columns = [
            'cpf', 'nome', 'data_nascimento', 'numero_conta_corrente', 'numero_agencia',
            'salario', 'idade', 'cbo', 'uf_endereco', 'municipio_endereco', 'endereco',
            'numero_endereco', 'cep', 'tipo_orgao', 'uf_orgao', 'municipio_orgao',
            'NaoPerturbe'
        ]

        df = pd.read_csv(
            caminho_planilha,
            encoding='latin1',
            sep=';',
            header=None,  # pandas não usar a primeira linha como cabeçalho.
            usecols=range(len(expected_csv_columns)), # continua lendo apenas as primeiras 17 colunas por posição.
            names=expected_csv_columns, # Nomeia as colunas lidas de forma confiável.
            engine='python',
            on_bad_lines='warn'
        )
        df = df.iloc[1:].reset_index(drop=True)  # remove a primeira linha que contém os cabeçalhos originais.
        logging.info(f"CSV lido e cabeçalho removido. {len(df)} linhas de dados para processar.")

        logging.info("Iniciando pré-processamento dos dados.")
        df.rename(columns={'NaoPerturbe': 'nao_perturbe'}, inplace=True)
        df['cpf'] = df['cpf'].astype(str).str.replace(r'\D', '', regex=True)
        df['data_nascimento'] = pd.to_datetime(df['data_nascimento'], format='%d/%m/%Y', errors='coerce')
        df['salario'] = df['salario'].astype(str).str.replace(',', '.', regex=False)
        numeric_cols = ['salario', 'idade']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        bool_map = {'sim': True, 's': True, 'true': True, '1': True, 'não': False, 'n': False, 'false': False, '0': False}
        df['nao_perturbe'] = df['nao_perturbe'].astype(str).str.lower().map(bool_map)
        df['convenio'] = nome_convenio

        final_db_columns = [
            'cpf', 'nome', 'data_nascimento', 'numero_conta_corrente', 'numero_agencia', 'salario', 'idade',
            'cbo', 'uf_endereco', 'municipio_endereco', 'endereco', 'numero_endereco', 'cep',
            'tipo_orgao', 'uf_orgao', 'municipio_orgao', 'nao_perturbe', 'convenio'
        ]
        df_for_copy = df[final_db_columns]

        buffer = io.StringIO()
        df_for_copy.to_csv(
            buffer, sep=',', header=False, index=False, na_rep=r'\N', quoting=csv.QUOTE_MINIMAL
        )
        buffer.seek(0)

        conn = get_db_connection()
        cursor = conn.cursor()
        temp_table_name = "pessoas_temp_" + datetime.now().strftime("%Y%m%d%H%M%S%f")
        
        create_temp_table_query = sql.SQL("""
            CREATE TEMPORARY TABLE {table} (LIKE pessoas INCLUDING DEFAULTS) ON COMMIT DROP;
        """).format(table=sql.Identifier(temp_table_name))
        cursor.execute(create_temp_table_query)

        logging.info(f"Iniciando COPY para a tabela temporária '{temp_table_name}'...")
        
        copy_sql = sql.SQL("""
            COPY {table} FROM STDIN
            WITH (FORMAT CSV, DELIMITER ',', NULL E'\\\\N', QUOTE '\"')
        """).format(table=sql.Identifier(temp_table_name))
        
        cursor.copy_expert(sql=copy_sql, file=buffer)
        logging.info(f"{cursor.rowcount} linhas copiadas para a tabela temporária.")

        logging.info("Inserindo dados na tabela 'pessoas' a partir da temporária...")
        insert_sql = sql.SQL("""
            INSERT INTO pessoas SELECT * FROM {table} ON CONFLICT (cpf) DO NOTHING;
        """).format(table=sql.Identifier(temp_table_name))
        
        cursor.execute(insert_sql)
        inserted_rows = cursor.rowcount
        conn.commit()
        cursor.close()

        total_rows_in_csv = len(df)
        skipped_rows = total_rows_in_csv - inserted_rows
        
        summary = (
            f"Importação da planilha concluída para convênio '{nome_convenio}'.<br>"
            f"Total de linhas no CSV: {total_rows_in_csv}.<br>"
            f"Usuários inseridos no banco: {inserted_rows}.<br>"
            f"Usuários pulados (CPF já existente): {skipped_rows}."
        )
        logging.info(summary.replace('<br>', '\n'))
        return True, summary, []

    except FileNotFoundError:
        error_msg = f"Erro: Planilha não encontrada em '{caminho_planilha}'"
        logging.error(error_msg)
        return False, error_msg, []
    except pd.errors.EmptyDataError:
        error_msg = f"Erro: A planilha '{caminho_planilha}' está vazia ou mal formatada."
        logging.error(error_msg)
        return False, error_msg, []
    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Erro geral durante a importação: {e}"
        logging.error(error_msg, exc_info=True)
        return False, error_msg, []
    finally:
        if conn:
            conn.close()

@app.route('/api/cidades/<uf>')
def get_cidades(uf):
    # Retorna a lista de cidades para o UF fornecido, ou uma lista vazia se o UF não for encontrado
    cidades = CIDADES_POR_ESTADO.get(uf.upper(), [])
    return jsonify(cidades)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        if 'csv_file' not in request.files:
            flash('Nenhum arquivo enviado.', 'error')
            return redirect(request.url)

        file = request.files['csv_file']
        if file.filename == '':
            flash('Nenhum arquivo selecionado.', 'error')
            return redirect(request.url)

        # --- LÓGICA PARA CONSTRUIR O NOME DO CONVÊNIO ---
        esfera = request.form.get('esfera')
        nome_convenio = ''

        if esfera == 'federal':
            nome_convenio = 'FEDERAL'
        elif esfera == 'estadual':
            estado_convenio = request.form.get('estado_convenio')
            if not estado_convenio:
                flash('Por favor, selecione o estado do convênio.', 'error')
                return redirect(request.url)
            nome_convenio = f'GOVERNO_{estado_convenio}'
        elif esfera == 'prefeitura':
            estado_prefeitura = request.form.get('estado_prefeitura')
            cidade_prefeitura = request.form.get('cidade_prefeitura')
            if not estado_prefeitura or not cidade_prefeitura:
                flash('Por favor, selecione o estado e a cidade do convênio.', 'error')
                return redirect(request.url)
            nome_convenio = f'PREFEITURA_{cidade_prefeitura.replace(" ", "_").upper()}_{estado_prefeitura}'
        else:
            flash('Por favor, selecione a esfera do convênio.', 'error')
            return redirect(request.url)
        # --- FIM DA LÓGICA ---

        if file and file.filename.lower().endswith('.csv'):
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(filepath)

            # A função de importação recebe o nome do convênio já formatado
            success, message, logs = importar_planilha_pessoas(filepath, nome_convenio)

            if os.path.exists(filepath):
                os.remove(filepath)

            if success:
                flash('Importação concluída com sucesso!', 'success')
                flash(message, 'info')
            else:
                flash('Falha na importação.', 'error')
                flash(message, 'error')

            return redirect(url_for('visualizar_dados'))
        else:
            flash('Tipo de arquivo não permitido. Por favor, envie um arquivo CSV.', 'error')
            return redirect(request.url)

    return render_template('index.html', estados=ESTADOS_BRASILEIROS)


@app.route('/visualizar')
def visualizar_dados():
    # Inicializa todas as variáveis com valores padrão seguros
    conn = None
    usuarios = []
    page = 1
    total_pages = 1
    
    # Lê os parâmetros de filtro da URL
    estado_filtro = request.args.get('estado', '')
    cpf_busca = request.args.get('cpf_busca', '')
    nome_busca = request.args.get('nome_busca', '')
    convenio_filtro = request.args.get('convenio', '')

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Define as variáveis de paginação
        page = request.args.get('page', 1, type=int)
        per_page = 20
        offset = (page - 1) * per_page

        # Constrói a query de filtro dinamicamente
        base_query = "FROM pessoas WHERE 1=1"
        params = []
        
        if estado_filtro and estado_filtro != 'TODOS':
            base_query += " AND uf_endereco = %s"
            params.append(estado_filtro)
        if convenio_filtro and convenio_filtro != 'TODOS':
            base_query += " AND convenio = %s"
            params.append(convenio_filtro)
        if cpf_busca:
            base_query += " AND cpf ILIKE %s"
            params.append(f"%{cpf_busca.strip()}%")
        if nome_busca:
            base_query += " AND nome ILIKE %s"
            params.append(f"%{nome_busca.strip()}%")

        # Executa a query para CONTAR o total de registros
        count_query = f"SELECT COUNT(*) {base_query};"
        cur.execute(count_query, params)
        total_usuarios = cur.fetchone()[0]
        
        # Calcula o total de páginas
        if total_usuarios > 0:
            total_pages = (total_usuarios + per_page - 1) // per_page
        else:
            total_pages = 1

        # Busca os dados da PÁGINA ATUAL
        select_query = f"SELECT cpf, nome, uf_endereco, convenio {base_query} ORDER BY nome, cpf LIMIT %s OFFSET %s;"
        query_params_with_pagination = params + [per_page, offset]
        
        cur.execute(select_query, query_params_with_pagination)
        usuarios = cur.fetchall()
        cur.close()

    except Exception as e:
        flash(f"Erro ao carregar dados: {e}", 'error')
    
    finally:
        if conn:
            conn.close()

    # --- INÍCIO DA CORREÇÃO PARA O ERRO DE PAGINAÇÃO ---
    # Cria uma cópia dos argumentos da URL para poder modificá-los
    filter_args = request.args.to_dict()
    # Remove a chave 'page' para evitar o conflito no url_for
    filter_args.pop('page', None)
    # --- FIM DA CORREÇÃO ---

    convenios_disponiveis = get_convenios_from_db()

    return render_template('visualizar.html',
                           usuarios=usuarios,
                           estados=ESTADOS_BRASILEIROS,
                           selected_estado=estado_filtro,
                           current_page=page,
                           total_pages=total_pages,
                           cpf_busca=cpf_busca,
                           nome_busca=nome_busca,
                           convenios_disponiveis=convenios_disponiveis,
                           selected_convenio=convenio_filtro,
                           filter_args=filter_args) # <<-- Passa o dicionário limpo para o template


if __name__ == '__main__':
    app.run(debug=True, port=5000)