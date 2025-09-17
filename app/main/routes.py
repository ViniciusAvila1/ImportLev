# app/main/routes.py
from flask import Blueprint, render_template, request, current_app, flash, redirect, url_for
import os
import logging
# Importa as funções dos nossos módulos de serviço e banco
from app.services.ibge_api import get_estados_from_api
from app.services.importer import importar_planilha_pessoas
from app.database import get_db_connection

# Cria o Blueprint
main_bp = Blueprint('main', __name__)

@main_bp.route('/', methods=['GET', 'POST'])
def index():
    # 1. Busca a lista de estados no início.
    # Assim, 'estados' estará disponível para o GET e para o POST.
    estados = get_estados_from_api()
    if not estados:
        flash('Aviso: Não foi possível carregar a lista de estados da API do IBGE.', 'warning')
        estados = [] # Garante que 'estados' seja uma lista, mesmo em caso de falha na API

    # 2. Se a requisição for POST, processa o formulário
    if request.method == 'POST':
        esfera = request.form.get('esfera')
        estado_convenio = request.form.get('estado_convenio')
        estado_prefeitura = request.form.get('estado_prefeitura')
        cidade_prefeitura = request.form.get('cidade_prefeitura')
        file = request.files.get('csv_file')

        form_data = {
            'selected_esfera': esfera,
            'selected_estado_convenio': estado_convenio,
            'selected_estado_prefeitura': estado_prefeitura,
            'selected_cidade_prefeitura': cidade_prefeitura
        }

        # Validações (agora podemos re-renderizar com segurança, pois 'estados' já existe)
        if not file or file.filename == '':
            flash('Nenhum arquivo selecionado.', 'error')
            return render_template('index.html', estados=estados, **form_data)

        if not file.filename.lower().endswith('.csv'):
            flash('Tipo de arquivo não permitido. Por favor, envie um arquivo CSV.', 'error')
            return render_template('index.html', estados=estados, **form_data)
        
        # Construção do nome do convênio
        nome_convenio = ''
        if esfera == 'federal':
            nome_convenio = 'FEDERAL'
        elif esfera == 'estadual':
            if not estado_convenio: 
                flash('Por favor, selecione o estado do convênio.', 'error')
                return render_template('index.html', estados=estados, **form_data)
            # Corrigindo o nome do convênio para o seu padrão anterior
            nome_convenio = f'ESTADUAL_{estado_convenio}'
        elif esfera == 'prefeitura':
            if not estado_prefeitura or not cidade_prefeitura: 
                flash('Por favor, selecione o estado e a cidade do convênio.', 'error')
                return render_template('index.html', estados=estados, **form_data)
            nome_convenio = f'PREFEITURA_{cidade_prefeitura.replace(" ", "_").upper()}_{estado_prefeitura}'
        else:
            flash('Por favor, selecione a esfera do convênio.', 'error')
            return render_template('index.html', estados=estados, **form_data)

        # Se todas as validações passaram, continua com a importação
        filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], file.filename)
        file.save(filepath)

        success, message, logs = importar_planilha_pessoas(filepath, nome_convenio)

        if os.path.exists(filepath):
            os.remove(filepath)

        if success:
            flash('Importação concluída com sucesso!', 'success')
            flash(message, 'info')
        else:
            flash('Falha na importação.', 'error')
            flash(message, 'error')

        return redirect(url_for('main.visualizar_dados'))

    # 3. Se a requisição for GET, o código chega aqui.
    # A variável 'estados' já foi buscada e está pronta para ser usada.
    return render_template('index.html', estados=estados)


@main_bp.route('/visualizar')
def visualizar_dados():
    #
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
        per_page = 20 # Itens por página
        offset = (page - 1) * per_page

        # Constrói a query de filtro dinamicamente
        base_query = "FROM pessoas WHERE 1=1"
        params = []
        
        if estado_filtro and estado_filtro != 'TODOS': # Mantém compatibilidade com valor 'TODOS'
            base_query += " AND uf_endereco = %s" # Filtra por estado
            params.append(estado_filtro)
        if convenio_filtro and convenio_filtro != 'TODOS':
            base_query += " AND convenio = %s" # Filtra por convênio
            params.append(convenio_filtro)
        if cpf_busca:
            base_query += " AND cpf ILIKE %s" # Busca parcial por CPF
            params.append(f"%{cpf_busca.strip()}%")
        if nome_busca:
            base_query += " AND nome ILIKE %s" # Busca parcial por nome
            params.append(f"%{nome_busca.strip()}%")

        # Executa a query para CONTAR o total de registros
        count_query = f"SELECT COUNT(*) {base_query};" 
        cur.execute(count_query, params) 
        total_usuarios = cur.fetchone()[0] # Pega o total de registros
        
        # Calcula o total de páginas
        if total_usuarios > 0:
            total_pages = (total_usuarios + per_page - 1) // per_page
        else:
            total_pages = 1

        # Busca os dados da PÁGINA ATUAL
        select_query = f"SELECT cpf, nome, data_nascimento, convenio {base_query} ORDER BY nome, cpf LIMIT %s OFFSET %s;"
        query_params_with_pagination = params + [per_page, offset]
        
        cur.execute(select_query, query_params_with_pagination)
        usuarios_raw = cur.fetchall() # Pega os dados brutos
        cur.close()
        
        #Formatar data para padrão brasileiro (dd/mm/yyyy)
        for user in usuarios_raw:
            cpf, nome, data_nasc, convenio = user
            data_formatada = data_nasc.strftime('%d/%m/%Y') if data_nasc else None
            usuarios.append((cpf, nome, data_formatada, convenio))

    except Exception as e:
        flash(f"Erro ao carregar dados: {e}", 'error')
    
    finally:
        if conn:
            conn.close()
            
    filter_args = request.args.to_dict() # Mantém os filtros na paginação
    filter_args.pop('page', None) # Remove o parâmetro 'page' para evitar duplicatas
    
    estados = get_estados_from_api() # Busca a lista de estados para o filtro
    convenios_disponiveis = []
    try:
        conn = get_db_connection() # Reabre a conexão para buscar os convênios
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT convenio FROM pessoas WHERE convenio IS NOT NULL ORDER BY convenio;") # Busca convênios distintos
        convenios_disponiveis = [row[0] for row in cur.fetchall()] # Extrai os convênios
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Erro ao buscar convênios: {e}")
        
    return render_template('visualizar.html',
                           usuarios=usuarios,
                           estados=estados,
                           selected_estado=estado_filtro,
                           current_page=page,
                           total_pages=total_pages,
                           filter_args=filter_args,
                           convenios_disponiveis=convenios_disponiveis,
                           selected_convenio=convenio_filtro,
                           cpf_busca=cpf_busca,
                           nome_busca=nome_busca)
