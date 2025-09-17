# app/__init__.py
from flask import Flask
from dotenv import load_dotenv
import os

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv(encoding='cp1252')

def create_app():
    """Cria e configura uma instância da aplicação Flask."""
    app = Flask(__name__, template_folder='../templates')

    # Configurações da aplicação
    app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY')
    app.config['UPLOAD_FOLDER'] = 'uploads/'
    app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024
    
    # Garante que a pasta de uploads exista
    if not os.path.exists(app.config['UPLOAD_FOLDER']):
        os.makedirs(app.config['UPLOAD_FOLDER'])

    with app.app_context():
        # Importa e registra os Blueprints
        from .main.routes import main_bp
        from .api.routes import api_bp

        app.register_blueprint(main_bp)
        app.register_blueprint(api_bp, url_prefix='/api') # Rotas de API terão prefixo /api

    return app