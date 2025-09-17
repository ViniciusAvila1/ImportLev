# app/api/routes.py
from flask import Blueprint, jsonify
from app.services.ibge_api import get_cidades_from_api

api_bp = Blueprint('api', __name__)

@api_bp.route('/cidades/<uf>')
def get_cidades(uf):
    cidades = get_cidades_from_api(uf)
    if not cidades:
        return jsonify([]), 500
    return jsonify(cidades)