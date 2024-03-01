from flask import Blueprint

Main = Blueprint('Debug', __name__, url_prefix="/debug", template_folder='templates',
                     static_folder='static')

blueprints = [Main]