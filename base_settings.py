from flask import Blueprint
import os
import json
try:
    from .methods_simple import get_yaml_config
except:
    from methods_simple import get_yaml_config

config_data = get_yaml_config()

def save_server_options(server_options):
    with open(f'{os.path.dirname(__file__)}/server_options.json', 'w') as JSON:
        json.dump(server_options,JSON)

server_options_default = {
    'enable_api':False
}

try:
    with open(f'{os.path.dirname(__file__)}/server_options.json','r') as JSON:
        server_options = json.load(JSON)
except:
    server_options = server_options_default.copy()
    save_server_options(server_options)


if append_schema:= config_data.get('append_schema',None):
    append_schema = f'_{append_schema}'
else:
    append_schema = ''

if append_route:= config_data.get('append_route',None):
    append_route = f'-{append_route}'
else:
    append_route = ''

Main = Blueprint(f'ImageSorter{append_schema}', __name__, url_prefix=f"/imagesorter{append_route}", template_folder=f'templates/imagesorter',
                     static_folder=f'static/imagesorter')
API = Blueprint(f'ImageSorter{append_schema}_API',__name__, url_prefix=f'/imagesorter{append_route}/api',template_folder=f'templates/imagesorter/api',static_folder=f'static/imagesorter')
database_binding = f'imagesorter{append_schema}'


Schema = f'imagesorter{append_schema}'

Main.display_name = f'ImageSorter{append_route}'
Main.blueprint_name = f'ImageSorter{append_schema}'
API.blueprint_name = f'ImageSorter{append_schema}_API'
Main.config_data = config_data
Main.append_schema = append_schema

print('Loaded Config:',Main.config_data)

Main.server_settings = {
    'folder':
        {
         },
    'options':
        {
        }
}

Main.server_settings['options'].update(**server_options)

blueprints = [Main,API]
