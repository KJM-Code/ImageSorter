import json
from flask import send_file, render_template, jsonify, url_for, request

from imports import errorhandler
from . import methods
from .base_settings import Main,API, Schema

## TODO add keys and what they do / what's required
API.parameter_info = {
    'file_search':[
        ['SEARCH',1],
        ['RAW_RESULTS',1],
        ['MAIN_CATEGORY',1],
        ['SUB_CATEGORY',1],
        ['RANDOM_SEED',1],
        ['IMFI_AUTO_KEY',1],
        ['NO_COUNT',1],
        ['SEARCH',1],
        ['LIMIT',1],
        ['PAGE',1],
    ]
}




@API.before_request
@methods.only_on_server_system()
def before_request():
    ##TODO
    ## Create API Keys to authenticate
    if Main.server_settings['options'].get('enable_api',False) is False:
        return 'Api is disabled. Please enable it in your ImageSorter config settings. (This is a work in progress.)'
    pass

