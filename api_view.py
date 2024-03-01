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

@API.route('/search/files',methods=['GET'])
def api_search_files():
    return jsonify(methods.search_files(**request.args))

@API.route('/sequence/get_files/<sequence_name>', methods=['GET'])
def get_sequence_files(sequence_name):
    return jsonify(methods.load_sequence(sequence=sequence_name))
    pass

@API.route('/user/custom_search_data',methods=['GET','POST'])
def user_custom_search_data():
    if request.method == 'GET':
        return jsonify(methods.load_user_config('custom_search'))
    elif request.method == 'POST':
        if UPDATE_DATA:=request.form.get('UPDATE_DATA',None):
            return jsonify(methods.update_user_config('custom_search',json.loads(UPDATE_DATA))) #convert from string

@API.route('/tags/info/', methods=['GET'])
def tag_info():
    if tag_auto_key := request.args.get('TAG_AUTO_KEY', None):
        results = methods.get_tag_details(tag_auto_key,**request.args)
        return jsonify(results)
    else:
        return errorhandler.make_error(400, "TAG_AUTO_KEY must be supplied.")

@methods.process_method('load_image')
@methods.process_method('api_load_image')
@API.route('/load_image/<file_name>', methods=['GET'])
def load_image_file(file_name):
    """
    Returns an image based on the filename
    :param file_name:
    :return:
    """
    if request.method == 'GET':
        return methods.load_image_file(file_name,**request.args)
    return ''

