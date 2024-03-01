from flask import jsonify
from flask import request, render_template
from tqdm import tqdm
import time
import inspect

from .base_settings import Main
from ... import methods, db as imagesorter_db
from ...base_settings import Main as Main_IS


@Main.before_request
def before_first():
    pass

Main_IS.add_new_navigator_link('Debug Menu', 'Debug.debug_menu', 'Debug')


@Main.route('/', methods=['GET', 'POST'])
@methods.process_large_function_route()
def debug_menu():
    if request.method == 'GET':
        return render_template('debug_menu.html', available_functions=[
            ['Refresh all tag directory crossover', 'FULL_TAG_DIRECTORY_CROSSOVER_REFRESH'],
            ['Update all of IMTAG_PRECALC', 'UPDATE_ALL_IMTAG_PRECALC'],
            ['Remove duplicate tags on IMTAG', 'REMOVE_DUPLICATE_TAGS'],
            ['Update all file data', 'UPDATE_ALL_FILE_DATA'],
            ['update missing imfi or imtag precalc', 'UPDATE_MISSING_IMFI_OR_IMTAG_PRECALC']],
           blueprint_name=Main_IS.blueprint_name
                               )
    elif request.method == 'POST':
        if DEBUG_MODE := request.form.get('DEBUG_MODE', None):
            if DEBUG_MODE == 'FULL_TAG_DIRECTORY_CROSSOVER_REFRESH':
                imagesorter_db.full_tag_directory_crossover_refresh()
            elif DEBUG_MODE == 'UPDATE_ALL_IMTAG_PRECALC':
                imagesorter_db.update_all_imtag_precalc()
            elif DEBUG_MODE == 'REMOVE_DUPLICATE_TAGS':
                imagesorter_db.remove_duplicate_tags()
            elif DEBUG_MODE == 'UPDATE_ALL_FILE_DATA':
                imagesorter_db.update_all_file_data()
            elif DEBUG_MODE == 'UPDATE_MISSING_IMFI_OR_IMTAG_PRECALC':
                imagesorter_db.update_missing_imfi_or_imtag_precalc()
            return jsonify(True)
        else:
            return jsonify(None)


@Main.route('/test_process_lock/', methods=['GET'])
@methods.process_large_function_route('testLock', ['GET'])
def test_process_lock():

    """
    Testing large process locking (for deletions/updates/processing)
    :return:
    """
    pbar = tqdm(range(20))
    methods.resetLoadingStatus('test')
    methods.updateLoadingStatus('test', name='Testing Route')
    for item in pbar:
        methods.updateLoadingStatus('test', pbar, current_item=item)
        time.sleep(1)
    methods.updateLoadingStatus('test', pbar)

    return ''




@Main.route('/available_categories/', methods=['GET'])
def debug_available_categories():
    return jsonify(methods.get_available_categories())

@Main.route('/get_info/process_methods',methods=['GET'])
def show_process_methods():
    """
    Returns a list of all available uses and active uses of the method processing. This includes pre and post-processing.
    """
    return jsonify(Main_IS.process_method_wrapped)