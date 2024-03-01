import datetime as dt
import json
import mimetypes
import os
import subprocess
import time
from io import BytesIO
import shutil
import git

from PIL import Image
from flask import send_file, render_template, jsonify, url_for, request
from flask_login import login_required
from tqdm import tqdm

from imports import errorhandler
from imports.database import db
from . import db as imagesorter_db
from . import methods
from . import database_setup
from .base_settings import Main, Schema, API, save_server_options
from sqlalchemy import text as sql_text



#Required
from . import api_view
from flask import send_from_directory


Main.default_tags = ['GENERAL:DELETE', 'GENERAL:PRIVATE']

Main._before_first_request = False
Main._processing_images = False



Main.selectedImageCommands = []
Main.imageBaseCommands = []

Main.server_settings['folder']['pending_removal'] = Main.config_data.get('pending_removal_folder',Main.static_folder+'/pending_removal')
Main.server_settings['folder']['pending_removal_duplicates'] = Main.config_data.get('pending_removal_dupes_folder',Main.static_folder+'/pending_removal_duplicates')
Main.server_settings['folder']['thumbnails'] = Main.config_data.get('thumbnails_folder',Main.static_folder+'/thumbnails')
Main.server_settings['folder']['user_info'] = Main.config_data.get('user_data_folder',Main.static_folder+'/user_info')
Image.MAX_IMAGE_PIXELS = Main.config_data.get('pil_max_image_pixels',89478485)


for folder_key in Main.server_settings['folder'].keys():
    try:
        os.mkdir(Main.server_settings['folder'][folder_key])
    except:
        pass


def add_new_extension_postprocess_method(key, method):
    if key not in Main.extension_postprocess:
        Main.extension_postprocess[key] = []
    if key not in Main.process_method_wrapped:
        Main.process_method_wrapped[key] = {'Function': None,
                                                 'Module': None,
                                                 'Active': {'preprocess': [], 'postprocess': []}}
    Main.process_method_wrapped[key]['Active']['postprocess'].append({'Function':method.__name__,'Module':method.__module__})
    Main.extension_postprocess[key].append(method)


def add_new_extension_preprocess_method(key, method):
    if key not in Main.extension_preprocess:
        Main.extension_preprocess[key] = []
    if key not in Main.process_method_wrapped:
        Main.process_method_wrapped[key] = {'Function': None,
                                                 'Module': None,
                                                 'Active': {'preprocess': [], 'postprocess': []}}
    Main.process_method_wrapped[key]['Active']['preprocess'].append({'Function': method.__name__, 'Module': method.__module__})
    Main.extension_preprocess[key].append(method)


def add_new_navigator_link(url_name: str, endpoint: str, category: str = 'General', kwargs_dict={}):
    category = category.upper().strip()
    if category not in Main.navigator_links:
        Main.navigator_links[category] = []
    Main.navigator_links[category].append([url_name, endpoint, kwargs_dict])


def add_new_selected_image_command(json):
    # For use with the landing_page, context menu commands on images for selected images.
    Main.selectedImageCommands.append(json)
    pass

def add_new_image_base_commands(json):
    #For use with the landing_page, context menu commands on images for both selectable and selected images
    Main.imageBaseCommands.append(json)
    pass




Main.thumbnail_maxSize = Main.config_data.get('max_thumbnail_size',2000)
Main.thumbnail_sizes = Main.config_data.get('thumbnail_sizes',[200,400,800])

if isinstance(Main.thumbnail_maxSize,str):
    try:Main.thumbnail_maxSize = int(Main.thumbnail_maxSize)
    except:
        print("Error loading thumbnail max size\nLoading Default [2000].")
        print(f"Error with: {Main.thumbnail_maxSize}")
        Main.thumbnail_maxSize = 2000

if isinstance(Main.thumbnail_sizes,str):
    try:Main.thumbnail_sizes = [int(thumbnail_size) for thumbnail_size in Main.thumbnail_sizes.split(',')]
    except:
        print("Error loading thumbnail sizes\nLoading Defaults [200,400,800]")
        print(f"Error with: {Main.thumbnail_sizes}")
        Main.thumbnail_sizes = [200,400,800]

Main.thumbnail_sizes.sort()
Main.thumbnail_small,Main.thumbnail_medium,Main.thumbnail_large = Main.thumbnail_sizes[0:3]

Main.thumbnail_sizes.append(Main.thumbnail_maxSize)


Main.allVectors = {}
Main._pending_duplicate_removal = {}
Main.dupeChecking = False
Main.availableFolders = []
Main.unavailableFolders = []

Main.updateStatus = {}
Main.lockNewLargeProcesses = {}
Main.extension_data = {}  ## Example usage: {'search_tags':method} -- if it returns something, it returns that instead of the original route. Intercepting the original function
Main.extension_preprocess = {}
Main.extension_postprocess = {}

Main.navigator_links = {}
Main.add_new_extension_preprocess_method = add_new_extension_preprocess_method
Main.add_new_extension_postprocess_method = add_new_extension_postprocess_method
Main.add_new_navigator_link = add_new_navigator_link
Main.add_new_selected_image_command = add_new_selected_image_command
Main.add_new_image_base_commands = add_new_image_base_commands


Main.add_new_navigator_link('Load Files','load_files_landing_page','General')
Main.add_new_navigator_link('Duplicate Removal', 'duplicate_images', 'General')
Main.add_new_navigator_link('Pending Removal', 'pending_removal', 'General')
Main.add_new_navigator_link('Tags', 'tag_view', 'General')
Main.add_new_navigator_link('Options','options_menu','General')


Main.add_new_extension_preprocess_method('search_tags',methods.preprocess_tag_similar_tags)
# Main.add_new_navigator_link('Load Files','ImageSorter','Base')


import importlib

# Get the path of the 'extensions' folder
extensions_dir = os.path.join(os.path.dirname(__file__), 'extensions')
available_extensions = []
import sys

# Iterate over the folders in the 'extensions' directory, loading them into imports

for extension_folder in os.listdir(extensions_dir):
    # Construct the full path of the extension folder



    if extension_folder not in Main.config_data.get('disabled_extensions',[]):

        extension_path = os.path.join(extensions_dir, extension_folder)

        available_extensions.append([extension_folder])

        # Check if the item is a directory
        if os.path.isdir(extension_path):
            sys.path.append(extension_path)
            if os.path.isfile(f"{extension_path}/base_settings.py"):
                module = importlib.import_module(f'.extensions.{extension_folder}.base_settings', __package__)
                if hasattr(module, 'blueprints'):
                    blueprints = getattr(module, 'blueprints')
                    for blueprint in blueprints:
                        Main.register_blueprint(blueprint)
                        print(f'[[{Main.blueprint_name}]] - Registered Extension Blueprint:', blueprint)
                if hasattr(module, 'api_blueprints'):
                    blueprints = getattr(module, 'api_blueprints')
                    for blueprint in blueprints:
                        API.register_blueprint(blueprint)
                        print(f'[[{API.blueprint_name}]] - Registered Extension Blueprint:', blueprint)
            if os.path.isfile(f"{extension_path}/view.py"):
                # Import the 'main' module dynamically from the extension folder
                module = importlib.import_module(f'.extensions.{extension_folder}.view', __package__)



@methods.process_method('before_first')
def before_first():
    if not Main._before_first_request:
        Main._before_first_request = True
    else:
        return

    Main.navigator_links = {key: [[value[0], url_for(f"{Main.blueprint_name}.{value[1]}", **value[2])] for value in Main.navigator_links[key]] for
                            key in Main.navigator_links}
    for tag in Main.default_tags:
        tag = tag.split(':')
        TAG = imagesorter_db.TAGS.query.filter_by(category=tag[0].upper(), detail=tag[1].upper()).first()
        if not TAG:
            TAG = imagesorter_db.TAGS(category=tag[0].upper(), detail=tag[1].upper(), timestamp=dt.datetime.now(),
                                      last_used=dt.datetime.now(), notes='')
            db.session.add(TAG)
            db.session.commit()

    print(__name__,"[ImageSorter] - Setting up required database SQL.")
    database_setup.create_required_sql()

    return
    print("Getting available folders.")
    get_available_folders()

    # Main.folderStatus = {x[0]: {'processing': False, 'completed': 0} for x in Image_Dirs}


    """
        Checks that the default tags are in
    """

@Main.before_request
@login_required
def before():
    before_first()



from ssl import SSLEOFError
@Main.errorhandler(SSLEOFError)
def handle_ssleof_error(error):
    # Log the error or take appropriate actions
    Main.logger.error(f"SSLEOFError: {error}")
    return "Error loading image", 500



@Main.route('/loading_status/', methods=["GET"])
def get_loading_status_base():
    if '@DEBUG' in request.args:
        return jsonify(Main.updateStatus)
    return ''


@Main.route('/loading_status/<identifier>/', methods=['GET'])
def get_loading_status(identifier):

    if identifier in Main.updateStatus:
        return jsonify(Main.updateStatus[identifier])
    else:
        return jsonify({})

@Main.route('/cancel_loading_operation/', methods=["POST"])
def cancel_loading_operation_base():
    return ''


@Main.route('/cancel_loading_operation/<identifier>/', methods=['POST'])
def cancel_loading_operation(identifier):
    if identifier in Main.updateStatus:
        methods.cancelLoadingStatus(identifier)
        return jsonify(True)
    else:
        return jsonify(False)


@Main.route('/move_files_to_folder', methods=['POST'])
@methods.process_large_function_route()
def move_files_to_folder():
    if request.method == 'POST':
        try:
            results = methods.move_files_to_folder(**request.form)
            return jsonify(results)
        except Exception as e:
            return errorhandler.make_error(400, str(e))



@Main.route('/load_files/', methods=['GET', 'POST', 'DELETE'])
@methods.process_large_function_route()
def load_files_landing_page():
    if request.method == 'GET':
        dir_info = methods.convert_all_to_dict(db.session.execute(sql_text(f"""
                select MAIN.*
                from (SELECT IMDIR.IMDIR_AUTO_KEY,IMDIR.PATH,IMDIR.MAIN_CATEGORY,IMDIR.SUB_CATEGORY,IMDIR.LAST_UPDATED
                    ,count(imfi.IMFI_AUTO_KEY) "FILE_COUNT"
                    
                    from {Schema}.IMAGE_DIRS IMDIR
                    left join {Schema}.IMAGES IMG on IMG.IMDIR_AUTO_KEY = IMDIR.IMDIR_AUTO_KEY
                    left join {Schema}.IMAGE_FILES IMFI ON IMFI.FILE_NAME = IMG.FILE_NAME
                    group by IMDIR.IMDIR_AUTO_KEY) MAIN
                    group by MAIN.IMDIR_AUTO_KEY,MAIN.path,MAIN.sub_category,MAIN.last_updated,MAIN."FILE_COUNT",MAIN.MAIN_CATEGORY
                order by MAIN.MAIN_CATEGORY asc,LAST_UPDATED desc
                --order by LAST_UPDATED desc
        """)))
        sorted_dirs = {}
        for dir in dir_info:
            if dir['main_category'] not in sorted_dirs:
                sorted_dirs[dir['main_category']] = []
            sorted_dirs[dir['main_category']].append(dir)

        return render_template('refresh_images_landing_page.html', folders=sorted_dirs, blueprint_name=Main.blueprint_name,current_username=methods.get_username())

    elif request.method == 'POST':
        # Require INDEX and PATH to refresh/clear. Can't just send any folder name.
        Folder = request.form.get('PATH', None)

        imdir_ak = request.form.get('IMDIR_AUTO_KEY', None)
        if imdir_ak:
            imdir_ak = int(imdir_ak)

        if Folder and imdir_ak:
            return jsonify(methods.refresh_folder_sql_db(imdir_ak, Folder))
        else:
            return errorhandler.make_error(400,
                                           f"All of the following parameters are required: INDEX, PATH, IMDIR_AUTO_KEY\nPATH:{Folder}\nIMDIR_AUTO_KEY:{imdir_ak}")
    elif request.method == 'DELETE':
        IMDIR_AUTO_KEY = request.form.get('IMDIR_AUTO_KEY', None)
        if IMDIR_AUTO_KEY:
            try:
                IMDIR_AUTO_KEY = int(IMDIR_AUTO_KEY)
            except:
                raise Exception(f"IMDIR_AUTO_KEY must be an INTEGER -- Provided: {IMDIR_AUTO_KEY}")
        PATH = request.form.get('PATH', None)
        Clear_Folder = request.form.get('CLEAR_FOLDER', False)
        if Clear_Folder.upper() == 'TRUE':
            Clear_Folder = True
        if Clear_Folder == True:
            methods.clear_folder(PATH, IMDIR_AUTO_KEY)
    return ''



@methods.process_method('refresh_folder')
@Main.route('refresh/folder', methods=['POST', 'PATCH', 'DELETE'])
def modify_folders():
    if request.method == 'POST':
        return create_new_folder(request.form.get("PATH", None), request.form.get('MAIN_CATEGORY'),
                                 request.form.get('SUB_CATEGORY'))
    elif request.method == 'PATCH':
        # Requires ORIGINAL_PATH in args

        # Can include NEW_PATH,NEW_CATEGORY, SIM_CHECK (true/false), and DUPE_CHECK (true/false).
        return jsonify(methods.update_folder(request.form))
    elif request.method == 'DELETE':
        try:
            return remove_folder(request.form.get('IMDIR_AUTO_KEY', None), request.form.get('PATH', None),
                                 request.form.get('MAIN_CATEGORY', None), request.form.get('SUB_CATEGORY', None))
        except Exception as e:
            return errorhandler.make_error(400, str(e))
    else:
        return errorhandler.make_error(400, "Invalid Request.")


def create_new_folder(PATH, MAIN_CATEGORY, SUB_CATEGORY, SIM_CHECK=False, DUPE_CHECK=False):
    # check if folder already exists in DB
    if not imagesorter_db.IMAGE_DIRS.query.filter_by(path=PATH).first():
        # check if folder exists in the system. DO NOT CREATE NEW FOLDERS IN OS
        if os.path.isdir(PATH):
            newDir = imagesorter_db.IMAGE_DIRS(path=PATH, sub_category=SUB_CATEGORY.upper(),
                                               main_category=MAIN_CATEGORY.upper(), sim_check=SIM_CHECK,
                                               dupe_check=DUPE_CHECK,
                                               last_updated=dt.datetime.now(), available_dir=True)
            db.session.add(newDir)
            db.session.commit()
            return jsonify([True, "Successfully created directory", [newDir.imdir_auto_key, dt.datetime.now()]])
        else:
            return [False, "Folder does not exist in system."]
    else:
        return [False, "Directory already in database."]


def remove_folder(IMDIR_AUTO_KEY, PATH, MAIN_CATEGORY, SUB_CATEGORY):
    folder = imagesorter_db.IMAGE_DIRS.query.filter_by(path=PATH, sub_category=SUB_CATEGORY,
                                                       main_category=MAIN_CATEGORY,
                                                       imdir_auto_key=IMDIR_AUTO_KEY).first()
    if folder:
        # Remove all images/files with this folder from the database. Does not delete the image from the OS.
        # Does not delete the data from the images, just removes them from the pool of images.
        # imagesorter_db.IMAGES.query.filter_by(source_folder=folder.path).delete()

        # imagesorter_db.IMAGES.query.filter_by(imdir_auto_key=folder.imdir_auto_key).delete()

        if imagesorter_db.IMAGES.query.filter_by(imdir_auto_key=folder.imdir_auto_key).count() > 0:
            raise Exception(
                f"Cannot remove directory with existing files. Please clear the folder prior to removing the directory.")
        else:
            db.session.execute(sql_text(f"""
                update {Schema}.IMAGE_FILES
                set AVAILABLE_FILE = false
                where not exists (
                    select 1 from {Schema}.images
                    where {Schema}.images.imdir_auto_key = {Schema}.IMAGE_FILES.imdir_auto_key
                    and upper({Schema}.images.file_name) = upper({Schema}.IMAGE_FILES.file_name)
                )
            """))

            # Remove the directory from the database (not from the OS)
            imagesorter_db.IMAGE_DIRS.query.filter_by(imdir_auto_key=folder.imdir_auto_key).delete()
            db.session.commit()
            return jsonify(True)

    return jsonify(False)





@Main.route("/")
def landing_page():
    available_categories = methods.get_available_categories()
    loadedData = {'navigator_links': Main.navigator_links}

    if 'filename' in request.args:
        loadedData['filedata'] = methods.search_files(f'#filename:{request.args.get("filename", "")}', CATEGORY='ALL')
    else:
        loadedData['filedata'] = {}

    imageBaseCommands = list(Main.imageBaseCommands)

    if methods.check_if_server_computer():
        imageBaseCommands.extend([f"""{{
                        name:'openSystem',
                        classes:['menu-item','open-system','command','clickable'],
                        innerHTML:`Open Image On System`,
                        folder:"System",
                        onclick: () => {{
                                $.ajax({{
                                type: "POST",
                                data:{{csrf_token:csrf_token}},
                                url: "{url_for(Main.blueprint_name+'.open_file_computer_base')}"+this.virtualScroller.items[this.lastFileIndex].FILE_NAME,
                                success: function(data) {{
                                    alert("File Successfully opened.")
                                }},
                                error:function(err){{
                                    alert(err.responseText);
                                }},
                                fail: function() {{
                                    alert('Database Access Denied');
                                }}
                            }})
                        }}
                    }}""",
                      f"""{{
                        name:'openSystemDirectory',
                        classes:['menu-item','open-system-directory','command','clickable'],
                        innerHTML:`Open Image Directory On System`,
                        folder:"System",
                        onclick: () => {{
                            let dict = {{
                                open_directories:'',
                                csrf_token:csrf_token,
                            }}
                            $.ajax({{
                                type: "POST",
                                data:{{csrf_token:csrf_token}},
                                url: "{url_for(Main.blueprint_name+'.load_image_directory_base')}"+encodeURIComponent(this.virtualScroller.items[this.lastFileIndex].FILE_NAME),
                                data:dict,
                                success: function(data) {{
                                    alert("Folder Successfully opened.")
                                }},
                                error:function(err){{
                                    alert(err.responseText);
                                }},
                                fail: function() {{
                                    alert('Database Access Denied');
                                }}
                            }})
                        }}
                    }}""",
                                  f"""{{
                                name:'openSystemDirectory',
                                classes:['menu-item','open-system-directory','command','clickable'],
                                innerHTML:`Refresh Thumbnail`,
                                folder:"System",
                                onclick: () => {{
                                    let dict = {{
                                        csrf_token:csrf_token,
                                    }}
                                    $.ajax({{
                                        type: "POST",
                                        data:{{csrf_token:csrf_token}},
                                        url: "{url_for(Main.blueprint_name+'.remove_thumbnails_base')}"+encodeURIComponent(this.virtualScroller.items[this.lastFileIndex].FILE_NAME),
                                        data:dict,
                                        success: function(data) {{
                                            alert("Thumbnails successfully deleted. Please refresh to see the changes immediately.")
                                        }},
                                        error:function(err){{
                                            alert(err.responseText);
                                        }},
                                        fail: function() {{
                                            alert('Database Access Denied');
                                        }}
                                    }})
                                }}
                            }}"""
                                  ])


    return render_template('main_v6.html', categories=available_categories,
                           max_thumbnail=Main.thumbnail_maxSize,
                           thumbnail_sizes=[Main.thumbnail_small,Main.thumbnail_medium,Main.thumbnail_large],
                           isServerComputer=methods.check_if_server_computer(),
                           selectedImageCommands=Main.selectedImageCommands,
                           imageBaseCommands=imageBaseCommands,
                           user_custom_search_data=methods.load_user_config('custom_search'),
                           user_custom_ui_layout=methods.get_user_custom_layouts(),
                           current_username=methods.get_username(),
                           blueprint_name=Main.blueprint_name,
                           **loadedData)






@Main.route('/available_folders/', methods=['GET'])
def get_available_folders():
    return jsonify(methods.get_available_folders())

@Main.route('/open_file_computer/', methods=['POST'])
def open_file_computer_base():
    return ''


@Main.route('/open_file_computer/<filenames>', methods=['POST'])
@methods.only_on_server_system()
def open_file_computer(filenames):
    assert len(filenames.split('|')) < 5, 'Only 5 images at a time may be opened on the system.'
    for filename in filenames.split('|'):
        files = imagesorter_db.IMAGES.query.filter_by(file_name=filename).all()
        if len(files) > 0:
            for file in files:
                try:
                    os.startfile(file.path)
                except Exception as e:
                    raise e
        else:
            return errorhandler.make_error(400,"No files found.")
    return ''

@Main.route('/remove_thumbnails/',methods=['POST'])
def remove_thumbnails_base():
    return ''

@methods.only_on_server_system()
@Main.route('/remove_thumbnails/<filename>',methods=['POST'])
def remove_thumbnails(filename):
    IMFI_Record = imagesorter_db.IMAGE_FILES.query.filter_by(file_name=filename).first()
    if IMFI_Record:
        methods.remove_thumbnails(imfi_record=IMFI_Record,thumbnail_sizes=Main.thumbnail_sizes)
        imagesorter_db.update_file_data(IMFI_Record.imfi_auto_key)
        db.session.commit()
        return jsonify(True)
    else:
        return errorhandler.make_error(400,f"File not found: {filename}")


@Main.route('/tags/create', methods=['POST'])
def createtag():
    if request.form.get('TAG', None) is not None:
        tagValue = request.form.get('TAG')
        tagsplit = tagValue.split(':')
        if len(tagsplit) != 2 or ',' in tagValue:
            return errorhandler.make_error(400, "Tags may not contain commas (,) or more than one colon (:).")
        return jsonify(methods.create_new_tag(tag_category=tagsplit[0],tag_detail=tagsplit[1]))
    return jsonify(False)


@Main.route('/tags/edit', methods=['POST', 'DELETE'])
@methods.process_large_function_route('tags')
def edit_tags():

    if request.method == 'POST':
        return jsonify(methods.edit_tags('ADD',**request.form))
    elif request.method == 'DELETE':
        return jsonify(methods.edit_tags('REMOVE', **request.form))



@Main.route('/get_files', methods=['GET'])
def get_files():
    return methods.search_files(request.args.get('SEARCH', ''), int(request.args.get('LIMIT', '200')),
                                int(request.args.get('PAGE', '0')),
                                MAIN_CATEGORY=request.args.get('MAIN_CATEGORY', ''),
                                SUB_CATEGORY=request.args.get('SUB_CATEGORY', ''),
                                RANDOM_SEED=request.args.get("RANDOM_SEED", None),
                                QUERY=request.args.get('QUERY', None),
                                NO_COUNT=request.args.get('NO_COUNT', None),
                                PREVIEW_RESULTS=request.args.get('PREVIEW_RESULTS', None))

@Main.route('/search_tags/', methods=['GET'])
# @methods.process_route('search_tags')
def search_tags():
    DATA = methods.search_tags(**request.args)
    if isinstance(DATA,list) or isinstance(DATA,dict):
        return jsonify(DATA)




@Main.route('/duplicates/process/', methods=['POST'])
@methods.process_large_function_route()
def duplicates_process():
    update_file_data_imfi_ak = []
    if request.form.get('TYPE',None) == 'DUPLICATES':
        def rename_duplicate_file(duplicate_images_records, index, new_filename,img_auto_key):
            """
            :param duplicate_images_records: - List of the same image from the DB. Same filename.
            :param index:  - Index of the specific file in the list of IMAGES you wish to change
            :param new_filename: - new filename for the selected file
            :return:
            """

            if len(duplicate_images_records) > 1:
                IMAGE = duplicate_images_records[index]
                if IMAGE.img_auto_key == img_auto_key:
                    old_filename = str(IMAGE.file_name)
                    if '.' not in new_filename:
                        return errorhandler.make_error(400,
                                                       f"File must contain an extension (ie: filename.{IMAGE.file_extension})")
                    if new_filename.split('.')[-1] != IMAGE.file_extension:
                        return errorhandler.make_error(400,
                                                       f"Please include the same extension for the file: {IMAGE.file_extension}")
                    existingImageCheck = imagesorter_db.IMAGES.query.filter_by(file_name=new_filename).first()
                    if existingImageCheck:
                        return errorhandler.make_error(400,
                                                       f"File name || {new_filename} || for || INDEX: {index} || already exists. Please select a new name")
                    try:
                        shutil.move(IMAGE.path, IMAGE.path.rstrip(old_filename) + new_filename)
                        IMAGE.file_name = new_filename
                        IMAGE.path = IMAGE.path.rstrip(old_filename) + new_filename
                    except Exception as e:
                        db.session.rollback()
                        raise e

                    imfi_auto_key = methods.get_imfi_auto_key(new_filename)  # Create IMFI_RECORD for new file
                    update_file_data_imfi_ak.append(imfi_auto_key)
                    # imagesorter_db.update_missing_imfi_or_imtag_precalc()
                    # imagesorter_db.update_file_data(imfi_auto_key)
                    # db.session.commit()
                else:
                    return errorhandler.make_error(400, "Invalid index img_auto_key combination.")

        def remove_duplicate_image(duplicate_images_records, index,img_auto_key):
            """
            :param duplicate_images_records: - list of records of the same filename
            :param index: - index for the image that will be removed from the system
            :return:
            """


            if len(duplicate_images_records) > 1:
                # Moves the file into the dupe folder (after adding a random number to it?) ex: filename.jpg (datetime)
                image = duplicate_images_records[index]
                # print(image.img_auto_key,img_auto_key,image.original_date,image.file_name)

                try:
                    if image.img_auto_key == img_auto_key:
                        new_note = imagesorter_db.IMAGE_NOTES(imfi_auto_key=methods.get_imfi_auto_key(image.file_name),
                                                              notes=f"FILE DELETED. ORIGINAL FILE SAVE DATE: {image.original_date}",
                                                              timestamp=dt.datetime.now())

                        db.session.add(new_note)
                        local_imdir = methods.local_image_directory_db(
                            imagesorter_db.IMAGE_DIRS.query.filter_by(imdir_auto_key=image.imdir_auto_key).first())
                        try:
                            shutil.move(image.path,
                                        Main.server_settings['folder']['pending_removal_duplicates'].rstrip('\\') + f'\\{filename} {str(dt.datetime.now()).replace(":", ".")}.{image.file_extension}')
                        except Exception as e:
                            if not os.path.isfile(image.path):
                                pass
                            else:
                                raise e

                        local_imdir.remove_file_from_db(image.file_name, image.path)
                        imagesorter_db.IMAGES.query.filter_by(img_auto_key=image.img_auto_key).delete()
                    else:
                        return errorhandler.make_error(400,"Invalid index img_auto_key combination.")

                except Exception as e:
                    db.session.rollback()
                    raise e
            pass

        if request.form.get('PARENT', None):
            try:
                PARENT_INDEX = int(request.form.get('PARENT_INDEX'))
            except Exception as e:
                raise Exception("PARENT_INDEX Must be included and be an index within the range of files provided.")
            HEADER = imagesorter_db.IMAGES.query.filter_by(
                file_name=request.form.get('PARENT')).first()
            IMAGES = imagesorter_db.IMAGES.query.filter_by(file_name=request.form.get('PARENT')).order_by(
        imagesorter_db.IMAGES.original_date.desc()).all()
            if HEADER:
                if request.form.get('PARENT', None):
                    pass
                    filename = request.form.get('PARENT')
                    for dupeIndex,index in enumerate(request.form.get('RENAME', '').split("|*|")):
                        if index:
                            renameSplit = index.split('|!|')
                            index = int(renameSplit[0])
                            img_ak_list = request.form.get('RENAME_IMG_AK', '').split(',')
                            rename_duplicate_file(IMAGES, index, renameSplit[1],int(img_ak_list[dupeIndex]))

                    for dupeIndex,index in enumerate(request.form.get('DELETE').split("|*|")):

                        index = int(index)
                        img_ak_list = request.form.get('DELETE_IMG_AK', '').split(',')
                        if index > -1:
                            try:
                                remove_duplicate_image(IMAGES, index,int(img_ak_list[dupeIndex]))
                                # db.session.commit()
                            except Exception as e:
                                db.session.rollback()
                                raise e
                    # parent index
                    IMFI_Record = imagesorter_db.IMAGE_FILES.query.filter_by(file_name=filename).first()
                    if IMFI_Record:
                        IMFI_Record.frames = None
                        IMFI_Record.dimension_width = None
                        IMFI_Record.dimension_height = None
                        IMFI_Record.original_date = IMAGES[PARENT_INDEX].original_date
                        if IMFI_Record.path != IMAGES[PARENT_INDEX].path:
                            IMFI_Record.path = IMAGES[PARENT_INDEX].path
                        methods.remove_thumbnails(IMFI_Record, Main.thumbnail_sizes)
                    db.session.commit()

                    imagesorter_db.update_missing_imfi_or_imtag_precalc()
                    # imagesorter_db.update_all_file_data()
                    update_file_data_imfi_ak.append(methods.get_imfi_auto_key(request.form.get('PARENT')))
                    for imfi_ak in update_file_data_imfi_ak:
                        imagesorter_db.update_file_data(imfi_ak)

    return ''




@Main.route('/duplicates/', methods=['GET'])
def duplicate_images():  # Duplicate filenames
    """
    Webpage that allows you to remove duplicate images.
        Duplicates are the following:
            Files with the same filename
    :return:
    """

    file = request.args.get('FILENAME', None)

    file_like = ''
    query_dict = {}
    if file:
        file_like = 'AND FILE_NAME like :FILENAME'
        query_dict['FILENAME'] = file + '%'

    where_private = 'and IMFI.PRIVATE = false'

    if 'PRIVATE' in request.args:
        where_private = ''

    dupeFiles = methods.convert_all_to_dict(db.session.execute(sql_text(f"""
        select MAIN.FILE_NAME,MAIN.FILE_COUNT,MAIN.IMG_AUTO_KEYS from (
        SELECT IMG.FILE_NAME,count(IMG.FILE_NAME) as FILE_COUNT, max(IMG.ORIGINAL_DATE) as ORIGINAL_DATE, STRING_AGG(IMG.IMG_AUTO_KEY::text,',' order by IMG.ORIGINAL_DATE desc) as IMG_AUTO_KEYS   
                from {Schema}.IMAGES IMG
                inner join {Schema}.IMAGE_FILES IMFI on IMG.FILE_NAME = IMFI.FILE_NAME
                where IMG.AVAILABLE_FILE = True
                {where_private}
                {file_like}
                group by IMG.FILE_NAME ) MAIN
                where MAIN.file_count > 1
                order by MAIN.ORIGINAL_DATE asc
        limit 100
        """), query_dict))
    dupeFilesCount = methods.convert_all_to_dict(db.session.execute(sql_text(f"""
            select count(*) "COUNT" from (
            SELECT IMG.FILE_NAME,count(IMG.FILE_NAME) as FILE_COUNT, max(IMG.ORIGINAL_DATE) as ORIGINAL_DATE
                    from {Schema}.IMAGES IMG
                    inner join {Schema}.IMAGE_FILES IMFI on IMG.FILE_NAME = IMFI.FILE_NAME
                    where IMG.AVAILABLE_FILE = True
                    {where_private}
                    {file_like}
                    group by IMG.FILE_NAME ) MAIN
                    where MAIN.file_count > 1
            """), query_dict))

    totalDupes = dupeFilesCount[0]['COUNT']
    return render_template('image_duplicates.html', duplicates=dupeFiles, totalDupes=totalDupes, blueprint_name=Main.blueprint_name)


@Main.route('/duplicates/load_image/<filename>/<int:index>', methods=['GET'])
def load_duplicate_image(filename, index):
    """
    Returns the image/file for files with the same filename using the index provided.
    :param filename:
    :param index:
    :return:
    """
    files = imagesorter_db.IMAGES.query.filter_by(file_name=filename).order_by(
        imagesorter_db.IMAGES.img_auto_key.desc()).all()

    if files:
        if index + 1 > len(files):
            return errorhandler.make_error(400,
                                           f'OUT OF INDEX. TOTAL DUPLICATE FILES: {len(files)} - GIVEN INDEX: {index}')
        if request.args.get('size', None) != 'full':
            thumb = methods.get_thumbnail(files[index], (400, 400), True)
            if 'PATH' in thumb:
                return send_file(thumb['PATH'])
            return send_file(BytesIO(thumb['BYTES']), mimetype=thumb['MIMETYPE'])
        else:
            return send_file(files[index].path, files[index].mimetype)
    return ''


@Main.route('/duplicates/load_image/', methods=['GET'])
def load_duplicate_image_base():
    """
    Used for url_for() to create the base of the url.
    :return:
    """
    return ''

@Main.route('/duplicates/load_data/')
def load_duplicate_image_data_base():
    """
    Used for url_for() to create the base of the url.
    :return:
    """
    return ''

@methods.process_method('load_duplicate_image_data')
@Main.route('/duplicates/load_data/<filename>/<int:index>', methods=['GET'])
def load_duplicate_image_data(filename, index):
    """
    Returns the data from the image, such as the shape, last modified date, etc.
    :param filename:
    :param index:
    :return:
    """
    files = imagesorter_db.IMAGES.query.filter_by(file_name=filename).order_by(
        imagesorter_db.IMAGES.img_auto_key.desc()).all()
    if files:
        selectedFileRecord = files[index]
        if index > len(files):
            return errorhandler.make_error(400,
                                           f'OUT OF INDEX. TOTAL DUPLICATE FILES: {len(files)} - GIVEN INDEX: {index}')
        try:
            data = methods.get_image_file_data(selectedFileRecord)
        except:
            data = {}

        data['FULL_PATH'] = selectedFileRecord.path
        return jsonify(data)
    return ''



@methods.process_method('load_random_image')
@Main.route('/load_image/random', methods=['GET'])
def load_random_image():
    """
    :return: - A random image from the database
    """
    images = db.session.execute(
        sql_text(f"SELECT path from {Schema}.images order by random() limit 1")).fetchone()
    if images:
        return send_file(images[0])
    else:
        return jsonify(False)

@methods.process_method('load_image_directory')
@methods.only_on_server_system()
@Main.route('load_image_directory/<file_name>',methods=['POST'])
def load_image_directory(file_name):
    if request.method == 'POST':
        """
        Opens the folder(s) that contains the file. 
        This is intended to be used on the host computer. 
        """
        filename = file_name
        file = imagesorter_db.IMAGES.query.filter_by(file_name=filename).all()
        if file:
            if 'open_directories' in request.form:
                for f in file:
                    subprocess.Popen(["explorer", "/select,", os.path.normpath(f.path)], shell=True)
                return jsonify(True)
        else:
            return errorhandler.make_error(400,f"Provided file does not exist. File: {filename}")


@methods.process_method('load_image')
@Main.route('/load_image/<file_name>', methods=['GET'])
# @limiter.limit('500/second')
def load_image_file(file_name):
    """
    Returns an image based on the filename
    :param file_name:
    :return:
    """
    if request.method == 'GET':
        return methods.load_image_file(file_name,**request.args)
    return ''


@Main.route("/load_image_directory/")
def load_image_directory_base():
    """
    Used for url_for() to create the base of the url.
    :return:
    """
    return ''

@Main.route('/load_image/')
def load_image_file_base():
    """
    Used for url_for() to create the base of the url.
    :return:
    """
    return ''





@Main.route('/load/tag_information', methods=['GET'])
def load_tag_information():
    """
    :return: - Returns information about all the available tags.
        Includes:
            Each category
            The count for how many times this category has been used total
            Each detail within a category
            How many times a tag has been used
            LAST_USED date
            The TAG_AUTO_KEY
            and the TIMESTAMP for when the tag was created
    """
    return jsonify(methods.load_tag_information(**request.args))



@Main.route('/rename_file/',methods=['POST'])
def rename_file_base():
    pass

@Main.route('/rename_file/<filename>',methods=['POST'])
def rename_file(filename):
    if request.method == 'POST':
        imfi_auto_key = request.form.get('IMFI_AUTO_KEY',None)
        if imfi_auto_key is None:
            return errorhandler.make_error(400,"IMFI_AUTO_KEY is required.")
        new_filename = request.form.get('NEW_FILENAME',None)
        if new_filename is None:
            return errorhandler.make_error(400, "NEW_FILENAME is required.")
        try:
            return jsonify(methods.rename_file(int(imfi_auto_key),filename,new_filename))
        except Exception as e:
            return errorhandler.make_error(400,e)
    pass

@Main.route('/tags/info/search', methods=['GET'])
def tag_info_search():
    results = methods.search_tag_info(**request.args)
    try:
        return jsonify(results)
    except Exception as e:
        return errorhandler.make_error(400, str(e))


@Main.route('/tags/info/', methods=['GET'])
def tag_info():
    if tag_auto_key := request.args.get('TAG_AUTO_KEY', None):
        results = methods.get_tag_details(tag_auto_key,**request.args)
        return jsonify(results)
    else:
        return errorhandler.make_error(400, "TAG_AUTO_KEY must be supplied.")


@Main.route('/tags/', methods=['GET', 'PATCH', 'DELETE'])
def tag_view():
    if request.method == 'GET':
        """    
        :return:
        This is to view the tags available, and possibly edit them
        """

        return render_template(f'tag_view.html',
                               available_categories=methods.get_available_categories(),
                               blueprint_name=Main.blueprint_name,
                               current_username=methods.get_username())
    elif request.method == 'PATCH':
        if request.form.get('TYPE',None) == 'NOTE_UPDATE':
            return jsonify(methods.update_tag_notes(**request.form))
        else:
            return jsonify(methods.update_tag_data(**request.form))
    elif request.method == 'DELETE':
        return jsonify(methods.remove_tag(**request.form))


@Main.route('/sequence/get_names', methods=['GET'])
def get_sequence_names():
    """
    This just returns sequence names.
    :param sequence:
    :return:
    """
    sequence = request.args.get('sequence')

    SEQUENCES = methods.convert_all_to_dict(db.session.execute(sql_text(
        f"""select distinct SEQUENCE_NAME from {Schema}.SEQUENCE where SEQUENCE_NAME ilike :NAME order by SEQUENCE_NAME asc limit 50 """),
        {'NAME': f'%{sequence}%'}))
    return jsonify([x['sequence_name'] for x in SEQUENCES])


@Main.route('/sequence/get_sequence/<filename>', methods=['GET'])
def get_sequence(filename):
    return jsonify(methods.load_sequence(filename))
    pass


@Main.route('/sequence/get_files/', methods=['GET'])
def get_sequence_files_base():
    return ''


@Main.route('/sequence/get_files/<sequence_name>', methods=['GET'])
def get_sequence_files(sequence_name):
    return jsonify(methods.load_sequence(sequence=sequence_name))
    pass





@Main.route('/file_notes/update/', methods=['POST'])
def update_image_notes():
    if request.method == 'POST':
        methods.update_image_notes(request.json)
        return request.json





@Main.route('/sequence/data', methods=['POST', 'PATCH', 'DELETE'])
def sequence_data():
    """
    This is used to create/update/remove image SEQUENCES (comics, or slight changes)
    :return:
    """
    if request.method == 'POST':
        try:
            return jsonify(methods.update_or_create_sequence(**request.form))
        except Exception as e:
            return errorhandler.make_error(400,e)
    elif request.method == 'DELETE':
        if (delete_type:=request.form.get('DELETE_TYPE','')) in ['SEQUENCE_NAME','IMFI_AK']:
            if delete_type == 'SEQUENCE_NAME':
                return jsonify(methods.delete_sequence_by_name(**request.form))
            elif delete_type == 'IMFI_AK':
                return jsonify(methods.delete_sequence_by_imfi_ak(**request.form))
    return jsonify(False)


@Main.route('/user/custom_search_data',methods=['GET','POST'])
def user_custom_search_data():
    if request.method == 'GET':
        return jsonify(methods.load_user_config('custom_search'))
    elif request.method == 'POST':
        if UPDATE_DATA:=request.form.get('UPDATE_DATA',None):
            return jsonify(methods.update_user_config('custom_search',json.loads(UPDATE_DATA))) #convert from string


@Main.route('/user/custom_ui_layout',methods=['GET','POST'])
def user_custom_ui_layout():
    if request.method == 'GET':
        return jsonify(methods.get_user_custom_layouts())
    elif request.method == 'POST':
        if UPDATE_DATA := request.form.get('UPDATE_DATA', None):
            return jsonify(methods.update_user_config('custom_ui_layout', json.loads(UPDATE_DATA)))  # convert from string


# @Main.errorhandler(Exception)
# def bad_request(error):
#     return errorhandler.make_error(500,"Error: " + str(error))





@Main.route('/pending_removal/', methods=['GET', 'DELETE'])
@methods.process_large_function_route()
def pending_removal():
    if request.method == 'DELETE':
        files = request.form['files'].split('|')
        if files:
            print("Removing Files...")
            methods.resetLoadingStatus('remove_files')
            pbar = tqdm(files)
            for f in pbar:
                methods.updateLoadingStatus('remove_files', pbar)
                if methods.checkLoadingOperationCancelled('remove_files') is True:
                    break
                try:
                    if len(imagesorter_db.IMAGES.query.filter_by(file_name=os.path.split(f)[-1]).all()) == 1:
                        image_file = imagesorter_db.IMAGE_FILES.query.filter_by(file_name=os.path.split(f)[-1]).first()

                        methods.remove_image_by_imfi_ak(image_file.imfi_auto_key)
                    else:
                        return errorhandler.make_error(400,
                                                       f'Please remove all duplicate filenames before removing this file: {os.path.split(f)[-1]}')
                except Exception as e:
                    db.session.rollback()
                    raise e
                # imagesorter_db.IMAGES.query.filter_by(PATH=f).delete()
                # imagesorter_db.THUMBNAILS.query.filter_by(FILE_NAME=f.split('/')[-1]).delete()
                db.session.commit()
            methods.updateLoadingStatus('remove_files', pbar)
            return 'file(s) removed.'
    elif request.method == 'GET':

        try:page = int(request.args.get('page',1))
        except:page=1
        try:pagesize = int(request.args.get('pagesize',200))
        except:pagesize=200


        pending_deletion = methods.convert_all_to_dict(db.session.execute(sql_text(f"""SELECT * from {Schema}.TAGGED_IMAGES IMTAG
            inner join {Schema}.TAGS TAG on IMTAG.TAG_AUTO_KEY = TAG.TAG_AUTO_KEY and TAG.CATEGORY = 'GENERAL' and TAG.DETAIL = 'DELETE'
            inner join {Schema}.IMAGE_FILES IMFI on IMFI.IMFI_AUTO_KEY = IMTAG.IMFI_AUTO_KEY
            inner join {Schema}.IMAGES IMG on IMG.FILE_NAME = IMFI.FILE_NAME and IMG.AVAILABLE_FILE = true
            --order by IMFI.FILE_NAME ASC 
            limit :pagesize offset :offset
            """), {'offset': (pagesize * (page - 1)), 'pagesize': pagesize}))
        DATA = []
        if pending_deletion:
            for x in pending_deletion:
                DATA.append([x['file_name'], x['path']])
        return render_template(f'pending_removal.html', DATA=DATA, blueprint_name=Main.blueprint_name,current_username=methods.get_username())




@Main.route('/static/<filename>',methods=["GET"])
def load_required_file(filename):
    try:return send_from_directory(f'{Main.static_folder}/required_files/',filename)
    except:return 'File Not Found.'

@Main.route('/static/<folder>/<filename>',methods=['GET'])
def load_required_file_from_folder(folder,filename):
    #print(Main.static_folder)
    try:
        try:return send_from_directory(f'{Main.static_folder}/required_files/{folder}', filename,mimetype=mimetypes.MimeTypes['.{}'.format(filename.split('.')[-1])][-1])
        except:return send_from_directory(f'{Main.static_folder}/required_files/{folder}', filename)
    except:
        return 'File Not Found.'


# @methods.only_on_server_system()
@methods.process_method('system_config')
@Main.route('/options/',methods=['GET','POST'])
def options_menu():
    if request.method == 'GET':
        ##Should I do extensions here instead?
        return render_template('options_menu.html',blueprint_name=Main.blueprint_name,current_username=methods.get_username(),server_options=Main.server_settings['options'])
    elif request.method == 'POST':
        for option_update in request.json:
            if option_update[0] in Main.server_settings['options']:
                Main.server_settings['options'][option_update[0]] = option_update[1]
        save_server_options(Main.server_settings['options'])
        return ''
    else:
        return ''

@Main.route("/extensions",methods=['GET','POST'])
def extensions_menu():
    if request.method == 'GET':
        return render_template('extensions_menu.html',blueprint_name=Main.blueprint_name,current_username=methods.get_username(),available_extensions=available_extensions)
    elif request.method == 'POST':
        return '' ##Todo - used to update or install extensions
    else:
        return ''