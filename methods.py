import configparser
import datetime as dt
import json
import math
import mimetypes
import os
import pathlib
import random as rng
import re
import shutil
import sqlite3 as sql
import time
from functools import wraps
from io import BytesIO

import cv2
from PIL import Image
from flask import request,send_file,jsonify
from flask_login import current_user
from tqdm import tqdm

from imports import errorhandler
from imports.database import db
from . import db as imagesorter_db
from .base_settings import Main, Schema
from sqlalchemy import text as sql_text
import socket

Main.thumbnail_generation_queue = []
Main.thumbnail_generation_queue_count = 0
Main.process_method_wrapped = {}

def process_method(argument):
    def decorator(method_function):
        if argument not in Main.process_method_wrapped:
            '''Adds the argument to a viewable list from the debug menu. To see what's in use.'''
            Main.process_method_wrapped[argument] = {'Function': None,
                                                'Module': None,
                                                'Active': {'preprocess': [], 'postprocess': []}}
        Main.process_method_wrapped[argument].update({'Function':method_function.__name__,'Module':method_function.__module__})
        @wraps(method_function)
        def wrapper(*args, **kwargs):
            # Perform post-processing using the passed argument
            for method in Main.extension_preprocess.get(argument, []):
                result = method(*args, **kwargs)
                if result is not None:
                    return result

            original_result = method_function(*args, **kwargs)

            for method in Main.extension_postprocess.get(argument, []):
                result = method(*args, **kwargs)
                if result is not None:
                    return result
            return original_result

        return wrapper

    return decorator

def only_on_server_system():
    "Only allows the method to run if on the same system as the server."
    def decorator(method_function):
        @wraps(method_function)
        def wrapper(*args, **kwargs):
            if check_if_server_computer():
                return method_function(*args, **kwargs)
            else:
                return errorhandler.make_error(403,"Method not being run on the host system. Denied.")


        return wrapper

    return decorator

# def process_large_function_route2(lockIdentifiers=['general'],methods=['POST','PATCH','DELETE']):
#     """
#         This decorator, when used on a method will return an error if the Main.Intense_Procedure_Lock is activated.
#         Otherwise, if the lock is not in place, it sets the lock
#         :param function_name:
#         :return:
#         """
#
#     def decorator(route_function):
#         @wraps(route_function)
#         def wrapper(*args, **kwargs):
#             # Perform post-processing using the passed argument
#             try:
#                 if request.method in methods:
#                     try:
#                         currTime = dt.datetime.now().strftime('%Y-%m-%d - %I:%M:%S %p') #Key
#                         #Load key into the lockIdentifiers.
#                         startNewLargeProcessesCheck2(lockIdentifiers,currTime)
#                         #Creates a datetime element for a key?
#                     except Exception as e:
#                         releaseNewLargeProcesses(lockIdentifiers,currTime)
#                         return errorhandler.make_error(500, str(e))
#                     lockNewLargeProcesses2(lockIdentifiers,currTime)
#                     original_result = route_function(*args, **kwargs)
#                     releaseNewLargeProcesses2(lockIdentifiers,currTime)
#                     return original_result
#                 else:
#                     original_result = route_function(*args, **kwargs)
#                     return original_result
#             except Exception as e:
#                 releaseNewLargeProcesses2(lockIdentifiers,currTime)
#                 raise e
#
#         return wrapper
#
#     return decorator

def process_large_function_route(lockIdentifier='general', methods=['POST', 'PATCH', 'DELETE']):
    """
    This decorator, when used on a method will return an error if the Main.Intense_Procedure_Lock is activated.
    Otherwise, if the lock is not in place, it sets the lock
    :param function_name:
    :return:
    """

    def decorator(route_function):
        @wraps(route_function)
        def wrapper(*args, **kwargs):
            # Perform post-processing using the passed argument
            try:
                if request.method in methods:
                    try:
                        startNewLargeProcessesCheck(lockIdentifier)
                    except Exception as e:
                        releaseNewLargeProcesses(lockIdentifier)
                        return errorhandler.make_error(500, str(e))
                    lockNewLargeProcesses(lockIdentifier)
                    original_result = route_function(*args, **kwargs)
                    releaseNewLargeProcesses(lockIdentifier)
                    return original_result
                else:
                    original_result = route_function(*args, **kwargs)
                    return original_result
            except Exception as e:
                releaseNewLargeProcesses(lockIdentifier)
                raise e

        return wrapper

    return decorator

def check_debug(debug_string=None):
    debug = Main.config_data.get('debug_messages', False)
    if isinstance(debug,bool):
        return debug
    elif isinstance(debug,str):
        debug = debug.split(',')
    if isinstance(debug,list) and debug_string in debug:
        return True
    else:
        return False


def check_if_server_computer():
    server_ip = socket.gethostbyname(socket.gethostname())
    user_ip = request.remote_addr
    if server_ip == user_ip or user_ip == '127.0.0.1':
        return True
    else:
        return False

def get_imfi_auto_key(filename):
    """
    Returns the IMFI_AUTO_KEY based on the filename
    :param filename:
    :return:
    """
    file = imagesorter_db.IMAGE_FILES.query.filter_by(file_name=filename).first()
    if file:
        return file.imfi_auto_key
    else:
        img_record = imagesorter_db.IMAGES.query.filter_by(file_name=filename).first()
        if img_record:
            # create IMAGE_FILES record
            db.session.add(imagesorter_db.IMAGE_FILES(file_name=img_record.file_name, available_file=True,
                                                      private=False,
                                                      original_date=img_record.original_date, file_count=1,
                                                      date_added=dt.datetime.now(), path=img_record.path,
                                                      mimetype=img_record.mimetype,
                                                      imdir_auto_key=img_record.imdir_auto_key))
        return None


def convert_all_to_dict(cursor_results):
    cursor = cursor_results.cursor
    desc = cursor.description
    column_names = [col[0] for col in desc]
    return [dict(zip(column_names, row))
            for row in cursor.fetchall()]

def convert_fetch_to_dict(results,fetch_count):
    cursor = results.cursor
    desc = cursor.description
    column_names = [col[0] for col in desc]
    return [dict(zip(column_names, row))
            for row in cursor.fetchmany(fetch_count)]

def get_thumbnail(File, resize=None, override_thumbnail=False):
    if Main.config_data.get('disable_thumbnail_generation', False) is True:
        override_thumbnail = True

    Filepath = File.path
    if not resize:
        resize = (800, 800)

    filename = '.'.join(Filepath.split('/')[-1].split('.')[:-1])
    extension = Filepath.split('.')[-1]

    def get_max_available_thumbnail(resize):
        largestSize = 0
        for thumbnail_size in Main.thumbnail_sizes:
            if resize[0] <= thumbnail_size and resize[1] <= thumbnail_size and largestSize < thumbnail_size:
                largestSize = thumbnail_size
            if largestSize > 0:
                return largestSize
        return Main.thumbnail_maxSize

    thumbnail_size = get_max_available_thumbnail(resize)

    try:
        if extension.lower() in ['jpg', 'jpeg', 'webp', 'gif', 'png']:
            if override_thumbnail or (Main.thumbnail_maxSize < resize[0] and Main.thumbnail_maxSize < resize[1]):
                img = Image.open(Filepath, "r")
            else:
                try:
                    if os.path.isfile(
                            f"{Main.server_settings['folder']['thumbnails']}/{get_imfi_auto_key(File.file_name)}_{thumbnail_size}_{thumbnail_size}.{extension}"):
                        if (thumbnail_size, thumbnail_size) == resize:
                            return {
                                'PATH': f"""{Main.server_settings['folder']['thumbnails']}/{get_imfi_auto_key(File.file_name)}_{thumbnail_size}_{thumbnail_size}.{extension}"""}
                        img = Image.open(
                            f"{Main.server_settings['folder']['thumbnails']}/{get_imfi_auto_key(File.file_name)}_{thumbnail_size}_{thumbnail_size}.{extension}",
                            "r")

                    else:

                        img = create_thumbnail(Main.server_settings['folder']['thumbnails'], File, thumbnail_size,
                                               extension)
                except Exception as e:
                    print(e)
                    img = Image.open(Filepath, "r")
            img_format = img.format

            if (thumbnail_size, thumbnail_size) == resize and override_thumbnail is False:
                return {
                    'PATH': f"""{Main.server_settings['folder']['thumbnails']}/{get_imfi_auto_key(File.file_name)}_{thumbnail_size}_{thumbnail_size}.{extension}"""}

            img.thumbnail(resize)
            mime = mimetypes.guess_type('{}.{}'.format(filename, extension))
            if mime:
                # print(__name__,'MIME',mime)
                mime = mime[0]
            if not mime:
                if extension.upper() == 'WEBP':
                    mime = 'image/webp'
            stream = BytesIO()
            img.save(stream, format=img_format)
            thumbnail = {'BYTES': stream.getvalue(), 'MIMETYPE': mime}
            try:
                return thumbnail
            except Exception as e:
                stream.close()
                raise e
        elif extension.lower() in ['mp4','webm']:

            if os.path.isfile(
                    f"{Main.server_settings['folder']['thumbnails']}/{get_imfi_auto_key(File.file_name)}_{thumbnail_size}_{thumbnail_size}.png"):
                img = Image.open(
                    f"{Main.server_settings['folder']['thumbnails']}/{get_imfi_auto_key(File.file_name)}_{thumbnail_size}_{thumbnail_size}.png",
                    "r")
            else:
                # Get the image halfway through the video
                video = cv2.VideoCapture(File.path)
                # Get the total number of frames and frame rate
                num_frames = int(video.get(cv2.CAP_PROP_FRAME_COUNT))

                # Calculate the time halfway through the video
                thumbnail_time = num_frames // 2

                # Set the frame position to the thumbnail time
                video.set(cv2.CAP_PROP_POS_FRAMES, thumbnail_time)

                # Read the frame at the thumbnail time
                success, frame = video.read()

                # Save the frame as the thumbnail image
                img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
                img.thumbnail((thumbnail_size, thumbnail_size))
                if override_thumbnail is False:
                    img.save(
                        f"{Main.server_settings['folder']['thumbnails']}/{get_imfi_auto_key(File.file_name)}_{thumbnail_size}_{thumbnail_size}.png",
                        img.format)
                # Release the video file
                video.release()

            mime = mimetypes.guess_type('{}.{}'.format(filename, extension))
            # img_format = img.format
            mime = mime[0]
            stream = BytesIO()
            img.save(stream, format='PNG')
            thumbnail = {'BYTES': stream.getvalue(), 'MIMETYPE': mime}
            try:
                return thumbnail
            except Exception as e:
                stream.close()
                raise e


    except Exception as e:
        raise e


def create_thumbnail(folder, file_record, thumbnail_size, extension):
    fileKey = dt.datetime.now().strftime('%Y.%m.%d.%H.%M.%S.%f')+file_record.file_name
    thumbnailPath = f"{folder}/{get_imfi_auto_key(file_record.file_name)}_{thumbnail_size}_{thumbnail_size}.{extension}"
    Main.thumbnail_generation_queue.insert(2,fileKey)
    Main.thumbnail_generation_queue_count+=1
    timeoutCounter = 0
    sleepTimer = 0.01

    try:
        while fileKey != Main.thumbnail_generation_queue[0]:
            time.sleep(sleepTimer)
            timeoutCounter+=1
            if timeoutCounter % 100 == 0 and timeoutCounter < 1000:
                sleepTimer+=0.005

            if timeoutCounter > 30000:
                Main.thumbnail_generation_queue.remove(fileKey)
                raise Exception("Thumbnail Creation Timeout")
            # elif timeoutCounter % 50 == 0 and timeoutCounter != 0: #Every 5 seconds
            #     if Main.thumbnail_generation_queue.index(fileKey) == -1:
            #         noThumbnail = True
            #         break
        Main.thumbnail_generation_queue_count -= 1
    except Exception as e:
        Main.thumbnail_generation_queue_count-=1
        raise e



    try:
        disable_thumbnail_generation = Main.config_data.get('disable_thumbnail_generation', False)
        img = Image.open(file_record.path, "r")
        if os.path.isfile(thumbnailPath):
            if Main.thumbnail_generation_queue[0] == fileKey:
                Main.thumbnail_generation_queue = Main.thumbnail_generation_queue[1::]
            return get_thumbnail(img,thumbnail_size)

        # if img.size[0] + img.size[1] > Main.thumbnail_maxSize:  # Max size of 1000 x 1000

        if thumbnail_size:
            # Next time the file is loaded, it will use the thumbnail first, then resize

            img.thumbnail((thumbnail_size, thumbnail_size))
            if disable_thumbnail_generation is False:
                img.save(
                    thumbnailPath,
                    img.format)
            if Main.thumbnail_generation_queue[0] == fileKey:
                Main.thumbnail_generation_queue = Main.thumbnail_generation_queue[1::]
            return img
    except Exception as e:
        if Main.thumbnail_generation_queue[0] == fileKey:
            Main.thumbnail_generation_queue = Main.thumbnail_generation_queue[1::]
        raise e


def get_available_categories():
    """
    Returns all available categories in images
    """

    available_categories = convert_all_to_dict(db.session.execute(sql_text(f"""
                select distinct main_category,sub_category from {Schema}.IMAGE_DIRS
                order by MAIN_CATEGORY asc, SUB_CATEGORY asc
                """)))
    category_dict = {}
    for row in available_categories:
        if row['main_category'] not in category_dict:
            category_dict[row['main_category']] = []
        category_dict[row['main_category']].append(row['sub_category'].title())
    return category_dict


def remove_thumbnails(imfi_record, thumbnail_sizes):
    """
    Removes all available thumbnails within the thumbnail list
    :param imfi_auto_key:
    :param folder:
    :param thumbnail_sizes:
    :return:
    """
    thumbnail_folder = Main.server_settings['folder']['thumbnails']
    removedFiles = False
    for thumbnail_size in thumbnail_sizes:  # Main_IS.thumbnail_sizes:
        if os.path.isfile(
                f'{thumbnail_folder}/{imfi_record.imfi_auto_key}_{thumbnail_size}_{thumbnail_size}.{imfi_record.path.split(".")[-1].lower()}'):
            os.remove(
                f'{thumbnail_folder}/{imfi_record.imfi_auto_key}_{thumbnail_size}_{thumbnail_size}.{imfi_record.path.split(".")[-1].lower()}')
            removedFiles = True
    return removedFiles



@process_method('search_files')
def search_files(SEARCH='',LIMIT=200,PAGE=0,**kwargs):
    #Redo of search_files, but converting it to allow bracket searches within the base search rather than a separate bracket search.
    where_clauses = ''
    random = False

    noWildcardTags = [[]]
    noWildcardQueryWhere = ""

    query_selects = ['IMFI.IMFI_AUTO_KEY', 'IMFI.ORIGINAL_DATE', 'IMFI.FILE_NAME', 'IMTAG_PRECALC.TAGS',
                     'IMTAG_PRECALC.TAG_AUTO_KEYS', 'IMTAG_PRECALC.TAG_COUNT', 'IMFI.MIMETYPE',
                     'COALESCE(IMFI.FRAMES, 0) frames', 'IMFI.DIMENSION_WIDTH', 'IMFI.DIMENSION_HEIGHT',
                     'coalesce(IMFI.DURATION,0) "DURATION"', 'IMFI.IMDIR_AUTO_KEY', 'IMFI.PRIVATE']

    random_seed = None
    imagesWhere = ["available_file = True"]
    tagWhere = []
    tagHaving = []  # not used
    extra_joins = []
    extra_flags = {}
    extra_join_flags = {'notes': False, 'sequence': False}
    extra_join_data = {'notes': [], 'sequence': [], 'notes_having':[],'sequence_having':[]}
    order_by = []
    private_clause = 'and PRIVATE = false'

    search_parameters = {}

    mainCategory = None
    subCategory = None

    if SEARCH:
        filter = SEARCH
        if len(filter) > 0:
            filter = filter.replace('|', ',|').lstrip(',').rstrip(',')
            filters = []
            for searchItemIndex, searchItem in enumerate(re.split(r',(?![^\[]*\])(?![^\{]*\})', filter)):  # regex
                bracketItems = re.findall(r'(?<!\\)\[(?:\\\\)*([^\\[\]]+)]', searchItem)
                beforeBracket = re.split(r'(?<!\\)\[', searchItem)[0]

                if not bracketItems:
                    bracketItems = ['']
                    afterBracket = ''
                else:
                    bracketItems = bracketItems[0].split(',')
                    afterBracket = searchItem.split(f"{bracketItems[-1]}]")[-1]
                bracketInsertList = []
                for bracketIndex,bracketitem in enumerate(bracketItems):
                    combinedString = f"{beforeBracket}{bracketitem}{afterBracket}".lower()
                    combinedString = combinedString.lstrip(' ')
                    if combinedString:
                        wildCards = ['%', '%']
                        not_addition = ''
                        innerBracketInsertList = []

                        temporary_extra_join_data = {'sequence_having': [],'notes':[],'sequence':[], 'notes_having': []}
                        if '!' in combinedString[0]:
                            not_addition = 'not'
                            combinedString = combinedString[1::]  # Removes the !
                        Type = 'and'
                        if '|' in combinedString[0]:
                            combinedString = combinedString.lstrip('|')
                            Type = 'or'

                        if combinedString[0] == '#':
                            commandString = True
                        else:
                            commandString = False

                        if commandString is False:
                            if combinedString[0] == '^':
                                wildCards[0] = '%|'
                                # wildCards[0] = ''
                                combinedString = combinedString[1:]
                            if combinedString[-1] == '^':
                                wildCards[1] = '|%'
                                # wildCards[1] = ''
                                combinedString = combinedString[0:-1]
                        else:
                            if combinedString.count(':') >= 1:
                                command_key,command_value = combinedString.split(':',1)
                                if command_value[0] == '^':
                                    wildCards[0] = ''
                                    # wildCards[0] = ''
                                    command_value = command_value[1:]
                                if command_value[-1] == '^':
                                    wildCards[1] = ''
                                    # wildCards[1] = ''
                                    command_value = command_value[0:-1]
                                combinedString = ':'.join([command_key,command_value])

                        if commandString is True:
                            pass
                            combinedString = combinedString.lstrip('#')

                            #get filterCommand, and parameter(s)
                            if ':' in combinedString:
                                #filterParameters = combinedString.split(':')[1::]
                                filterParameters = re.split(r':(?![^\{]*\})', combinedString)[1::]
                            else:filterParameters = []
                            filterCommand = re.split(r':(?![^\{]*\})', combinedString)[0]
                            #sort by parameter types. range, and non-range.

                            if filterCommand in ['private','portrait','landscape','square',
                                                 'animated','images','image','videos','video','files','last_tagged','lt','filename_order','oldest','newest','orderby']:
                                #no parameters allowed. ie: #private is all that is required.
                                if filterCommand == 'private':
                                    private_clause = ''
                                elif filterCommand == 'portrait':
                                    innerBracketInsertList.append(f"""imfi.dimension_width < imfi.dimension_height""")
                                elif filterCommand == 'landscape':
                                    innerBracketInsertList.append(f"""imfi.dimension_width > imfi.dimension_height""")
                                elif filterCommand == 'square':
                                    innerBracketInsertList.append('imfi.dimension_width = imfi.dimension_height')
                                elif filterCommand == 'animated':
                                    innerBracketInsertList.append(f"coalesce(imfi.frames,0) > 1")
                                    innerBracketInsertList.append(f"(substring(imfi.mimetype,0,7) = 'image/')")
                                elif filterCommand in ['image','images']:
                                    innerBracketInsertList.append(f"(imfi.frames = 1 or imfi.frames is NULL)")
                                    innerBracketInsertList.append(f"(substring(imfi.mimetype,0,7) = 'image/')")
                                elif filterCommand in ['videos','video']:
                                    innerBracketInsertList.append(f"(substring(imfi.mimetype,0,7) = 'video/')")
                                elif filterCommand == 'files':
                                    innerBracketInsertList.append(f"(substring(imfi.mimetype,0,7) <> 'image/')")
                                    innerBracketInsertList.append(f"(substring(imfi.mimetype,0,7) <> 'video/')")
                                elif filterCommand in ['lt','last_tagged','filename_order','oldest','newest','orderby']:
                                    if filterCommand in ['last_tagged','lt']:
                                        if len(filterParameters) > 0:
                                            if filterParameters[0][0] == '{' and filterParameters[0][-1] == '}':
                                                tagSplits = filterParameters[0][1::][:-1].split(',')
                                                tagWhereSearches = []
                                                for tagSplit in tagSplits:
                                                    if ':' in tagSplit:
                                                        tagWhereSearches.append(
                                                            ['tag.category||\':\'||tag.detail', tagSplit])
                                                    else:
                                                        tagWhereSearches.append(
                                                            ['tag.category',
                                                             tagSplit.upper()])
                                                if len(tagWhereSearches) > 0:
                                                    orderWhereList = []
                                                    for tws_index, tagWhereSearch in enumerate(tagWhereSearches):
                                                        search_parameters[
                                                            f'ordertag_{searchItemIndex}_{bracketIndex}_{tws_index}_'] = f"{tagWhereSearch[1].upper()}"
                                                        orderWhereList.append(
                                                            f'{tagWhereSearch[0]} = :ordertag_{searchItemIndex}_{bracketIndex}_{tws_index}_')
                                                    order_by.append([f'''(select max(imtag.timestamp) from {Schema}.tags tag
                                                                inner join {Schema}.tagged_images imtag on imtag.tag_auto_key = tag.tag_auto_key 
                                                                where imtag.imfi_auto_key = imfi.imfi_auto_key and ({' or '.join(orderWhereList)}))''','desc','nulls last'])
                                            else:
                                                order_by.append([f'''(select max(imtag.timestamp) from {Schema}.tags tag
                                                                    inner join {Schema}.tagged_images imtag on imtag.tag_auto_key = tag.tag_auto_key 
                                                                    where imtag.imfi_auto_key = imfi.imfi_auto_key and tag.category = :lasttagged_{searchItemIndex}_{bracketIndex}_)''','desc','nulls last'])
                                                search_parameters[
                                                    f'lasttagged_{searchItemIndex}_{bracketIndex}_'] = f"{filterParameters[0].upper()}"
                                        else:
                                            order_by.append(['imfi.last_tag_update','desc','nulls last'])
                                    elif filterCommand == 'filename_order':
                                        order_by.append(['imfi.FILE_NAME','asc'])
                                    elif filterCommand == 'oldest':
                                        order_by.append(['imfi.ORIGINAL_DATE','asc'])
                                    elif filterCommand == 'newest':
                                        order_by.append(['imfi.ORIGINAL_DATE','desc'])
                                    elif filterCommand == 'orderby':
                                        if len(filterParameters) > 0:
                                            orderText = ''

                                            if filterParameters[0] in ['tag']:
                                                #For string orderby, not numeral. Uses 'asc' by default.
                                                orderType = 'asc'
                                                if len(filterParameters) > 2:
                                                    if filterParameters[-1] in ['desc']:
                                                        orderType = 'desc'

                                                if len(filterParameters) > 1:
                                                    if filterParameters[1][0] == '{' and filterParameters[1][-1] == '}':
                                                        tagSplits = filterParameters[1][1::][:-1].split(',')
                                                        tagWhereSearches = []
                                                        for tagSplit in tagSplits:
                                                            if ':' in tagSplit:
                                                                tagWhereSearches.append(['tag.category||\':\'||tag.detail',tagSplit])
                                                            else:
                                                                tagWhereSearches.append(
                                                                    ['tag.category',
                                                                     tagSplit.upper()])
                                                        if len(tagWhereSearches) > 0:
                                                            orderWhereList = []
                                                            for tws_index,tagWhereSearch in enumerate(tagWhereSearches):
                                                                search_parameters[
                                                                    f'ordertag_{searchItemIndex}_{bracketIndex}_{tws_index}_'] = f"{tagWhereSearch[1].upper()}"
                                                                orderWhereList.append(f'{tagWhereSearch[0]} = :ordertag_{searchItemIndex}_{bracketIndex}_{tws_index}_')
                                                            orderText = f'''(select string_agg(tag.detail,'|' order by tag.detail asc) from {Schema}.tags tag
                                                                                                                            inner join {Schema}.tagged_images imtag on imtag.tag_auto_key = tag.tag_auto_key where imtag.imfi_auto_key = imfi.imfi_auto_key and ({' or '.join(orderWhereList)}))'''

                                                    else:
                                                        orderText = f'''(select string_agg(tag.detail,'|' order by tag.detail asc) from {Schema}.tags tag
                                                            inner join {Schema}.tagged_images imtag on imtag.tag_auto_key = tag.tag_auto_key where imtag.imfi_auto_key = imfi.imfi_auto_key and tag.category = :ordertag_{searchItemIndex}_{bracketIndex}_)'''
                                                        search_parameters[
                                                            f'ordertag_{searchItemIndex}_{bracketIndex}_'] = f"{filterParameters[1].upper()}"
                                            elif filterParameters[0] in ['imfi_ak']:
                                                if len(filterParameters) >= 2:
                                                    custom_case_imfi = "CASE "
                                                    if filterParameters[1][0] == '{' and filterParameters[1][-1] == '}':
                                                        imfi_ak_list = filterParameters[1][1::][:-1].split(',')
                                                        for imfi_ak_index,imfi_ak in enumerate(imfi_ak_list):
                                                            custom_case_imfi+=f'\nwhen IMFI.IMFI_AUTO_KEY = :imfi_ak_order_{searchItemIndex}_{bracketIndex}_{imfi_ak_index}_ then {imfi_ak_index}'
                                                            search_parameters[
                                                                f'imfi_ak_order_{searchItemIndex}_{bracketIndex}_{imfi_ak_index}_'] = f"{imfi_ak}"
                                                        custom_case_imfi += f'\n END '
                                                        order_by.append([custom_case_imfi,"asc","nulls last"])
                                                    pass



                                            else:
                                                ## This section is more for numerical ordering, above is for custom/text-based ordering.
                                                orderType = 'desc'
                                                if len(filterParameters) > 1:
                                                    if filterParameters[-1] in ['asc']:
                                                        orderType = 'asc'

                                                if filterParameters[0] == 'size':
                                                    orderText = 'imfi.file_size'
                                                elif filterParameters[0] == 'filename':
                                                    orderText = 'imfi.file_name'
                                                elif filterParameters[0] == 'width':
                                                    orderText = 'imfi.dimension_width'
                                                elif filterParameters[0] == 'height':
                                                    orderText = 'imfi.dimension_height'
                                                elif filterParameters[0] == 'duration':
                                                    orderText = 'imfi.duration'
                                                elif filterParameters[0] == 'frames':
                                                    orderText = 'imfi.frames'
                                                elif filterParameters[0] == 'tag_count':
                                                    orderText = 'imtag_precalc.tag_count'

                                            if orderText:
                                                order_by.append([f"{orderText}",f"{orderType}","nulls last"])

                            elif filterCommand in ['main_category','sub_category','filename','imdir','sequence','notes','regex','random','rand','all','tag_notes','tag_notes_regex','tanore','tano']:
                                # has parameters, but not more than one.
                                if filterCommand == 'main_category' and len(filterParameters) > 0:
                                    mainCategory = filterParameters[0]
                                elif filterCommand == 'sub_category' and len(filterParameters) > 0:
                                    subCategory = filterParameters[0]
                                elif filterCommand == 'all':
                                    subCategory = ''
                                    mainCategory = ''
                                elif filterCommand == 'filename' and len(filterParameters) > 0:
                                    innerBracketInsertList.append(f"'|'||upper(FILE_NAME)||'|' {not_addition} like upper(:file_search_{searchItemIndex}_{bracketIndex}_)")
                                    search_parameters[f'file_search_{searchItemIndex}_{bracketIndex}_'] = f"{wildCards[0]}{filterParameters[0]}{wildCards[1]}"
                                elif filterCommand == 'imdir' and len(filterParameters) > 0:
                                    innerBracketInsertList.append(f"imdir.imdir_auto_key {'!' if not_addition == 'not' else ''}= :imdir_ak_{searchItemIndex}_{bracketIndex}_")
                                    search_parameters[f'imdir_ak_{searchItemIndex}_{bracketIndex}_'] = int(filterParameters[0])
                                elif filterCommand == 'regex' and len(filterParameters) > 0:
                                    innerBracketInsertList.append(f"{not_addition} regexp_like(upper(FILE_NAME),upper(:regex_search_ak_{searchItemIndex}_{bracketIndex}_))")
                                    search_parameters[f'regex_search_ak_{searchItemIndex}_{bracketIndex}_'] = filterParameters[0]
                                    pass
                                elif filterCommand == 'sequence':

                                    extra_join_flags['sequence'] = True
                                    if len(filterParameters) > 0:
                                        # print("SEQUENCE!",f"{wildCards[0]}{filterParameters[0]}{wildCards[1]}")
                                        extra_join_data['sequence'].append(f"(sequence_name {not_addition} like upper(:seq_name_{searchItemIndex}_{bracketIndex}_))")
                                        search_parameters[f'seq_name_{searchItemIndex}_{bracketIndex}_'] = f"{wildCards[0]}{filterParameters[0]}{wildCards[1]}"

                                elif filterCommand == 'notes':
                                    extra_join_flags['notes'] = True
                                    if len(filterParameters) > 0:
                                        temporary_extra_join_data['notes'].append(
                                            f"(lower(IMFINO.\"note_body_agg\") like lower(:note_body_{searchItemIndex}_{bracketIndex}_))")
                                        search_parameters[f'note_body_{searchItemIndex}_{bracketIndex}_'] = f"{wildCards[0]}{filterParameters[0]}{wildCards[1]}"
                                    # innerBracketInsertList.append('')
                                elif filterCommand in ['tag_notes','tano','tag_notes_regex','tanore']:
                                    ##TODO - Want to search up all files that have tags w/ requested notes.
                                    pass
                                    # if filterCommand in ['tag_notes','tano']:
                                    #     pass
                                    #     join_to_add = f"(select unique(imtag.imfi_auto_key) from {Schema}.tags tag inner join {Schema}.tagged_images imtag on imtag.tag_auto_key = tag.tag_auto_key where tag.notes like :note_body_{searchItemIndex}_{bracketIndex}) note_join on imfi.imfi_auto_key = note_join.imfi_auto_key"
                                    # elif filterCommand in ['tag_notes_regex','tanore']:
                                    #     pass
                                elif filterCommand in ['rand','random']:
                                    if len(filterParameters) > 0:
                                        try:
                                            random_seed = int(searchItem.split(':')[1])
                                        except:
                                            random_seed = int(''.join(([str(ord(x)) for x in str(searchItem.split(':')[1])]))) % 2147483647
                                    random = True
                                    pass
                            elif filterCommand in ['imfi_ak','date','year',
                                                   'week','day','qoy','wom','note_count',
                                                   'sequence_count','sequence_number','ratio','width',
                                                   'height','duration','tag_count','frames',
                                                   'age','last_tagged_age','lta','month','dow','quarter','size'] and len(filterParameters) > 0:
                                #can have a range within the parameter
                                rangeSplit = [x.strip() for x in filterParameters[0].split('-')[0:2]]
                                noProvidedParameter = False
                                if len(rangeSplit) > 1:
                                    searchTypeList = ['>=','<=']
                                else:

                                    if filterParameters[0][0] in ['>','=','<']:
                                        if filterParameters[0][1] == '=':
                                            searchTypeList = [f"{filterParameters[0][0]}{filterParameters[0][1]}"]
                                            rangeSplit = [x.lstrip(f"{filterParameters[0][0]}{filterParameters[0][1]}") for x in rangeSplit]
                                        else:
                                            searchTypeList = [f"{filterParameters[0][0]}"]
                                            rangeSplit = [x.lstrip(f"{filterParameters[0][0]}") for x in rangeSplit]
                                    else:
                                        noProvidedParameter = True
                                        searchTypeList = ['=']
                                for rangeSplitIndex,range_split in enumerate(rangeSplit):
                                    if filterCommand == 'imfi_ak':
                                        innerBracketInsertList.append(f"""imfi.imfi_auto_key {searchTypeList[rangeSplitIndex]} :imfi_auto_key_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_""")
                                        search_parameters[f'imfi_auto_key_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(range_split)
                                    elif filterCommand == 'date':
                                        try:
                                            year, month, day = range_split.split('/')
                                            innerBracketInsertList.append(f"TO_CHAR(ORIGINAL_DATE,'YYYY/MM/DD') {searchTypeList[rangeSplitIndex]} :date_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_")
                                            search_parameters[
                                                f'date_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = '/'.join([year.zfill(4),month.zfill(2),day.zfill(2)])
                                        except:pass
                                    elif filterCommand == 'year':
                                        innerBracketInsertList.append(f"extract('year' from ORIGINAL_DATE) {searchTypeList[rangeSplitIndex]} :year_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_")
                                        search_parameters[
                                            f'year_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(range_split)
                                    elif filterCommand == 'month':
                                        innerBracketInsertList.append(
                                            f"extract('month' from ORIGINAL_DATE) {searchTypeList[rangeSplitIndex]} :month_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_")
                                        months = ['january','february','march','april','may','june','july','august','september','october','november','december']
                                        try:
                                            search_parameters[
                                                f'month_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(
                                                range_split)
                                        except:
                                            try:
                                                for monthIndex,month in enumerate(months):
                                                    if range_split in month:
                                                        search_parameters[
                                                            f'month_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(monthIndex+1)
                                                        break
                                            except:
                                                #If it fails, the parameter will still be loaded and return nothing.
                                                search_parameters[
                                                    f'month_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = 0
                                    elif filterCommand == 'week':
                                        innerBracketInsertList.append(f"extract('week' from ORIGINAL_DATE) {searchTypeList[rangeSplitIndex]} :week_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_")
                                        search_parameters[
                                            f'week_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(
                                            range_split)
                                    elif filterCommand == 'day':
                                        innerBracketInsertList.append(f"extract('day' from ORIGINAL_DATE) {searchTypeList[rangeSplitIndex]} :day_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_")
                                        search_parameters[
                                            f'day_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(range_split)
                                    elif filterCommand in ['dow']:
                                        day_of_week_list = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday',
                                                    'friday',
                                                    'saturday']
                                        if range_split in day_of_week_list:
                                            dowIndex = day_of_week_list.index(range_split)
                                        else:
                                            try:
                                                dowIndex = int(range_split)
                                                dowIndex-=1
                                            except:dowIndex = -1
                                        if dowIndex >= 0 and dowIndex < 7:
                                            innerBracketInsertList.append(
                                                f"'0'||extract('dow' from ORIGINAL_DATE) {searchTypeList[rangeSplitIndex]} :search_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_")
                                            search_parameters[f'search_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = str(dowIndex).zfill(2)

                                    elif filterCommand == 'wom':
                                        innerBracketInsertList.append(f"(CEIL(EXTRACT(DAY FROM ORIGINAL_DATE) / 7.0) {searchTypeList[rangeSplitIndex]} :wom_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_")
                                        search_parameters[f'wom_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(range_split)
                                    elif filterCommand in ['qoy','quarter']:
                                        innerBracketInsertList.append(f'EXTRACT(QUARTER FROM ORIGINAL_DATE) {searchTypeList[rangeSplitIndex]} :qoy_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_')
                                        search_parameters[f'qoy_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(range_split)
                                    elif filterCommand == 'note_count':
                                        extra_join_flags['notes'] = True
                                        search_parameters[f'note_count_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(range_split)
                                        temporary_extra_join_data['notes_having'].append(f'count(imfi_auto_key) {searchTypeList[rangeSplitIndex]} :note_count_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_')
                                        # extra_joins.append(f"""inner join
                                        # (select imfi_auto_key,count(imfi_auto_key) from {Schema}.image_file_notes group by imfi_auto_key )
                                        #  note_count on note_count.imfi_auto_key = imfi.imfi_auto_key""")
                                    elif filterCommand == 'sequence_count':
                                        extra_join_flags['sequence'] = True
                                        search_parameters[f'seq_count_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(range_split)
                                        temporary_extra_join_data['sequence_having'].append(f"count(sequence_name) {searchTypeList[rangeSplitIndex]} :seq_count_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_")
                                        # extra_join_data['sequence_having'].append(f"count(sequence_name) {searchTypeList[rangeSplitIndex]} :seq_count_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_")
                                        # extra_joins.append(f"""inner join
                                        #     (select imfi_auto_key from {Schema}.sequence seq
                                        #     inner join (select sequence_name from {Schema}.sequence group by sequence_name having count(imfi_auto_key) {searchTypeList[rangeSplitIndex]} :seq_count_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_) seq_count on seq_count.sequence_name = seq.sequence_name
                                        #     )
                                        #      seq_count on seq_count.imfi_auto_key = imfi.imfi_auto_key""")
                                    elif filterCommand == 'sequence_number':
                                        extra_join_flags['sequence'] = True
                                        filters.append(
                                            [Type,
                                             f"(seq.sequence_number {searchTypeList[rangeSplitIndex]} :seq_number_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_)"])
                                        search_parameters[f'seq_number_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(range_split)
                                    elif filterCommand == 'ratio':
                                        ratioSplit = range_split.split('/')
                                        innerBracketInsertList.append(f"""imfi.dimension_width::float / imfi.dimension_height::float {searchTypeList[rangeSplitIndex]} :ratioWidth_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_ / :ratioHeight_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_""")
                                        search_parameters[f'ratioWidth_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = float(ratioSplit[0])
                                        search_parameters[f'ratioHeight_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = float(ratioSplit[1])
                                    elif filterCommand == 'width':
                                        if filterParameters[0] == 'height':
                                            innerBracketInsertList.append(f"""imfi.dimension_width {searchTypeList[rangeSplitIndex]} imfi.dimension_height""")
                                        else:
                                            if range_split == 'null':
                                                innerBracketInsertList.append(
                                                    f"""imfi.dimension_width is NULL""")
                                            else:
                                                innerBracketInsertList.append(
                                                    f"""imfi.dimension_width {searchTypeList[rangeSplitIndex]} :search_width_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_""")
                                                search_parameters[f'search_width_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_' \
                                                              f''] = int(range_split)
                                    elif filterCommand == 'height':
                                        if filterParameters[0] == 'width':
                                            innerBracketInsertList.append(f"""imfi.dimension_height {searchTypeList[rangeSplitIndex]} imfi.dimension_width""")
                                        else:
                                            if range_split == 'null':
                                                innerBracketInsertList.append(
                                                    f"""imfi.dimension_height is NULL""")
                                            else:
                                                innerBracketInsertList.append(f"""imfi.dimension_height {searchTypeList[rangeSplitIndex]} :search_height_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_""")
                                                search_parameters[f'search_height_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(range_split)
                                    elif filterCommand == 'duration':
                                        if "(substring(imfi.mimetype,0,7) = 'video/')" not in innerBracketInsertList:
                                            innerBracketInsertList.append(f"(substring(imfi.mimetype,0,7) = 'video/')")
                                        innerBracketInsertList.append(f"""imfi.duration {searchTypeList[rangeSplitIndex]} :search_duration_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_""")
                                        search_parameters[f'search_duration_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(range_split)
                                    elif filterCommand == 'tag_count':
                                        detailSplit = range_split.split(':')
                                        if len(filterParameters) > 1:
                                            categoryCount = filterParameters[1]
                                        else:
                                            categoryCount = None
                                        tagCount = detailSplit[0]
                                        if categoryCount:
                                            innerBracketInsertList.append(f"""(length(TAGS) - length(REPLACE(TAGS, :categoryCount_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_, ''))) / length(:categoryCount_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_) {searchTypeList[rangeSplitIndex]} CAST(:tagCount_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_ as INTEGER)""")
                                            search_parameters[f'tagCount_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(tagCount)
                                            search_parameters[f'categoryCount_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = f'|{categoryCount.upper()}:'
                                        else:
                                            filters.append(
                                                [Type, f"TAG_COUNT {searchTypeList[rangeSplitIndex]} CAST(:tagCount_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_ as INTEGER)"])
                                            search_parameters[f'tagCount_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(tagCount)
                                    elif filterCommand == 'frames':
                                        innerBracketInsertList.append(f"(substring(imfi.mimetype,0,7) = 'image/')")
                                        innerBracketInsertList.append(f"""imfi.frames {searchTypeList[rangeSplitIndex]} :search_frames_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_""")
                                        search_parameters[f'search_frames_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = int(range_split)
                                    #slightly more complicated
                                    elif filterCommand in ['age','last_tagged_age','lta']:
                                        if filterCommand == 'age':
                                            filterColumn = 'IMFI.ORIGINAL_DATE'
                                        elif filterCommand in ['last_tagged_age','lta']:
                                            filterColumn = 'IMFI.LAST_TAG_UPDATE'
                                        if noProvidedParameter and len(rangeSplit) == 1:
                                            searchTypeList[0] = '>='
                                        innerBracketInsertList.append(f"(NOW() - INTERVAL :interval_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_) {searchTypeList[rangeSplitIndex]} {filterColumn}")
                                        for interval in [
                                            ['seconds', ['sec']],
                                            ['minutes', ['mnt']],
                                            ['hours', ['hrs']],
                                            ['days', ['dys']],
                                            ['weeks', ['wks']],
                                            ['months', ['mnths']],
                                            ['years', ['yrs']],
                                            ['decades', ['dec']],
                                            ['century', ['cent']],
                                            ['centuries', []],
                                            ['millennium', ['mil']]
                                        ]:
                                            foundInterval = False
                                            intervalSearchItem = range_split.split(' ')[-1].lower()
                                            if intervalSearchItem in interval[0]:
                                                foundInterval = True
                                                range_split = f"{range_split.split(' ')[0]} {interval[0]}"
                                                break
                                            else:
                                                breakOut = False
                                                for abbrevInterval in interval[1]:
                                                    if intervalSearchItem in abbrevInterval:
                                                        range_split = f"{range_split.split(' ')[0]} {interval[0]}"
                                                        foundInterval = True
                                                        breakOut = True
                                                        break
                                            if breakOut:
                                                break
                                        if foundInterval is False:
                                            range_split = f"{range_split.split(' ')[0]} day"
                                        search_parameters[f'interval_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = range_split
                                        pass
                                    elif filterCommand == 'size':
                                        #defaults to bytes

                                        file_size_base = 'IMFI.FILE_SIZE'
                                        if range_split == 'null':
                                            innerBracketInsertList.append(
                                                f"{file_size_base} is NULL")
                                        else:
                                            # print(range_split)
                                            range_split_split = range_split.split(' ')
                                            if len(range_split_split) > 1:
                                                # parameterSplit = filterParameters[0].split(' ')
                                                if range_split_split[1].lower() in ['mb','gb']:
                                                    if range_split_split[1].lower() == 'mb':
                                                        file_size_base = f'({file_size_base} / 1e6)'
                                                    elif range_split_split[1].lower() == 'gb':
                                                        file_size_base = f'({file_size_base} / 1e9)'

                                            innerBracketInsertList.append(f"{file_size_base} {searchTypeList[rangeSplitIndex]} :size_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_")
                                            search_parameters[f'size_{searchItemIndex}_{bracketIndex}_{rangeSplitIndex}_'] = range_split_split[0]

                        elif combinedString == ':':
                            innerBracketInsertList.append(f"imtag_precalc.tags {not_addition} like upper(:category_{searchItemIndex}_{bracketIndex}_)")
                            search_parameters[f'category_{searchItemIndex}_{bracketIndex}_'] = f'{wildCards[0]}{combinedString}{wildCards[1]}'

                        elif ':' in combinedString:
                            split = combinedString.split(':')
                            innerBracketInsertList.append(f"imtag_precalc.tags {not_addition} like upper(:search_{searchItemIndex}_{bracketIndex}_)")
                            search_parameters[f'search_{searchItemIndex}_{bracketIndex}_'] = f'{wildCards[0]}{split[0]}:{split[1]}{wildCards[1]}'

                        else:
                            innerBracketInsertList.append(f"imtag_precalc.tags {not_addition} like upper(:search_{searchItemIndex}_{bracketIndex}_)")
                            search_parameters[f'search_{searchItemIndex}_{bracketIndex}_'] = f'{wildCards[0]}{combinedString}{wildCards[1]}'

                        if len(temporary_extra_join_data['sequence_having']) > 0:
                            extra_join_data['sequence_having'].append(
                                f"({' and '.join(temporary_extra_join_data['sequence_having'])})")
                        if len(temporary_extra_join_data['notes_having']) > 0:
                            extra_join_data['notes_having'].append(
                                f"({' and '.join(temporary_extra_join_data['notes_having'])})")
                        if len(temporary_extra_join_data['notes']) > 0:
                            extra_join_data['notes'].append(
                                f"({' or '.join(temporary_extra_join_data['notes'])})")

                        if innerBracketInsertList:
                            bracketInsertList.append(f"({' and '.join(innerBracketInsertList)})" if len(innerBracketInsertList) > 1 else innerBracketInsertList[0])

                if bracketInsertList:
                    if not_addition:
                        filters.append([Type, f"({' and '.join(bracketInsertList)})"])
                    else:
                        filters.append([Type, f"({' or '.join(bracketInsertList)})"])







            if extra_join_flags['notes'] is True:
                notes_where_string = ''
                if len(extra_join_data['notes']) > 0:
                    # notes_where_string = f'where {" and ".join(extra_join_data["notes"])}'
                    filters.append([Type,f' and '.join(extra_join_data["notes"])])
                else:
                    pass
                    # notes_where_string = ''
                if len(extra_join_data['notes_having']) > 0:
                    notes_having_string = f'having {" or ".join(extra_join_data["notes_having"])}'
                else:
                    notes_having_string = ''

                extra_joins.append(
                    f'inner join (select imfi_auto_key,string_agg(note_body,\'|*|\') as note_body_agg from {Schema}.IMAGE_FILE_NOTES IMFINO {notes_where_string} group by imfi_auto_key {notes_having_string}) IMFINO on IMFINO.IMFI_AUTO_KEY = IMFI.IMFI_AUTO_KEY')
            if extra_join_flags['sequence'] is True:
                if len(extra_join_data['sequence']) > 0:
                    sequence_where_string = f'where {" and ".join(extra_join_data["sequence"])}'
                else:
                    sequence_where_string = ''
                if len(extra_join_data['sequence_having']) > 0:
                    inner_join_string = f'inner join (select sequence_name from {Schema}.SEQUENCE group by sequence_name having {" or ".join(extra_join_data["sequence_having"])} ) seq_count on seq_count.sequence_name = seq.sequence_name'
                else:
                    inner_join_string = ''
                extra_joins.append(
                    f'inner join (select * from {Schema}.SEQUENCE SEQ {inner_join_string} {sequence_where_string}) SEQ on SEQ.IMFI_AUTO_KEY = IMFI.IMFI_AUTO_KEY')





            for xy, f in enumerate(filters):
                if xy == 0:
                    where_clauses += ' and ('
                else:
                    where_clauses += ' {} '.format(f[0])
                where_clauses += f[1]
            if filters:
                where_clauses += ' )'


    ## END
    ## END
    ## END
    else:  # If no search parameter (or if it's '')

        pass

    if mainCategory is None:
        mainCategory = kwargs.get('MAIN_CATEGORY', '')
    if mainCategory:
        main_categoryWhere = 'and IMDIR.MAIN_CATEGORY = upper(:MAIN_CATEGORY)'
        search_parameters['MAIN_CATEGORY'] = mainCategory
    else:
        main_categoryWhere = ''

    if subCategory is None:
        subCategory = kwargs.get('SUB_CATEGORY', '')
    if subCategory:
        categorySplit = subCategory.split('|*|')
        sub_categoryWhere = f'and IMDIR.SUB_CATEGORY in ({",".join([f"upper(:SUB_CATEGORY_{i})" for i in range(len(categorySplit))])})'
        search_parameters.update({f'SUB_CATEGORY_{i}': sub_category for i, sub_category in enumerate(categorySplit)})
    else:
        sub_categoryWhere = ''

    if random is True:
        # Used hashint4 instead of setseed because setseed just wasn't working for me.
        order_by.append(['hashint4(imtag_precalc.imfi_auto_key + :random_seed)'])
        # if random_seed is None and (kwargs.get('RANDOM_SEED', None) or kwargs.get('RANDOM_SEED', None)):
        #     random_seed = int(random_seed)

        if random_seed is None and kwargs.get('RANDOM_SEED', None) is not None:
            try:
                random_seed = int(kwargs.get('RANDOM_SEED', None))
            except:
                random_seed = None

        if random_seed is not None and abs(random_seed) > 2147483647:
            random_seed = random_seed % 2147483647
        elif random_seed is None:
            random_seed = rng.randint(-2147483647, 2147483647)
        search_parameters['random_seed'] = random_seed
    else:
        order_by.append(['IMFI.ORIGINAL_DATE', 'desc'])

    order_by_str = f"order by {','.join([' '.join(x) for x in order_by])}"

    if IMFI_AUTO_KEY := kwargs.get('IMFI_AUTO_KEY', None):
        filters.append(
            [Type, f"""imfi.imfi_auto_key = :imfi_auto_key_"""])
        search_parameters[f'imfi_auto_key_'] = int(IMFI_AUTO_KEY)

    if LIMIT > 0:
        offset = (int(PAGE) * LIMIT) - LIMIT
        if offset < 0:
            offset = 0
    else:
        offset = 0

    if tagWhere:
        # tagWhere = f'where {" and ".join(tagWhere)}'
        tagWhere = f"""inner join (
                        	select mainsub.imfi_auto_key from (
                        	select distinct ti.imfi_auto_key from {Schema}.tagged_images ti
                        					inner join (select * from {Schema}.tags where {' or '.join(tagWhere)}) t on t.tag_auto_key = ti.tag_auto_key
                        					group by ti.imfi_auto_key
                        					) mainsub
                        	--order by mainsub.timestamp desc --This only effects tagged time.
                        ) TAG_CHECK on TAG_CHECK.imfi_auto_key = IMTAG_PRECALC.imfi_auto_key"""
    else:
        tagWhere = ''
    if tagHaving:  # Not used?
        tagHaving = f'WHERE {" and ".join(tagHaving)}'
    else:
        tagHaving = ''
    if imagesWhere:
        imagesWhere = f'where {" and ".join(imagesWhere)}'
    else:
        imagesWhere = ''

    if len(noWildcardTags[0]) > 0:  #
        # print(__name__,'NWCT',noWildcardTags)
        _final_list = []
        noWildcardQuery = """
                inner join (
                    {}
                ) imtag on imtag.imfi_auto_key = imtag_precalc.imfi_auto_key
                """

        for tagListIndex, tagList in enumerate(noWildcardTags):
            # print(tagList)

            tagWhereList = []
            for tagIndex, tag in enumerate(tagList):
                tagWhereList.append(f"category||':'||detail = upper(:noWC_{tagListIndex}_{tagIndex}_)")
                search_parameters[f'noWC_{tagListIndex}_{tagIndex}_'] = tag

            havingStatement = 'group by imtag.imfi_auto_key'
            if len(tagWhereList) > 1:
                havingStatement = f'''group by imtag.imfi_auto_key
                                  f'having count(imfi_auto_key) = {len(tagWhereList)}'''
            outerTagQuery = f"""
                    select imfi_auto_key from {Schema}.tagged_images imtag
                    inner join (
                            SELECT TAG_AUTO_KEY FROM {Schema}.TAGS
                            WHERE {' or '.join(tagWhereList)}
                    ) tags on tags.tag_auto_key = imtag.tag_auto_key
                   {havingStatement} 
                """
            _final_list.append(outerTagQuery)
        noWildcardQueryWhere = noWildcardQuery.format('\nunion all\n'.join(_final_list))

    extra_joins = '\n'.join(extra_joins)


    custom_query_data = {'EXT_ORDER_BY':[]}
    if kwargs.get('CUSTOM_QUERY',False) is True:
        query_selects.extend([f"{order[0]} \"EXT_ORDER_{xy}\"" for xy,order in enumerate(order_by)])
        custom_query_data['EXT_ORDER_BY'].extend([f"MAIN.\"EXT_ORDER_{xy}\" {' '.join(order[1::])}" for xy,order in enumerate(order_by)])
        custom_query_data['EXT_ORDER_BY_STR'] = 'order by '+', '.join(custom_query_data['EXT_ORDER_BY'])


    mainQuery = f""" from (
                    select {', '.join(query_selects)}
    				from {Schema}.IMAGE_FILES IMFI
                    inner join {Schema}.image_dirs imdir on imdir.imdir_auto_key = imfi.imdir_auto_key
    				left join {Schema}.tagged_images_precalc IMTAG_PRECALC on IMTAG_PRECALC.IMFI_AUTO_KEY = IMFI.IMFI_AUTO_KEY
    				{extra_joins}
    				{noWildcardQueryWhere}
    				{tagWhere} 
                    {imagesWhere} {main_categoryWhere} {sub_categoryWhere} {private_clause}

                    {tagHaving} 
                    {where_clauses} 
                    {order_by_str} 
                    {"limit :limit OFFSET :offset" if LIMIT > 0 else ''}
                    ) MAIN
                        """

    mainQueryCount = f""" select count(*) "full_count" from (
                    select IMFI.IMFI_AUTO_KEY
    				from {Schema}.IMAGE_FILES IMFI
    				inner join {Schema}.image_dirs imdir on imdir.imdir_auto_key = imfi.imdir_auto_key
    				left join {Schema}.tagged_images_precalc IMTAG_PRECALC on IMTAG_PRECALC.IMFI_AUTO_KEY = IMFI.IMFI_AUTO_KEY
    				{extra_joins}
    				{noWildcardQueryWhere}
    				{tagWhere} 
                    {imagesWhere} {main_categoryWhere} {sub_categoryWhere} {private_clause}
                    {tagHaving}
                    {where_clauses}
                    ) MAIN
                            """

    finalQuery = f"""
                    select *
                    {mainQuery}
                    """

    dataDict = {'offset': offset, 'limit': LIMIT, **search_parameters}

    ##DEBUG

    if kwargs.get('CUSTOM_QUERY', False) is True:
        return {'QUERY': finalQuery.replace('\n', '   ').replace('\t', ' '), 'DATA': dataDict,
                'RANDOM_SEED': random_seed,
                'CUSTOM_QUERY_DATA':custom_query_data}

    if kwargs.get('QUERY', kwargs.get('QUERY', None)) is not None:
        return {'QUERY': finalQuery.replace('\n', '   ').replace('\t', ' '), 'DATA': dataDict,
                'RANDOM_SEED': random_seed}

    finalQuery = finalQuery.format(SELECT_ITEMS=', '.join(query_selects))

    # DEBUG
    if check_debug('file_search'):
        print('Query___-\n', finalQuery)
        print('Dict____-\n', dataDict)

    imgs = convert_all_to_dict(db.session.execute(sql_text(finalQuery), dataDict))

    if kwargs.get("RAW_RESULTS", False) == True:
        return imgs

    for key in dataDict:
        finalQuery = finalQuery.replace(f':{key}', f"'{dataDict[key]}'")

    totalCount = 0
    if not kwargs.get('NO_COUNT', None):
        if imgs:
            count = convert_all_to_dict(
                db.session.execute(sql_text(mainQueryCount.replace(' limit  :limit offset :offset', '')),
                                   {'offset': offset, 'limit': LIMIT,
                                    **search_parameters}))
            totalCount = count[0]['full_count']
    # DEBUG
    finalized_data = {'FILES': [{'FILE_NAME': x['file_name'], 'MIMETYPE': x['mimetype'],
                                 'TAG_COUNT': x['tag_count'] if x['tag_count'] else 0,
                                 'TAG_AUTO_KEY': x['tag_auto_keys'].split('|*|') if x['tag_auto_keys'] else [],
                                 'TAGS': [y.lstrip('|').rstrip('|') for y in x['tags'].split('|*|')] if x[
                                     'tags'] else [],
                                 'ANIMATED': True if x.get('frames', 0) > 1 else False,
                                 'FRAMES': int(x.get('frames', 0)),
                                 'DURATION': round(float(x.get('duration', 0)), 2),
                                 'DIMENSIONS': [x.get('dimension_width', 0), x.get('dimension_height', 0)],
                                 'IMFI_AUTO_KEY': x['imfi_auto_key'],
                                 'ORIGINAL_DATE': x['original_date'],
                                 'IMDIR_AUTO_KEY': x['imdir_auto_key'],
                                 'PRIVATE':x['private'],
                                 'PAGE':PAGE
                                 } for x in imgs],

                      'COUNT': totalCount,
                      'PAGES': math.ceil(totalCount / LIMIT),
                      'RANDOM_SEED': random_seed}
    # finalized_data = {'FILES':imgs,
    #         'COUNT':totalCount,
    #         'PAGES':math.ceil(totalCount/limit),
    #         'RANDOM_SEED':random_seed}
    return finalized_data

    pass

@process_method('search_tags')
def search_tags(**kwargs):
    limit = 500
    try:
        limit = min(int(kwargs.get('LIMIT', 500)), 1000)
    except:
        pass
    custom_order = []
    order_by = '"VALUE"'
    if orderBy := kwargs.get('ORDER_BY', '') in ['LAST_USED']:
        if orderBy == 'LAST_USED':
            order_by = 'LAST_USED DESC'
    page = kwargs.get('PAGE', 1)
    if page:
        try:
            page = int(page)
        except:
            page = 1
    if page <= 0:
        page = 1

    offset = (page - 1) * limit

    mainCategory = kwargs.get("MAIN_CATEGORY", None)
    subCategory = kwargs.get("SUB_CATEGORY", '')
    tag_directory_crossover = ''

    searchText = kwargs.get('SEARCH_TEXT', None)
    filter_data = {'limit': limit, 'offset': offset}

    if searchText != None:
        searchtext = searchText.rstrip(',').lstrip(',')

        if searchText == '':  # If an empty search, return most recently used tags
            Query = f"""select TAG_AUTO_KEY,category||':'||detail "VALUE",to_char(LAST_USED,'MM-DD-YYYY') "LAST_USED",notes "NOTES"  from {Schema}.tags
                    ORDER BY LAST_USED desc
                    limit :limit offset :offset"""
            DATA = convert_all_to_dict(db.session.execute(sql_text(Query), filter_data))
            return DATA
        else:
            where_clauses = []
            filter = searchtext
            filter = filter.replace('|', ',|')
            filter = filter.rstrip(',')

            tag_count_total_join = ''

            base_filters = []
            filters = []

            if mainCategory:
                tag_directory_crossover = f"""inner join {Schema}.tag_directory_crossover tadicr on tadicr.tag_auto_key = tag.tag_auto_key and total_count > 0
                                             inner join {Schema}.image_dirs imdir on imdir.imdir_auto_key = tadicr.imdir_auto_key 
                                            """
                base_filters.append('imdir.main_category = :main_category')
                filter_data['main_category'] = mainCategory
                if subCategory:
                    subCatString = ','.join(
                        [f'upper(:subCategory_{catIndex}_)' for catIndex, x in enumerate(subCategory.split('|*|'))])
                    filter_data.update(
                        {f'subCategory_{catIndex}_': x for catIndex, x in enumerate(subCategory.split('|*|'))})
                    base_filters.append(f'imdir.sub_category in ({subCatString})')

            for searchItemIndex, searchItem in enumerate(re.split(r',(?![^\[]*\])', filter.rstrip(','))):
                searchItem = searchItem.strip().upper()



                bracketItems = re.findall(r'(?<!\\)\[(?:\\\\)*([^\\[\]]+)]', searchItem)
                if not bracketItems:
                    bracketItems = [searchItem]
                    beforeBracket = ''
                    afterBracket = ''
                else:
                    beforeBracket = searchItem.split('[')[0]
                    afterBracket = searchItem.split(']')[-1]

                for bracketItemIndex, searchitem in enumerate(bracketItems[0].split(',')):
                    wildcards = ['%', '%']
                    combinedSearchItem = f'{beforeBracket}{searchitem}{afterBracket}'
                    Type = 'or'
                    not_addition = ''
                    commandItem = False
                    splitCommandString = combinedSearchItem.split(':')
                    commandString = ''

                    if len(splitCommandString[0]) > 0:
                        if splitCommandString[0][0] == '#':
                            splitCommandString[0] = splitCommandString[0][1::]
                            commandString = splitCommandString[0]
                            dataString = ':'.join(splitCommandString[1::])
                            commandItem = True
                        else:
                            dataString = ':'.join(splitCommandString)
                    else:
                        dataString = ':'.join(splitCommandString)


                    if len(dataString) > 0:
                        if dataString[0] == '!':
                            not_addition = 'not'
                            dataString = dataString[1::]

                        if dataString[0] == '^':
                            wildcards[0] = ''
                            dataString = dataString.lstrip('^')
                        if dataString[-1] == '^' and len(searchitem) > 1:
                            wildcards[1] = ''
                            dataString = dataString.rstrip('^')

                    ###Tag Filters
                    if commandItem is True:
                        if commandString.lower() in ['last_used','lu']:
                            order_by = 'LAST_USED desc'
                        elif commandString.lower() in ['top_used','tu']:
                            tag_count_total_join = f'inner join (select tag_auto_key,sum(total_count) "total_count" from {Schema}.TAG_DIRECTORY_CROSSOVER group by tag_auto_key) tag_count on tag_count.tag_auto_key = tag.tag_auto_key'
                            order_by = 'total_count desc'
                        elif commandString.lower() in ['old','oldest']:
                            order_by = 'timestamp asc'
                        elif commandString.lower() in ['newest','new']:
                            order_by = 'timestamp desc'
                        elif commandString.lower() in ['notes','note']:
                            filters.append(
                                [Type,
                                 f"tag.notes {not_addition} ilike :search_note_{int(searchItemIndex)}_{bracketItemIndex}_"])
                            filter_data[
                                f'search_note_{int(searchItemIndex)}_{bracketItemIndex}_'] = f'{wildcards[0]}{dataString if len(dataString)>0 else "_"}{wildcards[1]}'.upper()
                        elif combinedSearchItem[1::].split(':')[0].lower() in ['regex_notes','regex_note'] and len(dataString) > 0:
                            filters.append(
                                [Type,
                                 f"{not_addition} regexp_like(upper(tag.notes),upper(:search_note_{int(searchItemIndex)}_{bracketItemIndex}_))"])
                            filter_data[
                                f'search_note_{int(searchItemIndex)}_{bracketItemIndex}_'] = f'{wildcards[0]}{dataString if len(dataString)>0 else "_"}{wildcards[1]}'.upper()
                    else:
                        filters.append(
                            [Type,
                             f"tag.category||':'||tag.detail {not_addition} like :search_{int(searchItemIndex)}_{bracketItemIndex}_"])
                        filter_data[
                            f'search_{int(searchItemIndex)}_{bracketItemIndex}_'] = f'{wildcards[0]}{":".join(dataString.split(":")[0:2])}{wildcards[1]}'.upper()

                        custom_order_index = None

                        if dataString.count(':') >= 2:
                            potentialOrderItems = dataString.split(':')[2::][0:2][::-1] #Flips the order so the outer bracket is higher than the inner bracket float conversion
                            if potentialOrderItems:
                                try:
                                    custom_order_index = float('.'.join([str(x.replace('.','')) for x in potentialOrderItems if x.replace('.','').isdigit()]))
                                except Exception as e:
                                    raise e
                        if custom_order_index is not None and isinstance(custom_order_index,float):
                            try:
                                if '%' in wildcards:
                                    isWildcard = True
                                else:
                                    isWildcard = False
                                custom_order.append([custom_order_index+(bracketItemIndex/100),f'{wildcards[0]}{":".join(dataString.split(":")[0:2])}{wildcards[1]}',isWildcard])
                            except:pass



            if len(base_filters) > 0:
                where_clauses.append([])
                for xy, f in enumerate(base_filters):
                    where_clauses[-1].append(f)
                where_clauses[-1] = f"({' and '.join(where_clauses[-1])})"
            if len(filters) > 0:
                where_clauses.append([])
                for xy, f in enumerate(filters):
                    if xy == 0:
                        where_clauses[-1].append(f[1])
                    else:
                        where_clauses[-1].append(' '.join(f))
                where_clauses[-1] = f"({' '.join(where_clauses[-1])})"

            where_clauses_str = ''
            if len(where_clauses) > 0:
                where_clauses_str = f'where {" and ".join(where_clauses)}'


            custom_order_str = ''
            if len(custom_order) > 0:
                custom_order_str = "CASE "
                max_order_index = -1
                for xy,customOrder in enumerate(custom_order):
                    if customOrder[0] > max_order_index:
                        max_order_index = customOrder[0]
                    custom_order_str+=f"""\n When MAIN.\"VALUE\" {'like' if customOrder[2] is True else '='} :customOrder_{xy}_ then {customOrder[0]}"""
                    filter_data[f'customOrder_{xy}_'] = f'{customOrder[1]}'.upper()
                if max_order_index <= 0:
                    max_order_index = 1
                custom_order_str+= f'\n else {round(max_order_index,0)+1}\n END, '



            Query = f"""
                            select * from (
                            select distinct tag.TAG_AUTO_KEY, tag.CATEGORY||':'||tag.DETAIL \"VALUE\" , tag.LAST_USED, tag.timestamp {', tag_count.total_count' if tag_count_total_join else ''}, tag.notes
                            from {Schema}.TAGS TAG
                            {tag_directory_crossover}
                            {tag_count_total_join}
                            {where_clauses_str}
                            ) MAIN
                            order by \n{custom_order_str} {order_by} limit :limit offset :offset
                            """

            if check_debug('tag_search'):
                print('[DEBUG]','tag_search Query:____\n', Query,'\nFilter:',filter_data)
            if kwargs.get('QUERY', None):
                return {'query': Query.replace('\n',''), 'filtered_data': filter_data}
            DATA = convert_all_to_dict(db.session.execute(sql_text(Query),filter_data))

            return DATA
    return []

def preprocess_tag_similar_tags(**kwargs):
    #Only works for single files
    IMFI_AUTO_KEYS = kwargs.get('IMFI_AUTO_KEYS', '').split(',')
    CONTEXT = kwargs.get('CONTEXT',None)
    PARAMETERS = kwargs.get('PARAMETERS',None)
    TAG_AUTO_KEYS = kwargs.get('TAG_AUTO_KEYS',None)
    SEARCH_TYPE = kwargs.get('SEARCH_TYPE',None)
    if SEARCH_TYPE == 'FILE' and len(IMFI_AUTO_KEYS[0]) > 0 and CONTEXT is not None and PARAMETERS is not None and TAG_AUTO_KEYS is not None:
        if CONTEXT.upper() == 'SIMTAG' or CONTEXT.upper() == 'SIMILAR TAG':
            #check that the IMFI_AK is correct
            for imfi_auto_key in IMFI_AUTO_KEYS:
                IMFI_AK = imagesorter_db.IMAGE_FILES.query.filter_by(imfi_auto_key=imfi_auto_key).first()
                if not IMFI_AK:
                    raise Exception(f"IMFI_AUTO_KEY ({imfi_auto_key}) INVALID. (No record found).")
            #get all current tags
            # IMFI_TAGS = imagesorter_db.TAGGED_IMAGES.query.filter_by(imfi_auto_key=IMFI_AK.imfi_auto_key).all()
            existing_tag_string = ''

            where_list = []
            dataDict = {}
            order_by = 'order by "COUNT" desc'
            if IMFI_AK:
                dataDict['IMFI_AUTO_KEY'] = IMFI_AK.imfi_auto_key
                where_list.append(f'IMTAG.imfi_auto_key in ({",".join([str(x) for x in IMFI_AUTO_KEYS])})')

            for paramIndex,paramSplit in enumerate(PARAMETERS.split('|')):
                if paramIndex == 0:
                    where_list_tag_joins = []
                    for tagIndex,tag in enumerate(paramSplit.split(',')):
                        temp_where_list_tag_joins = []
                        tagSplit = tag.split(':')
                        if len(tagSplit) > 1:
                            temp_where_list_tag_joins.append(f'(tag.detail = :tag_detail_{paramIndex}_{tagIndex}_)')
                            dataDict[f'tag_detail_{paramIndex}_{tagIndex}_'] = tagSplit[1].upper()
                        temp_where_list_tag_joins.append(f'(tag.category = :tag_category_{paramIndex}_{tagIndex}_)')
                        dataDict[f'tag_category_{paramIndex}_{tagIndex}_'] = tagSplit[0].upper()
                        where_list_tag_joins.append(' and '.join(temp_where_list_tag_joins))
                    where_list.append(f"({' or '.join(where_list_tag_joins)})")
                elif paramSplit == '!' and 'EXCLUDE_EXISTING' not in dataDict:
                    if TAG_AUTO_KEYS:
                        where_list.append(
                            f'(tag_2.tag_auto_key not in ({",".join([str(x) for x in TAG_AUTO_KEYS.split(",")])}))')
                        # dataDict['EXCLUDE_EXISTING'] = True
                elif paramSplit[0] == '!':
                    paramSplit = paramSplit.lstrip('!')
                    where_result_category_not_list = []
                    for resultCateIndex,notcategory in enumerate(paramSplit.split(',')):
                        where_result_category_not_list.append(f'tag_2.category != :NOT_RESULT_CATEGORY_{paramIndex}_{resultCateIndex}_')
                        dataDict[f'NOT_RESULT_CATEGORY_{paramIndex}_{resultCateIndex}_'] = notcategory.upper()
                    where_list.append(f"({' and '.join(where_result_category_not_list)})")
                    pass
                elif '@' == paramSplit[0]:
                    #CATEGORY_RESULT_TYPE
                    paramSplit = paramSplit.lstrip('@')
                    where_list_category_joins = []
                    for categoryIndex,category in enumerate(paramSplit.split(',')):
                        where_list_category_joins.append(f'tag_2.category = :RESULT_CATEGORY_{paramIndex}_{categoryIndex}')
                        dataDict[f'RESULT_CATEGORY_{paramIndex}_{categoryIndex}'] = category.upper()
                    where_list.append(f'({" or ".join(where_list_category_joins)})')
                elif paramSplit in ['#lu','#last used','#last_used']:
                    order_by = 'order by tag_2.last_used desc'


            if IMFI_AK:
                where_list_string = '\n and '.join(where_list)
                query = f"""
                    select tag_2.tag_auto_key,tag_2.category||':'||tag_2.detail "VALUE",count(tag_2.tag_auto_key) "COUNT",tag_2.last_used
                    from {Schema}.tagged_images imtag
                    inner join {Schema}.tags tag on tag.tag_auto_key = imtag.tag_auto_key
                    inner join (select tag_auto_key,imfi_auto_key from {Schema}.tagged_images) imtag_2 on imtag_2.tag_auto_key = tag.tag_auto_key
                    inner join (select tag_auto_key,imfi_auto_key from {Schema}.tagged_images imtag_3) imtag_3 on imtag_3.imfi_auto_key = imtag_2.imfi_auto_key
                    inner join {Schema}.tags tag_2 on tag_2.tag_auto_key  = imtag_3.tag_auto_key
                    where {where_list_string}
                    group by tag_2.tag_auto_key,tag_2.category,tag_2.detail,tag_2.last_used
                    {order_by}
                    limit 50;
                """

                if check_debug('preprocess_tag_similar'):
                    print('[DEBUG]',__name__,'preprocess_tag_similar','QUERY')
                    print(query)
                    print(dataDict)


                results = convert_all_to_dict(db.session.execute(sql_text(query),dataDict))
                if results:
                    return results
                else:
                    return []
        pass
    return None
    pass


def get_tag_details(tag_auto_key,**kwargs):
    """
    returns crossover information between directories, along with LAST_USED date and DATE_CREATED
    :param tag_auto_key:
    :return:
    """
    try:
        tag_auto_key = int(tag_auto_key)
    except Exception as e:
        raise e

    dataDict = {'tag_auto_key': tag_auto_key}

    search_text = kwargs.get('SEARCH_TEXT','')


    # where_gt_zero = ''
    # if '#imdir:' in search_text.lower():
    where_gt_zero = "and tadicr.total_count > 0"

    crossover_query = f"""SELECT imdir.*,tadicr.total_count FROM
        {Schema}.TAG_DIRECTORY_CROSSOVER TADICR
        inner join {Schema}.IMAGE_DIRS imdir on imdir.imdir_auto_key = tadicr.imdir_auto_key
        where tadicr.tag_auto_key = :tag_auto_key
        {where_gt_zero}
        order by imdir.MAIN_CATEGORY asc,imdir.SUB_CATEGORY asc,tadicr.total_count desc
        """

    if kwargs.get("QUERY", None) is not None:
        return {'QUERY': crossover_query, 'DATA': dataDict}

    crossover_results = convert_all_to_dict(db.session.execute(sql_text(crossover_query), dataDict))



    try:
        tag_info = convert_all_to_dict(db.session.execute(sql_text(f"""
                        SELECT * FROM {Schema}.TAGS TAG
                        WHERE TAG.TAG_AUTO_KEY = :tag_auto_key
                    """), {'tag_auto_key': tag_auto_key}))[0]
    except:
        tag_info = {}

    return {'crossover_info': crossover_results, 'tag_info': tag_info}


def search_tag_info(**kwargs):
    """
        yields all categories and subcategories for the searched text
        :return:
        """
    try:
        limit = min(int(kwargs.get('LIMIT', 1000)), 5000)
    except:
        limit = 1000
    try:
        page = int(kwargs.get('PAGE', 1))
    except:
        page = 1

    offset = limit * (page - 1)

    where_statements = []
    search_text = kwargs.get('SEARCH_TEXT', '').upper()

    wildcards = ['%', '%']
    order_by_override = ''
    where_statement = "tag.category||':'||tag.detail like :search_text"

    where_statements.append(where_statement)

    data_dict = {'limit': limit, 'offset': offset}
    comparators = ['<', '=', '>', '>=', '<=']
    search_text_split = search_text.split(',')
    search_text = ''

    imdir_selection = []

    if len(search_text_split) > 0:
        for xy, splitString in enumerate(search_text_split):
            if len(splitString) > 0 and splitString[0] == '#':
                splitString = splitString.lstrip('#')
                param, value = splitString.upper().split(':')
                if param.upper() == 'TAG_COUNT' and ':' in splitString:

                    if value[0] in comparators:
                        for comparator in comparators:
                            if value[0:len(comparator)] == comparator:
                                break
                        value = value[1::]
                        where_statements.append(
                            f' exists (select 1 from {Schema}.tag_directory_crossover tadicr where tadicr.tag_auto_key = tag.tag_auto_key group by tadicr.tag_auto_key having sum(tadicr.total_count) {comparator} cast(:total_count_{xy}_ as INTEGER))')
                        data_dict[f'total_count_{xy}_'] = value
                    elif '-' in value:
                        value1, value2 = value.split('-')
                        where_statements.append(
                            f' exists (select 1 from {Schema}.tag_directory_crossover tadicr where tadicr.tag_auto_key = tag.tag_auto_key group by tadicr.tag_auto_key having sum(tadicr.total_count) >= cast(:total_count_{xy}_0_ as INTEGER))')
                        where_statements.append(
                            f' exists (select 1 from {Schema}.tag_directory_crossover tadicr where tadicr.tag_auto_key = tag.tag_auto_key group by tadicr.tag_auto_key having sum(tadicr.total_count) <= cast(:total_count_{xy}_1_ as INTEGER))')
                        data_dict[f'total_count_{xy}_0_'] = value1
                        data_dict[f'total_count_{xy}_1_'] = value2
                    elif value == '0':
                        where_statements.append(
                            f'not exists (select 1 from {Schema}.tag_directory_crossover tadicr where tadicr.tag_auto_key = tag.tag_auto_key group by tadicr.tag_auto_key having sum(tadicr.total_count) >= 1)')
                    else:
                        where_statements.append(
                            f' exists (select 1 from {Schema}.tag_directory_crossover tadicr where tadicr.tag_auto_key = tag.tag_auto_key group by tadicr.tag_auto_key having sum(tadicr.total_count) = cast(:total_count_{xy}_ as INTEGER))')
                        data_dict[f'total_count_{xy}_'] = value
                elif param == 'ORDERBY':
                    if value == 'COUNT':
                        order_by_override = 'total_count desc'
                    elif value == 'COUNT_ASC':
                        order_by_override = 'total_count asc'
                elif param in ['IMDIR','IMDIR_AUTO_KEY']:
                    where_statements.append(
                        f' exists (select 1 from {Schema}.tag_directory_crossover tadicr where tadicr.tag_auto_key = tag.tag_auto_key and tadicr.imdir_auto_key = :imdir_ak_{xy}_)')
                    data_dict[f'imdir_ak_{xy}_'] = value
                    imdir_selection.append(f'imdir_ak_{xy}_')
                elif param in ['TAG_AUTO_KEY','TAG_AK']:
                    where_statements.append(f'tag.tag_auto_key = :tag_ak_{xy}_')
                    data_dict[f'tag_ak_{xy}_'] = value


            else:
                search_text = splitString

    data_dict['search_text'] = f"{wildcards[0]}{search_text}{wildcards[1]}"

    if len(search_text) > 0:
        if search_text[0] == '^':
            wildcards[0] = ''
            search_text = search_text[1::]
            # search_text[0] = '';
        if search_text[-1] == '^':
            wildcards[1] = ''
            search_text = search_text[:-1]



    if len(imdir_selection) > 0:
        imdir_select_join = ','.join([f":{x}" for x in imdir_selection])
        where_statements.append(f'tadicr.imdir_auto_key in ({imdir_select_join})')
        where_statements.append(f'tadicr.total_count > 0')



    if kwargs.get('SEARCH_TYPE', 'CATEGORY') == 'CATEGORY':
        if main_category := kwargs.get("MAIN_CATEGORY", None):
            inner_join_statement = f"""
            left join {Schema}.TAG_DIRECTORY_CROSSOVER tadicr on tadicr.tag_auto_key = tag.tag_auto_key
            inner join {Schema}.IMAGE_DIRS IMDIR on IMDIR.IMDIR_AUTO_KEY = TADICR.IMDIR_AUTO_KEY and IMDIR.MAIN_CATEGORY = upper(:MAIN_CATEGORY)
            """
            data_dict['MAIN_CATEGORY'] = main_category
        else:
            inner_join_statement = f'left join {Schema}.TAG_DIRECTORY_CROSSOVER tadicr on tadicr.tag_auto_key = tag.tag_auto_key'

        tag_info_query = f"""
            select tag.category,count(distinct tag.tag_auto_key) "total_count" 
            from {Schema}.tags tag
            {inner_join_statement}
            where {' and '.join(where_statements)}
            group by tag.category
            --having count(tag.tag_auto_key) > 0
            order by {order_by_override if len(order_by_override) > 0 else 'tag.category asc'}
            limit :limit offset :offset
            """
        if check_debug('tag_search_info'):
            print('[DEBUG]',__name__, 'CATEGORY QUERY:\n', tag_info_query, '\n', data_dict, '\n', kwargs)
        if request.args.get("QUERY",None) is not None:
            return {'QUERY':tag_info_query.replace('\n',''),'DATA':data_dict,'kwargs':kwargs}

        results = convert_all_to_dict(db.session.execute(sql_text(tag_info_query), data_dict))

        return results
    elif kwargs.get('SEARCH_TYPE', 'CATEGORY') == 'DETAILS':
        if main_category := kwargs.get("MAIN_CATEGORY", None):
            inner_join_statement = f"""
            inner join {Schema}.IMAGE_DIRS IMDIR on IMDIR.IMDIR_AUTO_KEY = TADICR.IMDIR_AUTO_KEY and IMDIR.MAIN_CATEGORY = upper(:MAIN_CATEGORY)
            """
            data_dict['MAIN_CATEGORY'] = main_category
        else:
            inner_join_statement = ''
        if kwargs.get('CATEGORY', None) is None:
            raise Exception("Please supply the CATEGORY value.")
        else:
            category = kwargs.get('CATEGORY', None)

        category_statement = 'tag.category = :category'

        added_statements = [category_statement]
        if len(search_text) > 0:
            added_statements.extend(where_statements)
        else:
            added_statements.extend(where_statements[1::])
        data_dict['category'] = category

        tag_info_query = f"""
                    select tag.tag_auto_key,tag.detail,coalesce(sum(tadicr.total_count),0) "total_count" 
                    from {Schema}.tags tag
                    left join {Schema}.TAG_DIRECTORY_CROSSOVER tadicr on tadicr.tag_auto_key = tag.tag_auto_key
                    {inner_join_statement}
                    where {' and '.join(added_statements)}
                      
                    group by tag.detail,tag.tag_auto_key
                    --having coalesce(sum(tadicr.total_count),0) > 0
                    order by {order_by_override if len(order_by_override) > 0 else 'tag.detail asc'}
                    limit :limit offset :offset
                    """

        if check_debug('tag_search_info'):
            print('[DEBUG]',__name__, 'DETAIL QUERY:\n', tag_info_query, '\n', data_dict, '\n', kwargs)
        if request.args.get("QUERY",None) is not None:
            return {'QUERY':tag_info_query.replace('\n',''),'DATA':data_dict,'kwargs':kwargs}
        results = convert_all_to_dict(db.session.execute(sql_text(tag_info_query), data_dict))
        return results



@process_method('relocate_files')
def move_files_to_folder(**kwargs):
    """
    Required in request
        SEARCH_QUERY
        DESTINATION_PATH
            - This requires the full path of the directory to send to
        MAIN_CATEGORY
        (optional) SUB_CATEGORY

    :return:
    """
    resetLoadingStatus('moving_files')
    searchQueryString = kwargs.get("SEARCH_QUERY", '').replace('#random', '').replace('#rand','').replace(',,',',')
    destinationPathKey = kwargs.get('DESTINATION_PATH_KEY', None)
    destinationPath = kwargs.get('DESTINATION_PATH', None)
    destinationPathSubfolder = kwargs.get('DESTINATION_PATH_SUBFOLDER', '')
    mainCategory = kwargs.get('MAIN_CATEGORY', None)
    subCategory = kwargs.get('SUB_CATEGORY', '').replace('\\', '')



    exceptionList = []
    if len(searchQueryString.replace('|', '|*|').replace(',', '|*|').split('|*|')) < 3:
        exceptionList.append("Query needs to be at least 3 variables long.")
    if not destinationPathKey:
        exceptionList.append("Destination Path Key Required.")
    if not destinationPath:
        exceptionList.append("Destination Path Required.")
    directory_record = imagesorter_db.IMAGE_DIRS.query.filter_by(imdir_auto_key=destinationPathKey,
                                                                 path=destinationPath).first()
    if not directory_record:
        exceptionList.append("Destination Path Record Not Found.")
    if not mainCategory:
        exceptionList.append("Main Category Required.")

    duplicate_images = convert_all_to_dict(db.session.execute(sql_text(f"""
    select 1 from {Schema}.images img
    group by file_name
    having count(file_name) > 1
    """)))
    if len(duplicate_images) > 0:
        exceptionList.append(
            'Duplicate images found in the database. Please remove all duplicate file_name entries before continuing.')

    if exceptionList:
        raise Exception(f"Error:{exceptionList}")

    ## QUERY=True usage is fine here, the 'order by' is not needed from the original query
    searchQuery = search_files(searchQueryString, LIMIT=-1, QUERY=True, MAIN_CATEGORY=mainCategory,
                               SUB_CATEGORY=subCategory)

    searchQuery['DATA']['limit'] = 100
    searchQuery['DATA']['OFFSET'] = 0  # Shouldn't be needed since they'll be moved
    searchQuery['DATA']['imdir_auto_key'] = directory_record.imdir_auto_key
    filesTransferred = 0
    old_localImageDirectoryDB = None
    new_localImageDirectoryDB = local_image_directory_db(directory_record)

    if destinationPathSubfolder:
        if len(destinationPathSubfolder) < 5:
            raise Exception("Destination Subfolder needs to be at least 5 characters long")
        backslash = "\\"
        new_path = directory_record.path + f'\\{destinationPathSubfolder.split("/")[0].split(backslash)[0]}'
    else:
        new_path = directory_record.path.rstrip('\\').rstrip('/')  # To ensure there's just one forward slash
    try:
        os.mkdir(new_path)
    except FileExistsError:
        pass
    except OSError as e:
        raise e

    lastImdir_AK = -1
    # update the path of every file IF it isn't already in the destination path

    imdir_to_update = {}

    new_path_escaped = new_path.replace('\\','\\\\')+'\\\\'
    searchQuery['DATA']['new_path'] = f'{new_path_escaped}%'
    expectedCount = db.session.execute(
        sql_text(f"""
            select count(*) from (
            {searchQuery['QUERY']}
            ) MAIN
            inner join {Schema}.IMAGE_FILES IMFI on IMFI.IMFI_AUTO_KEY = MAIN.IMFI_AUTO_KEY
            WHERE IMFI.PATH not like :new_path
            """)
        , searchQuery['DATA']).fetchone()

    if expectedCount:
        pbar = tqdm(total=expectedCount[0])
    else:
        pbar = tqdm()

    previousIMFI = []

    pbar.set_description(f'Transferred Files: {0}')
    while results := convert_all_to_dict(db.session.execute(sql_text(
            f"""
            select * from (
            {searchQuery['QUERY']}
            ) MAIN
            inner join {Schema}.IMAGE_FILES IMFI on IMFI.IMFI_AUTO_KEY = MAIN.IMFI_AUTO_KEY
            WHERE IMFI.PATH not like :new_path
            order by IMFI.path asc
            limit :limit offset :offset
            """)
            , searchQuery['DATA'])):
        tempPreviousIMFI = []
        for record in results:
            if record['imfi_auto_key'] in previousIMFI:
                searchQuery['DATA']['offset']+=1
            else:
                tempPreviousIMFI.append(record['imfi_auto_key'])
                image_record = imagesorter_db.IMAGES.query.filter_by(file_name=record['file_name']).all()
                if len(image_record) > 1:
                    raise Exception(f"Duplicate images found. Please clean up any duplicate images before proceeding.")
                if lastImdir_AK != record['imdir_auto_key']:
                    old_localImageDirectoryDB = local_image_directory_db(
                        imagesorter_db.IMAGE_DIRS.query.filter_by(imdir_auto_key=record['imdir_auto_key']).first())
                    lastImdir_AK = record['imdir_auto_key']

                # check if paths are the same
                # raise
                if record['path'] != os.path.join(new_path, record['file_name']):
                    # Remove image from local DB
                    old_localImageDirectoryDB.remove_file_from_db(record['file_name'], record['path'])
                    # move image to new folder
                    try:
                        shutil.move(record['path'], new_path)
                    except:
                        pass

                    # Now add it to the new localDB
                    new_localImageDirectoryDB.add_file_to_db(record['file_name'],
                                                             os.path.join(new_path, record['file_name']))
                    # update the postgres database, both IMAGES and IMAGE_FILES
                    image_record = imagesorter_db.IMAGES.query.filter_by(file_name=record['file_name']).first()
                    imfi_record = imagesorter_db.IMAGE_FILES.query.filter_by(file_name=record['file_name']).first()

                    if imfi_record.imdir_auto_key != directory_record.imdir_auto_key:
                        if imfi_record.imdir_auto_key not in imdir_to_update:
                            imdir_to_update[imfi_record.imdir_auto_key] = {}
                        if new_localImageDirectoryDB.dir_record.imdir_auto_key not in imdir_to_update:
                            imdir_to_update[new_localImageDirectoryDB.dir_record.imdir_auto_key] = {}

                        for imtag_record in imagesorter_db.TAGGED_IMAGES.query.filter_by(
                                imfi_auto_key=imfi_record.imfi_auto_key).all():
                            if imtag_record.tag_auto_key not in imdir_to_update[imfi_record.imdir_auto_key]:
                                imdir_to_update[imfi_record.imdir_auto_key][
                                    imtag_record.tag_auto_key] = 0  # [imtag_record.tag_auto_key,-1])
                            if imtag_record.tag_auto_key not in imdir_to_update[
                                new_localImageDirectoryDB.dir_record.imdir_auto_key]:
                                imdir_to_update[new_localImageDirectoryDB.dir_record.imdir_auto_key][
                                    imtag_record.tag_auto_key] = 0  # .append([imtag_record.tag_auto_key,1])
                            imdir_to_update[imfi_record.imdir_auto_key][imtag_record.tag_auto_key] -= 1
                            imdir_to_update[new_localImageDirectoryDB.dir_record.imdir_auto_key][imtag_record.tag_auto_key] += 1
                    for file_record in [image_record, imfi_record]:
                        file_record.path = os.path.join(new_path, record['file_name'])
                        file_record.imdir_auto_key = new_localImageDirectoryDB.dir_record.imdir_auto_key


                    filesTransferred += 1
                    pbar.set_description(f'Transferred Files: {filesTransferred}')
                    db.session.commit()

                    if filesTransferred % 1000 == 0 and filesTransferred != 0:
                        for imdir_ak in imdir_to_update:
                            for tag_ak in imdir_to_update[imdir_ak]:
                                valueChange = imdir_to_update[imdir_ak][tag_ak]
                                if valueChange > 0:
                                    imagesorter_db.tag_directory_crossover_update(tag_ak, imdir_ak, valueChange)
                                    imdir_to_update[imdir_ak][tag_ak] = 0


                else:
                    pass
                pbar.update(1)
                updateLoadingStatus('moving_files', pbar)
                if checkLoadingOperationCancelled('moving_files') is True:
                    break
        previousIMFI = list(tempPreviousIMFI)
        if checkLoadingOperationCancelled('moving_files') is True:
            break
    updateLoadingStatus('moving_files', pbar)

    pbar.close()
    for imdir_ak in imdir_to_update:
        for tag_ak in imdir_to_update[imdir_ak]:
            valueChange = imdir_to_update[imdir_ak][tag_ak]
            imagesorter_db.tag_directory_crossover_update(tag_ak, imdir_ak, valueChange)

    if filesTransferred > 0:
        db.session.commit()

    return f'Transferred Files: {filesTransferred}'

    return ''


class local_image_directory_db:
    def __init__(self, dir_record):
        assert dir_record != None, "IMAGE_DIRECTORY_RECORD IS NULL"
        self.dir_record = dir_record
        self.conn = sql.connect(
            f'{os.path.join(dir_record.path, "image_dir")}_{dir_record.imdir_auto_key}.db')
        self.create_tables()
        self.bypassFileCheck = False

    def create_tables(self):
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS TABLE_INFO (
                TI_AUTO_KEY INTEGER PRIMARY KEY,
                LAST_UPDATED VARCHAR NOT NULL,
                PATH VARCHAR NOT NULL UNIQUE
            )
        """)

        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS IMAGES_LOADED (
                IMLO_AUTO_KEY INTEGER PRIMARY KEY,
                FILE_NAME VARCHAR NOT NULL,
                FILE_PATH VARCHAR NOT NULL UNIQUE
            )
        """)

        self.conn.execute(f"""
            CREATE INDEX IF NOT EXISTS
            INDEX_IMLO_FILES on IMAGES_LOADED (
                FILE_NAME asc,
                FILE_PATH asc
            )
        """)

        self.conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS
                    INDEX_PATH on IMAGES_LOADED (
                        FILE_PATH asc
                    )
                """)
        self.conn.commit()

    def check_file_already_loaded(self, FILE_PATH):
        """
        Check if file is in the DB
        :param FILE_PATH:
        :return: BOOL
        """
        if self.conn.execute("SELECT 1 from IMAGES_LOADED WHERE FILE_PATH = :FP", {'FP': FILE_PATH}).fetchone() is None:
            return False
        else:
            return True

    def add_file_to_db_executemany(self, FILE_LIST):
        self.conn.executemany("INSERT OR IGNORE INTO IMAGES_LOADED(FILE_NAME,FILE_PATH) VALUES(?,?)", FILE_LIST)
        self.conn.commit()

    def add_file_to_db(self, FILE_NAME, FILE_PATH):
        self.conn.execute("INSERT OR IGNORE INTO IMAGES_LOADED(FILE_NAME,FILE_PATH) VALUES(?,?)",
                          (FILE_NAME, FILE_PATH))
        self.conn.commit()

    def remove_file_from_db(self, FILE_NAME, FILE_PATH):
        self.conn.execute("DELETE from IMAGES_LOADED WHERE FILE_NAME = ? AND FILE_PATH = ?", (FILE_NAME, FILE_PATH))
        self.conn.commit()

    def remove_all_files_from_db(self,autocommit=True):
        self.conn.execute("DELETE from IMAGES_LOADED")
        if autocommit:
            self.conn.commit()

    def load_files(self):
        toUpload = []
        totalToUpload = 0
        # LIST_add_to_db = []
        # imdir_path = self.dir_record.path
        startTime = dt.datetime.now()
        resetLoadingStatus('file_loading')
        updateLoadingStatus('file_loading',**{'processing': False,
                                            'files_uploaded': 0,
                                            'time_started': dt.datetime.now().strftime('%Y-%m-%d - %I:%M:%S %p'),
                                            'current_folder': '.',
                                            'total_files': 0,
                                            'last_updated':dt.datetime.now().strftime('%Y-%m-%d - %I:%M:%S %p')})


        updateLoadingStatus('file_loading',processing=True)

        print(f"\nChecking files in: {self.dir_record.path}\n")

        for root, folders, files in os.walk(self.dir_record.path):
            pbar = tqdm(files)
            updateLoadingStatus('file_loading', current_folder=os.path.relpath(root, self.dir_record.path),total_files=len(files))
            if checkLoadingOperationCancelled('file_loading') is True:
                break
            for file in pbar:
                updateLoadingStatus('file_loading',pbar)
                if checkLoadingOperationCancelled('file_loading') is True:
                    break
                
                pbar.set_description(f'totalToUpload:{totalToUpload}')
                ext = file.split('.')[-1]
                if ext.upper() in ['PNG', 'GIF', 'JPEG', 'JPG', 'WEBP', 'MP4','WEBM']:
                    full_path = os.path.join(root, file)
                    if self.check_file_already_loaded(full_path) == False or self.bypassFileCheck:
                        # if 1 == 1:
                        toUpload.append([file, full_path])
                        totalToUpload += 1
                        if len(toUpload) >= 20:
                            self.upload_file_list(toUpload, startTime)
                            toUpload = []

                        updateLoadingStatus('file_loading', total_to_upload=totalToUpload,files_uploaded=totalToUpload)
        if toUpload:
            self.upload_file_list(toUpload, startTime)

        imagesorter_db.update_all_file_data()
        updateLoadingStatus('file_loading', pbar,total_to_upload=totalToUpload,last_updated=dt.datetime.now().strftime('%Y-%m-%d - %I:%M:%S %p'),processing=False)
        self.dir_record.last_updated = dt.datetime.now()
        db.session.commit()
        return Main.updateStatus['file_loading']

    def upload_file_list(self, toUpload, startTime):
        bulkUpload = []
        uploadedFile = []
        # totalFiles = len(toUpload)
        # print("Loading files")
        # pbar = tqdm(toUpload)
        # imdir_path = self.dir_record.path
        counter = 0
        insert_query = sql_text(f"""INSERT INTO {Schema}.IMAGES (file_name,path,original_date,file_extension,mimetype,category,source_folder,available_file,has_thumbnail,imdir_auto_key) VALUES(:file_name,:path,:original_date,:file_extension,:mimetype,:category,:source_folder,:available_file,:has_thumbnail,:imdir_auto_key)
                ON CONFLICT DO NOTHING
                """)
        for file in toUpload:
            counter += 1
            # Main.folderStatus[imdir_path]['completed'] = ((pbar.n / totalFiles * 100) - 1)
            # Main.folderStatus[imdir_path]['completed'] = ((counter / totalFiles * 100) - 1)

            mime = mimetypes.guess_type(file[0])
            try:
                st_mtime = pathlib.Path(file[1]).stat().st_ctime
            except:
                st_mtime = pathlib.Path(file[1]).stat().st_mtime
            original_date = dt.datetime.fromtimestamp(st_mtime)

            if mime:
                ext = file[0].split('.')[-1]
                if mime[0] == None and ext.upper() == 'WEBP':
                    mimeType = "image/webp"
                else:
                    mimeType = mime[0]

            if mimeType == None:
                raise Exception(f"File mimetype not found. - {file[1]} - Mime:{mime}")

            IMFI_AUTO_KEY = get_imfi_auto_key(file[0])

            if IMFI_AUTO_KEY:
                IMFI_AK = imagesorter_db.IMAGE_FILES.query.filter_by(imfi_auto_key=IMFI_AUTO_KEY).first()
                if IMFI_AK.path != file[1]:
                    IMFI_AK.path = file[1]
                if IMFI_AK.mimetype != mimeType:
                    IMFI_AK.mimetype = mimeType
                if IMFI_AK.original_date != original_date:
                    IMFI_AK.original_date = original_date
                if IMFI_AK.available_file == False:
                    IMFI_AK.available_file = True

            # file_name,path,original_date,file_extension,mimetype,category,source_folder,available_file,has_thumbnail
            uploadTuple = {'file_name':file[0], 'path':file[1], 'original_date':original_date, 'file_extension':ext, 'mimetype':mimeType, 'category':self.dir_record.sub_category,'source_folder':self.dir_record.path,
                 'available_file':True, 'has_thumbnail':False, 'imdir_auto_key':self.dir_record.imdir_auto_key}
            bulkUpload.append(uploadTuple)

            uploadedFile.append(file)
            if len(bulkUpload) > 1000:
                db.session.execute(insert_query, bulkUpload)
                db.session.commit()
                bulkUpload = []
                self.add_file_to_db_executemany(uploadedFile)
                uploadedFile = []
        if bulkUpload:
            db.session.execute(insert_query, bulkUpload)
            db.session.commit()
            bulkUpload = []
            self.add_file_to_db_executemany(uploadedFile)
            uploadedFile = []

        # Double-check the IMFI_AKs on this one
        for file in toUpload:
            filepath = file[1]
            update_mismatched_imfi_img(filepath)
        self.dir_record.last_updated = startTime
        db.session.commit()

        return True


def lockNewLargeProcesses(lockIdentifiers):
    if isinstance(lockIdentifiers,str):
        lockIdentifiers = [lockIdentifiers]
    if isinstance(lockIdentifiers,list) is False:
        raise Exception("lockIdentifiers must be a list or string object.")
    for lockIdentifier in lockIdentifiers:
        Main.lockNewLargeProcesses[lockIdentifier.lower()] += 1


def releaseNewLargeProcesses(lockIdentifiers):
    if isinstance(lockIdentifiers,str):
        lockIdentifiers = [lockIdentifiers]
    if isinstance(lockIdentifiers,list) is False:
        raise Exception("lockIdentifiers must be a list or string object.")
    for lockIdentifier in lockIdentifiers:
        Main.lockNewLargeProcesses[lockIdentifier.lower()] -= 1
        if Main.lockNewLargeProcesses[lockIdentifier.lower()] < 0:
            Main.lockNewLargeProcesses[lockIdentifier.lower()] = 0


def startNewLargeProcessesCheck(lockIdentifiers, checkLimit=240, sleepInterval=0.25,lockLimit=1):
    if isinstance(lockIdentifiers,str):
        lockIdentifiers = [lockIdentifiers]
    if isinstance(lockIdentifiers,list) is False:
        raise Exception("lockIdentifiers must be a list or string object.")
    counter = 0
    lockIdentifiers = [lockIdentifier.lower() for lockIdentifier in lockIdentifiers]
    for lockIdentifier in lockIdentifiers:
        if lockIdentifier not in Main.lockNewLargeProcesses:
            Main.lockNewLargeProcesses[lockIdentifier] = 0
        while Main.lockNewLargeProcesses[lockIdentifier] >= lockLimit:
            time.sleep(sleepInterval)
            counter += 1
            if counter > checkLimit:
                raise Exception(f"New processes for |{lockIdentifier}| are currently locked. Please try again later.")

# def lockNewLargeProcesses(lockIdentifier):
#     Main.lockNewLargeProcesses[lockIdentifier.lower()] = True
#
#
# def releaseNewLargeProcesses(lockIdentifier):
#     Main.lockNewLargeProcesses[lockIdentifier.lower()] = False
#
#
# def startNewLargeProcessesCheck(lockIdentifier, checkLimit=240, sleepInterval=0.25):
#     counter = 0
#     lockIdentifier = lockIdentifier.lower()
#     if lockIdentifier not in Main.lockNewLargeProcesses:
#         Main.lockNewLargeProcesses[lockIdentifier] = False
#     while Main.lockNewLargeProcesses[lockIdentifier] == True:
#         time.sleep(sleepInterval)
#         counter += 1
#         if counter > checkLimit:
#             raise Exception(f"New processes for |{lockIdentifier}| are currently locked. Please try again later.")


def resetLoadingStatus(identifier):
    Main.updateStatus[identifier] = {'progress_data': {}, 'custom_data': {}}

def cancelLoadingStatus(identifier):
    if identifier not in Main.updateStatus:
        Main.updateStatus[identifier] = {'progress_data': {}, 'custom_data': {}}
    Main.updateStatus[identifier]['custom_data']['cancel_operation'] = True

def checkLoadingOperationCancelled(identifier,**kwargs):
    if identifier not in Main.updateStatus:
        Main.updateStatus[identifier] = {'progress_data': {}, 'custom_data': {}}
    if 'cancel_operation' in Main.updateStatus[identifier]['custom_data']:
        if Main.updateStatus[identifier]['custom_data']['cancel_operation'] is True:
            return True
        else:
            return False
    else:
        return False

def updateLoadingStatus(identifier, progress_bar=None, **kwargs):
    if identifier not in Main.updateStatus:
        Main.updateStatus[identifier] = {'progress_data': {}, 'custom_data': {}}
    updateStatus = Main.updateStatus[identifier]

    if progress_bar:
        updateDict = {}
        if progress_bar.format_dict['rate'] is not None:
            updateDict['rate'] = progress_bar.format_dict["rate"]
            remaining_seconds = (progress_bar.format_dict["total"] - progress_bar.format_dict["n"]) // \
                                progress_bar.format_dict["rate"]
            updateDict['remaining_time'] = "{:02d}:{:02d}:{:02d}".format(int(remaining_seconds // 3600),
                                                                         int(remaining_seconds % 3600) // 60,
                                                                         int(remaining_seconds % 60))

        else:
            pass
        if progress_bar.format_dict['total'] is not None and progress_bar.n > 0:
            updateDict['percentage_complete'] = round(progress_bar.n / progress_bar.format_dict['total'], 4)
        else:
            updateDict['percentage_complete'] = 0

        updateDict.update({'n': progress_bar.n,
            'elapsed': "{:02d}:{:02d}:{:02d}".format(
            int(int(progress_bar.format_dict['elapsed']) // 3600),
            int(int(progress_bar.format_dict['elapsed']) % 3600 // 60),
            int(int(progress_bar.format_dict['elapsed']) % 60)),
            'total':progress_bar.format_dict['total']})
        # {'n': progress_bar.n,
        #  'remaining_time': "{:02d}:{:02d}:{:02d}".format(int(remaining_seconds // 3600),
        #                                                  int(remaining_seconds % 3600) // 60,
        #                                                  int(remaining_seconds % 60)),
        #  'rate': progress_bar.format_dict['rate'],
        #  'elapsed': progress_bar.format_dict["elapsed"]
        #  }

        updateStatus['progress_data'].update(updateDict)
    if kwargs:
        updateStatus['custom_data'].update(kwargs)


def update_mismatched_imfi_img(full_file_path):
    img = imagesorter_db.IMAGES.query.filter_by(path=full_file_path).first()
    imfi_img = imagesorter_db.IMAGE_FILES.query.filter_by(file_name=img.file_name).first()
    if imfi_img:
        allowCommit = False
        if imfi_img.path != img.path:
            imfi_img.path = img.path
            allowCommit = True
        if imfi_img.imdir_auto_key != img.imdir_auto_key:
            imfi_img.imdir_auto_key = img.imdir_auto_key
            allowCommit = True
        if allowCommit:
            db.session.commit()


def _remove_record_from_database(record_query_object, dict_object, dictSection: str, save_info=True):
    if save_info:
        dict_info = [x.__dict__ for x in record_query_object.all()]
    record_query_object.delete()
    if save_info:
        for xy, row in enumerate(dict_info):
            if '_sa_instance_state' in row:
                del dict_info[xy]['_sa_instance_state']
            for key in row:
                dict_info[xy][key] = str(row[key])

        dict_object[dictSection] = str(dict_info)


@process_method('remove_file')
def remove_image_by_imfi_ak(imfi_auto_key):
    """
    :param imfi_auto_key:
    :return:
    """

    IMFI = imagesorter_db.IMAGE_FILES.query.filter_by(imfi_auto_key=imfi_auto_key).first()

    MULTI_IMAGE_CHECK = imagesorter_db.IMAGES.query.filter_by(file_name=IMFI.file_name).all()
    if len(MULTI_IMAGE_CHECK) > 1:
        raise Exception(f'Please remove all duplicate filenames before removing this file: {IMFI.file_name}')

    local_image_directory_db(imagesorter_db.IMAGE_DIRS.query.filter_by(imdir_auto_key=IMFI.imdir_auto_key).first()
                             ).remove_file_from_db(IMFI.file_name, IMFI.path)

    filename = IMFI.file_name

    for tag in imagesorter_db.TAGGED_IMAGES.query.filter_by(imfi_auto_key=IMFI.imfi_auto_key).all():
        imagesorter_db.tag_directory_crossover_update(tag_auto_key=tag.tag_auto_key, imdir_auto_key=IMFI.imdir_auto_key,
                                                      increment=-1)

    dict_info = {}
    # _remove_record_from_database(imagesorter_db.SIMILAR_IMAGES.query.filter_by(imfi_auto_key=imfi_auto_key), dict_info,
    #               'SIMILAR_IMAGES')
    # _remove_record_from_database(
    #     imagesorter_db.SIMILAR_IMAGE_PROCESSED.query.filter_by(secondary_imfi_auto_key=imfi_auto_key),
    #     dict_info, 'SIMILAR_IMAGE_PROCESSED_SECONDARY')
    # _remove_record_from_database(
    #     imagesorter_db.SIMILAR_IMAGE_PROCESSED.query.filter_by(primary_imfi_auto_key=imfi_auto_key),
    #     dict_info, 'SIMILAR_IMAGE_PROCESSED_PRIMARY')
    _remove_record_from_database(imagesorter_db.IMAGE_NOTES.query.filter_by(imfi_auto_key=imfi_auto_key), dict_info,
                                 'IMAGE_NOTES')
    _remove_record_from_database(imagesorter_db.SEQUENCE.query.filter_by(imfi_auto_key=imfi_auto_key), dict_info,
                                 'SEQUENCE')
    _remove_record_from_database(imagesorter_db.TAGGED_IMAGES.query.filter_by(imfi_auto_key=imfi_auto_key), dict_info,
                                 'TAGGED_IMAGES')
    _remove_record_from_database(imagesorter_db.IMAGE_FILES.query.filter_by(imfi_auto_key=imfi_auto_key), dict_info,
                                 'IMAGE_FILES')
    _remove_record_from_database(imagesorter_db.TAGGED_IMAGES_PRECALC.query.filter_by(imfi_auto_key=imfi_auto_key),
                                 dict_info,
                                 'TAGGED_IMAGES_PRECALC')
    _remove_record_from_database(imagesorter_db.IMAGE_FILE_NOTES.query.filter_by(imfi_auto_key=imfi_auto_key),
                                 dict_info, 'IMAGE_FILE_NOTES')

    # Delete ALL thumbnails
    remove_thumbnails(IMFI, Main.thumbnail_sizes)
    IMAGE = imagesorter_db.IMAGES.query.filter_by(file_name=filename).first()
    dict_info['IMAGE'] = str(IMAGE.__dict__)
    # del dict_info['SIMILAR_IMAGES']

    if IMAGE:
        # Files are moved to the pending_removal folder
        try:
            with open(f"{Main.server_settings['folder']['pending_removal']}/{IMAGE.file_name}.json", 'w') as JSON_FILE:
                json.dump(dict_info, JSON_FILE)
            try:
                shutil.move(IMAGE.path, Main.server_settings['folder']['pending_removal'])
            except FileNotFoundError:
                pass

        except Exception as e:
            raise e

        imagesorter_db.IMAGES.query.filter_by(file_name=filename).delete()
        # Move to the static folder to be deleted manually? Just in case you didn't mean to delete it?
        pass

@process_method('available_folders')
def get_available_folders():

    available_folders = convert_all_to_dict(db.session.execute(sql_text(
        f"SELECT IMDIR_AUTO_KEY,PATH,MAIN_CATEGORY,SUB_CATEGORY FROM {Schema}.IMAGE_DIRS order by MAIN_CATEGORY asc,SUB_CATEGORY asc")))
    final_dict = {}  # Sort them by MAIN_CAT/SUB_CAT
    for folder in available_folders:
        if folder['main_category'] not in final_dict:
            final_dict[folder['main_category']] = {}
        if folder['sub_category'] not in final_dict[folder['main_category']]:
            final_dict[folder['main_category']][folder['sub_category']] = []
        final_dict[folder['main_category']][folder['sub_category']].append(
            {'imdir_auto_key': folder['imdir_auto_key'], 'path': folder['path']})

    return final_dict



@process_method('refresh_folders')
def refresh_folder_sql_db(dir_ak, path, bypassFileCheck=False):
    """
    Creates a sqlite db if needed
    :param dir_ak:
    :return:
    """
    local_imdir_db = local_image_directory_db(
        imagesorter_db.IMAGE_DIRS.query.filter_by(imdir_auto_key=dir_ak, path=path).first())

    if bypassFileCheck:
        local_imdir_db.bypassFileCheck = True

    return local_imdir_db.load_files()
    pass

@process_method('clear_folders')
def clear_folder(folder, imdir_auto_key):
    dir_record = imagesorter_db.IMAGE_DIRS.query.filter_by(path=folder).first()
    if dir_record:
        local_imdir = local_image_directory_db(dir_record)
        local_imdir.remove_all_files_from_db(autocommit=False)

        imagesorter_db.IMAGES.query.filter_by(imdir_auto_key=imdir_auto_key).delete()
        imagesorter_db.IMAGE_FILES.query.filter_by(imdir_auto_key=imdir_auto_key).update({
            imagesorter_db.IMAGE_FILES.available_file: False},
            synchronize_session=False)
        db.session.commit()
        local_imdir.conn.commit()
        return True
    return False

@process_method('update_folders')
def update_folder(folder_dict):
    if 'ORIGINAL_PATH' in folder_dict:
        pathRecord = imagesorter_db.IMAGE_DIRS.query.filter_by(path=folder_dict['ORIGINAL_PATH'],
                                                               imdir_auto_key=folder_dict['IMDIR_AUTO_KEY']).first()

        ##For now I'll do it only on empty folders. Can't change the path unless you empty out the folder server-side.
        # ^ If you want to relocate files, use the move_files
        countFiles = db.session.execute(
            sql_text(f"SELECT COUNT(imdir_auto_key) FROM {Schema}.IMAGE_FILES WHERE IMDIR_AUTO_KEY = :imdir_auto_key"),
            {'imdir_auto_key': pathRecord.imdir_auto_key}).fetchone()

        updated_folder = False
        if 'NEW_PATH' in folder_dict:

            if countFiles[0] > 0:
                # Unable to update the path if there are files in there already. Need to clear it out first in the DB.
                raise Exception(
                    "Cannot update folder path with files loaded from the folder. Please use the 'clear folder' function before relocating")
            # This is used in the case of moving the folder to a new location (in its entirety). All of the images should still be in there.
            if folder_dict['NEW_PATH'] != folder_dict['ORIGINAL_PATH']:
                if not os.path.isdir(folder_dict['NEW_PATH']):
                    return [False, "New path is not a valid path."]

                pathRecord.path = folder_dict['NEW_PATH']

                # Update all images to the new path
                IMAGES = imagesorter_db.IMAGES.query.filter_by(source_folder=folder_dict['ORIGINAL_PATH']).all()
                for img in IMAGES:
                    img.path = f"{folder_dict['NEW_PATH']}/{img.file_name}"
                    img.SOURCE_FOLDER = folder_dict['NEW_PATH']

                updated_folder = True
        if 'NEW_SUB_CATEGORY' in folder_dict:

            if folder_dict['NEW_SUB_CATEGORY'] != pathRecord.sub_category:
                pathRecord.sub_category = folder_dict['NEW_SUB_CATEGORY'].upper()

                updated_folder = True
        if 'NEW_MAIN_CATEGORY' in folder_dict:
            if folder_dict['NEW_MAIN_CATEGORY'] != pathRecord.main_category:
                pathRecord.main_category = folder_dict['NEW_MAIN_CATEGORY'].upper()

                updated_folder = True
        if updated_folder:
            db.session.commit()
        return [True, '']
    else:
        pass
    return [False, "Path not found"]

@process_method('load_image_notes')
def load_image_notes(imfi_auto_key):
    return convert_all_to_dict(db.session.execute(sql_text(
        f"""SELECT * FROM {Schema}.IMAGE_FILE_NOTES IMFINO WHERE IMFI_AUTO_KEY = :imfi_auto_key order by y_coord asc,x_coord asc"""),
        {'imfi_auto_key': imfi_auto_key}))

@process_method('load_sequence')
def load_sequence(filename=None, sequence=None):
    if filename:

        SEQUENCE = convert_all_to_dict(db.session.execute(sql_text(
            f"""select string_agg(IMFI2.FILE_NAME,'|*|' order by SEQ2.SEQUENCE_NUMBER) "FILE_NAMES",SEQ.SEQUENCE_NAME,SEQ.SEQUENCE_NUMBER "INDEX",string_agg(SEQ2.SEQUENCE_NUMBER::character varying,'|*|' order by SEQ2.SEQUENCE_NUMBER) "SEQUENCE_NUMBERS",
                                        string_agg(IMFI2.IMFI_AUTO_KEY::character varying,'|*|' order by SEQ2.SEQUENCE_NUMBER) "IMFI_AUTO_KEYS"
                    from {Schema}.SEQUENCE SEQ
                    inner join {Schema}.SEQUENCE SEQ2 on SEQ.SEQUENCE_NAME = SEQ2.SEQUENCE_NAME
                    inner join {Schema}.IMAGE_FILES IMFI on IMFI.IMFI_AUTO_KEY = SEQ.IMFI_AUTO_KEY
                    inner join {Schema}.IMAGE_FILES IMFI2 on IMFI2.IMFI_AUTO_KEY = SEQ2.IMFI_AUTO_KEY
                    where IMFI.FILE_NAME like :FILE_NAME
                    and IMFI.AVAILABLE_FILE = True
                    group by SEQ.SEQUENCE_NAME,SEQ.SEQUENCE_NUMBER,SEQ2.SEQUENCE_NAME--,SEQ2.SEQUENCE_NUMBER   
                    order by SEQ2.SEQUENCE_NAME asc--, SEQ2.SEQUENCE_NUMBER asc"""),
            {'FILE_NAME': filename}))
        pass
        if SEQUENCE:
            if SEQUENCE[0]['INDEX']:
                # print(348957439875345, SEQUENCE)
                return {
                    'INDEX': SEQUENCE[0]['INDEX'],
                    'FILE_NAMES': SEQUENCE[0]['FILE_NAMES'].split('|*|'),
                    'SEQUENCE_NUMBERS': SEQUENCE[0]['SEQUENCE_NUMBERS'].split('|*|'),
                    'IMFI_AUTO_KEYS': SEQUENCE[0]['IMFI_AUTO_KEYS'].split('|*|'),
                    'SEQUENCE_NAME': SEQUENCE[0]['sequence_name']
                }
        else:
            return {}
    elif sequence:

        SEQUENCE = convert_all_to_dict(db.session.execute(sql_text(
            f"""select IMFI.FILE_NAME,SEQUENCE_NUMBER from {Schema}.SEQUENCE SEQ inner join {Schema}.IMAGE_FILES IMFI on IMFI.IMFI_AUTO_KEY = SEQ.IMFI_AUTO_KEY where SEQUENCE_NAME = :SEQUENCE order by SEQUENCE_NAME asc, SEQUENCE_NUMBER asc"""),
            {'SEQUENCE': f'{sequence}'}))
        pass
        finalSEQUENCE = [[x['file_name'], x['sequence_number']] for x in SEQUENCE]
        return finalSEQUENCE

@process_method('update_or_create_sequence')
def update_or_create_sequence(**kwargs):
    if sequence_files_imfi := kwargs.get('SEQUENCE_FILES_IMFI', None):
        if not kwargs.get('SEQUENCE_NAME', None):
            raise Exception("SEQUENCE_NAME is required.")
        if not kwargs.get('STARTING_INDEX', None):
            raise Exception("STARTING_INDEX is required.")

        sequence_name = kwargs.get('SEQUENCE_NAME')
        starting_index = kwargs.get('STARTING_INDEX')
        try:
            starting_index = int(starting_index)
            if starting_index <= 0:
                raise
        except:
            raise Exception("STARTING_INDEX must be > 0")

        for imfi_index, imfi_auto_key in enumerate(sequence_files_imfi.split('|')):
            # Check if the file already has a SEQUENCE
            existing_sequence = imagesorter_db.SEQUENCE.query.filter_by(imfi_auto_key=imfi_auto_key).first()
            # if exists, update to new parameters
            if existing_sequence:
                existing_sequence.sequence_name = sequence_name
                existing_sequence.sequence_number = starting_index + imfi_index
                db.session.commit()
            else:
                db.session.add(imagesorter_db.SEQUENCE(imfi_auto_key=imfi_auto_key, sequence_name=sequence_name.upper(),
                                                       sequence_number=starting_index + imfi_index))
                db.session.commit()
        return True
        pass
    else:
        raise Exception("SEQUENCE_FILES_IMFI necessary to continue")
    return False

@process_method('delete_sequence_by_name')
def delete_sequence_by_name(**kwargs):
    if (SEQUENCE_NAME:= kwargs.get('SEQUENCE_NAME',None)):
        sequenced_items = imagesorter_db.SEQUENCE.query.filter(imagesorter_db.SEQUENCE.sequence_name.ilike(SEQUENCE_NAME)).all()
        if len(sequenced_items) > 0:
            #delete the sequence.
            for sequenced_item in sequenced_items:
                imagesorter_db.SEQUENCE.query.filter_by(se_auto_key=sequenced_item.se_auto_key).delete()
            db.session.commit()
            return True
        return False
    else:
        raise Exception("SEQUENCE_NAME is required.")

@process_method('delete_sequence_by_imfi_ak')
def delete_sequence_by_imfi_ak(**kwargs):
    #Get list of IMFI
    IMFI_AK = kwargs.get('IMFI_AK',[])
    if isinstance(IMFI_AK,str):
        IMFI_AK = IMFI_AK.split(',')
    if isinstance(IMFI_AK,list) is False:
        raise Exception("IMFI_AK must be a list or a string.")
    deleted_keys = 0
    for key in IMFI_AK:
        if imagesorter_db.SEQUENCE.query.filter_by(imfi_auto_key=key).count() > 0:
            imagesorter_db.SEQUENCE.query.filter_by(imfi_auto_key=key).delete()
            db.session.commit()
            deleted_keys+=1
    #return {'Successful':True,'Deleted Linked Sequences':deleted_keys}
    if deleted_keys > 0:
        return True
    else:
        return False

def get_username():
    try:
        if current_user.is_authenticated:
            username = current_user.username
        else:
            username = 'Guest'
    except Exception as e:
        if str(e) == "'Flask' object has no attribute 'login_manager'":
            username = 'Guest'
        else:
            raise e
    return username


def get_user_config_filename(username=None):
    # if authenticated, load user file, else load "general_custom_search_data.json" for all users accessing the site.
    if username is None:
        username = get_username()
    try:
        if current_user.is_authenticated:
            filename = f"user_{username}_config_data.json"
        else:
            filename = f"general_{username}_config_data.json"
    except Exception as e:
        if str(e) == "'Flask' object has no attribute 'login_manager'":
            filename = f"general_{username}_config_data.json"
        else:
            raise e
    return f"{Main.server_settings['folder']['user_info']}/{filename}"

def load_user_config(key=None,username=None):
    filename = get_user_config_filename(username)
    if os.path.isfile(filename):
        with open(filename, 'r') as FILE:
            JSON_DATA = json.load(FILE)
            if key:
                if key in JSON_DATA:
                    return JSON_DATA[key]
                else:
                    return {}
            return JSON_DATA
    else:
        with open(filename,'w') as FILE:
            json.dump({},FILE)
            pass
        return {}


def get_user_custom_layouts(username=None):
    user_custom_layouts = load_user_config('custom_ui_layout',username)

    if len(user_custom_layouts.keys()) == 0:
        user_custom_layouts = {'layout':
            {"Default": {
                "uiColumns": "'selection selection tags tagadder' 'selection selection imagescroller imagescroller'",
                "gridAreas": {
                    "Selected_Images": [[1, 1], [2, 1], [1, 2], [2, 2]],
                    "Linked_Tags": [[3, 1]],
                    "Tag_Search": [[4, 1]],
                    "Scrollable_Images": [[3, 2], [4, 2]]
                },
                "rows": "1fr 1fr",
                "columns": "1fr 1fr 1fr 1fr",
                "rows_raw": [1, 1],
                "columns_raw": [1, 1, 1, 1],
                "totalRows": 2,
                "totalColumns": 4
            }
            }
        }
    return user_custom_layouts


def update_user_config(category,update_dict,username=None):
    filename = get_user_config_filename(username)
    user_config = load_user_config()
    if 'csrf_token' in update_dict:
        del update_dict['csrf_token']
    if category in user_config:
        user_config[category].update(update_dict)
    else:
        user_config[category] = update_dict
    with open(filename, 'w') as FILE:
        json.dump(user_config, FILE)
        return True
    return False


def rename_file(imfi_auto_key,old_filename,new_filename):
    #Ensure there is only one image (no dupes) before renaming.
    if check_debug():
        print('IMFI:',imfi_auto_key)
        print('Old Filename:',old_filename)
        print("New Filename:",new_filename)
    imfi = imagesorter_db.IMAGE_FILES.query.filter_by(imfi_auto_key=imfi_auto_key,file_name=old_filename).first()
    if imfi:
        if (imgCount:=imagesorter_db.IMAGES.query.filter_by(file_name=old_filename).count()) > 1:
            raise Exception("Please remove all duplicate images before renaming.")
        elif imgCount == 0:
            raise Exception("Image not found in system.")
        else:
            if imagesorter_db.IMAGES.query.filter_by(file_name=new_filename).count() > 0:
                raise Exception("New filename already exists.")
            try:

                img = imagesorter_db.IMAGES.query.filter_by(file_name=old_filename).first()
                new_filename = new_filename.rstrip(f".{img.file_extension}")+f'.{img.file_extension}'
                old_filepath = img.path
                new_filepath = img.path.rstrip(old_filename) + new_filename
                shutil.move(old_filepath, new_filepath)
                img.file_name = new_filename
                imfi.file_name = new_filename
                img.path = new_filepath
                imfi.path = new_filepath
                db.session.commit()
                return new_filename
            except Exception as e:
                db.session.rollback()
                raise e

    else:
        raise Exception("IMFI Not Found")


def edit_tags(process_type,**kwargs):
    allowCommit = False
    existingTag = imagesorter_db.TAGS.query.filter_by(tag_auto_key=kwargs.get('TAG_AUTO_KEY', None)).first()
    update_tag_imdir_combo = []
    if not existingTag:
        return errorhandler.make_error(400, 'Tag does not exist')

    allowPbarUpdates = False

    tag_auto_keys_split = kwargs.get('IMFI_AUTO_KEYS', '').split('||')
    if len(tag_auto_keys_split) > 10:
        resetLoadingStatus('loading_imtags')
        updateLoadingStatus('loading_imtags', title='Updating Imtags')
        allowPbarUpdates = True
        pbar = tqdm(tag_auto_keys_split)
        pbar.set_description(f'{process_type} - Updating imtags')
    else:
        pbar = tag_auto_keys_split

    bulk_insert = []
    bulk_remove = []
    for imfi_ak in pbar:
        if allowPbarUpdates:
            updateLoadingStatus('loading_imtags', pbar)
            if checkLoadingOperationCancelled('loading_imtags') is True:
                break
        existingImageTagged = imagesorter_db.TAGGED_IMAGES.query.filter_by(imfi_auto_key=imfi_ak,
                                                                           tag_auto_key=request.form[
                                                                               'TAG_AUTO_KEY']).first()

        # filecheck
        imageFile = imagesorter_db.IMAGE_FILES.query.filter_by(imfi_auto_key=imfi_ak).first()
        if not imageFile:
            return errorhandler.make_error(400, f"IMAGE FILE NOT FOUND FOR IMFI_AUTO_KEY:{imfi_ak}")
        imfi_ak = int(imfi_ak)
        if process_type == 'ADD':
            if 'ADD_TAG' == request.form['TYPE']:
                # print('Add:',request.form)
                if not existingImageTagged:
                    if existingTag.category == 'GENERAL' and existingTag.detail == 'PRIVATE':
                        Image = imagesorter_db.IMAGE_FILES.query.filter_by(imfi_auto_key=imfi_ak).first()
                        Image.private = True
                    newrecord = imagesorter_db.TAGGED_IMAGES(imfi_auto_key=imfi_ak,
                                                             tag_auto_key=existingTag.tag_auto_key,
                                                             timestamp=dt.datetime.now())
                    bulk_insert.append(newrecord)

                    update_tag_imdir_combo.append((existingTag.tag_auto_key, imageFile.imdir_auto_key))

                    if len(bulk_insert) >= 100:
                        db.session.bulk_save_objects(bulk_insert)
                        bulk_insert = []
                        allowCommit = True


        elif process_type == 'REMOVE':

            if existingTag.category == 'GENERAL' and existingTag.detail == 'PRIVATE':
                Image = imagesorter_db.IMAGE_FILES.query.filter_by(imfi_auto_key=imfi_ak).first()
                Image.private = False
            if existingImageTagged:
                bulk_remove.append(existingImageTagged)
            if len(bulk_remove) >= 100:
                for record in bulk_remove:
                    db.session.delete(record)
                bulk_remove = []
                allowCommit = True
            update_tag_imdir_combo.append((existingTag.tag_auto_key, imageFile.imdir_auto_key))
            db.session.commit()

    if len(bulk_insert) > 0:
        db.session.bulk_save_objects(bulk_insert)
        allowCommit = True
    if len(bulk_remove) > 0:
        for record in bulk_remove:
            db.session.delete(record)
        allowCommit = True

    if allowCommit:
        if process_type == 'ADD':
            existingTag.last_used = dt.datetime.now()

        db.session.commit()
        existingTagResult = db.session.execute(
            sql_text(
                f"""select tag_auto_key,category,detail from {Schema}.tags tag where tag_auto_key = :tag_auto_key"""),
            {'tag_auto_key': existingTag.tag_auto_key}).first()

        return [
                {'tag_auto_key': existingTagResult[0], 'category': existingTagResult[1], 'detail': existingTagResult[2]}
                ]
        # return jsonify(True)
    else:
        db.session.rollback()
        return False


def load_image_file(filename,**kwargs):
    file = imagesorter_db.IMAGES.query.filter_by(file_name=filename).all()
    if file:
        index = kwargs.get('index', 0)
        index = int(index)
        if len(file) < (index + 1):
            index = len(file) - 1

        file = file[index]

        if 'file_data' in kwargs:
            return jsonify(get_image_file_data(file))

        if 'frame' in kwargs:
            try:
                frame = int(kwargs.get('frame'))
            except Exception as e:
                return errorhandler.make_error(400, "Invalid frame number. Not an integer.")
            IMG = Image.open(file.path)

            if frame < 1:
                frame = 1
            try:
                frame = frame % IMG.n_frames
            except:
                frame = 0

            IMG.seek(frame)
            img_io = BytesIO()
            IMG.save(img_io, 'PNG')
            img_io.seek(0)
            return send_file(img_io, 'image/png')

        if 'size_data' in kwargs:
            IMG = Image.open(file.path)
            return jsonify(IMG.size)
        if 'size' in kwargs:
            if os.path.isfile(file.path) is False:
                return errorhandler.make_error(400,"File not found")
            if '.gif' in file.path.lower()[-4::] and 'play' in kwargs:
                return send_file(file.path)

            elif kwargs['size'] == 'small':
                thumb = get_thumbnail(file, (Main.thumbnail_small, Main.thumbnail_small))
                if 'PATH' in thumb:
                    return send_file(thumb['PATH'])
                return send_file(BytesIO(thumb['BYTES']), mimetype=thumb['MIMETYPE'])
            elif kwargs['size'] == 'medium':
                thumb = get_thumbnail(file, (Main.thumbnail_medium, Main.thumbnail_medium))
                if 'PATH' in thumb:
                    return send_file(thumb['PATH'])
                return send_file(BytesIO(thumb['BYTES']), mimetype=thumb['MIMETYPE'])
            elif kwargs['size'] == 'large':
                thumb = get_thumbnail(file, (Main.thumbnail_large, Main.thumbnail_large))
                if 'PATH' in thumb:
                    return send_file(thumb['PATH'])
                return send_file(BytesIO(thumb['BYTES']), mimetype=thumb['MIMETYPE'])

            elif kwargs['size'] == 'custom':
                # This will not create a thumbnail for the item.
                thumb = get_thumbnail(file,
                                              (int(kwargs['custom_size']), int(kwargs['custom_size'])),
                                              override_thumbnail=bool(
                                                  kwargs.get('override_thumbnail', False)))
                if 'PATH' in thumb:
                    return send_file(thumb['PATH'])
                return send_file(BytesIO(thumb['BYTES']), mimetype=thumb['MIMETYPE'])
            elif kwargs['size'] == 'full':
                return send_file(file.path, mimetype=file.mimetype)
            elif file.path.split('.')[-1].upper() == 'WEBP':
                return send_file(file.path, mimetype='image/webp', as_attachment=False)
            else:
                return send_file(file.path, mimetype=file.mimetype)

def get_image_file_data(file_record):
    """
    Return details related to the file
    :param file_record:
    :return:
    """
    return {'FILE_NAME': file_record.file_name,
            'ORIGINAL_DATE': dt.datetime.fromtimestamp(os.path.getmtime(file_record.path)).strftime(
                '%Y-%m-%d %H:%M:%S'),
            'SEQUENCE': load_sequence(filename=file_record.file_name),
            'NOTES': load_image_notes(imfi_auto_key=get_imfi_auto_key(file_record.file_name))}



def update_image_notes(note_json_list):
    try:
        for note in note_json_list:
            if note.get('pending_remove', False) == True:
                imagesorter_db.IMAGE_FILE_NOTES.query.filter_by(imno_auto_key=note.get('imno_auto_key'),
                                                                imfi_auto_key=note.get(
                                                                    'imfi_auto_key')).delete()
                db.session.commit()
            elif note.get('pending_update', False) == True:
                if note.get('imno_auto_key', None):
                    IMFINO = imagesorter_db.IMAGE_FILE_NOTES.query.filter_by(
                        imno_auto_key=note.get('imno_auto_key'), imfi_auto_key=note.get('imfi_auto_key')).first()
                    if IMFINO:
                        IMFINO.x_coord = note.get('x_coord', 0)
                        IMFINO.y_coord = note.get('y_coord', 0)
                        IMFINO.note_width = note.get('note_width', 0)
                        IMFINO.note_height = note.get('note_height', 0)
                        IMFINO.note_body = note.get('note_body', '')
                        IMFINO.comments = note.get('comments', '')
                        IMFINO.rotation = note.get('rotation', 0)
                        db.session.commit()

                else:
                    if note.get('imfi_auto_key', None):
                        IMFINO = imagesorter_db.IMAGE_FILE_NOTES(x_coord=note.get('x_coord', 0),
                                                                 y_coord=note.get('y_coord', 0),
                                                                 note_width=note.get('note_width', 0),
                                                                 note_height=note.get('note_height', 0),
                                                                 note_body=note.get('note_body', ''),
                                                                 comments=note.get('comments', ''),
                                                                 rotation=note.get('rotation', 0),
                                                                 imfi_auto_key=note.get('imfi_auto_key'),
                                                                 timestamp=dt.datetime.now()
                                                                 )
                        db.session.add(IMFINO)
                        db.session.commit()
    except Exception as e:
        raise e


def update_tag_notes(**kwargs):
    original_tag = kwargs.get('ORIGINAL_TAG', None)
    tag_auto_key = kwargs.get('TAG_AUTO_KEY', None)
    new_note = kwargs.get('NEW_NOTE', None)
    if not original_tag or new_note is None or not tag_auto_key:
        return errorhandler.make_error(400, "ORIGINAL_TAG, TAG_AUTO_KEY, AND NEW_NOTE MUST BE SUPPLIED.")
    original_tag = original_tag.upper()
    original_tag_split = original_tag.split(':')
    originalTag = imagesorter_db.TAGS.query.filter_by(category=original_tag_split[0],
                                                      detail=original_tag_split[1],
                                                      tag_auto_key=tag_auto_key).first()
    if originalTag is None:
        return errorhandler.make_error(400, f"Tag not found: {original_tag}")

    originalTag.notes = new_note
    db.session.commit()
    return True

def update_tag_data(**kwargs):
    allowCommit = False
    original_tag = kwargs.get('ORIGINAL_TAG', None)
    new_tag = kwargs.get('NEW_TAG', None)
    tag_auto_key = kwargs.get('TAG_AUTO_KEY', None)
    if not original_tag or not new_tag or not tag_auto_key:
        return errorhandler.make_error(400, "ORIGINAL_TAG, NEW_TAG, AND TAG_AUTO_KEY MUST BE SUPPLIED.")

    original_tag = original_tag.upper()
    new_tag = new_tag.upper()

    if original_tag == new_tag:
        return errorhandler.make_error(400, "ORIGINAL_TAG AND NEW_TAG MUST BE DIFFERENT")
    original_tag_split = original_tag.split(':')

    new_tag_split = new_tag.split(':')
    if not new_tag_split[0] or not new_tag_split[1]:
        return errorhandler.make_error(400, "NEW TAG MUST SUPPLY BOTH A CATEGORY AND DETAIL")

    originalTag = imagesorter_db.TAGS.query.filter_by(category=original_tag_split[0],
                                                      detail=original_tag_split[1],
                                                      tag_auto_key=tag_auto_key).first()
    newTagCheck = imagesorter_db.TAGS.query.filter_by(category=new_tag_split[0],
                                                      detail=new_tag_split[
                                                          1]).first()  # Check if the new tag already exists
    # tag_precalc_list = []  # Used for lists of tags to be updated

    if f"{originalTag.category}:{originalTag.detail}" in Main.default_tags:
        return errorhandler.make_error(400, f"Unable to modify default tags: {Main.default_tags}")

    if newTagCheck:
        ##COMBINE originalTag items into this one.

        # update all tagged items to have newTagCheck ID
        taggedItems_Updating = imagesorter_db.TAGGED_IMAGES.query.filter_by(
            tag_auto_key=originalTag.tag_auto_key).all()
        for tag in taggedItems_Updating:
            # Check if new key exists already, to prevent dupes
            if imagesorter_db.TAGGED_IMAGES.query.filter_by(imfi_auto_key=tag.imfi_auto_key,
                                                            tag_auto_key=newTagCheck.tag_auto_key).first():
                # delete the tag if it already exists
                imagesorter_db.TAGGED_IMAGES.query.filter_by(imtag_auto_key=tag.imtag_auto_key).delete()
            else:
                # if it's not a dupe
                tag.tag_auto_key = newTagCheck.tag_auto_key
        originalTag.notes = originalTag.notes + '\n' + newTagCheck.notes

        ##Delete original tag
        imagesorter_db.TAGS.query.filter_by(category=original_tag_split[0], detail=original_tag_split[1]).delete()


        db.session.commit()
        pass
    else:
        # update existing tag
        originalTag.category = new_tag_split[0]
        originalTag.detail = new_tag_split[1]

        db.session.commit()
        pass

    if allowCommit:
        db.session.commit()

    return True

def remove_tag(**kwargs):
    if kwargs.get('TYPE', None) in ['CLEAR_TAG_FROM_IMAGES', 'REMOVE_TAG']:
        if (tag_auto_key := kwargs.get("TAG_AUTO_KEY", None)) is None:
            return errorhandler.make_error(400, "TAG_AUTO_KEY is required.")
        if (tag_category := kwargs.get('CATEGORY', None)) is None:
            return errorhandler.make_error(400, "TAG_AUTO_KEY is required.")
        if (tag_detail := kwargs.get('DETAIL', None)) is None:
            return errorhandler.make_error(400, "TAG_AUTO_KEY is required.")
        if (confirm_delete := kwargs.get('CONFIRM_DELETE', None)) is None:
            return errorhandler.make_error(400, "CONFIRM_DELETE is required. (T/F)")
        if confirm_delete.upper() == 'T':
            confirm_delete = True
        else:
            confirm_delete = False
        TAG_RECORD = imagesorter_db.TAGS.query.filter_by(tag_auto_key=tag_auto_key, category=tag_category,
                                                         detail=tag_detail).first()

    if f"{TAG_RECORD.category}:{TAG_RECORD.detail}" in Main.default_tags:
        return errorhandler.make_error(400, f"Unable to modify default tags: {Main.default_tags}")
    allowCommit = False
    if kwargs.get('TYPE', None) == 'CLEAR_TAG_FROM_IMAGES':
        if TAG_RECORD and confirm_delete is True:
            # If it exists, then delete tags from images
            imagesorter_db.TAGGED_IMAGES.query.filter_by(tag_auto_key=TAG_RECORD.tag_auto_key).delete()
            allowCommit = True

        pass
    elif kwargs.get('TYPE', None) == 'REMOVE_TAG':
        # Only allow if there are no images attached. (unavailable images included)
        if TAG_RECORD and confirm_delete is True:
            # Check if there are any tagged images.
            if imagesorter_db.TAGGED_IMAGES.query.filter_by(tag_auto_key=TAG_RECORD.tag_auto_key).count() > 0:
                return errorhandler.make_error(400,
                                               'Tag Deletion Failed. Please ensure there are no images tagged with this tag (unavailable images included).')
            else:
                imagesorter_db.TAGS.query.filter_by(tag_auto_key=TAG_RECORD.tag_auto_key).delete()
                allowCommit = True

        pass

    if allowCommit:
        # Removes from the precalc crossover since they should be at 0 now.
        # imagesorter_db.TAG_DIRECTORY_CROSSOVER.query.filter_by(tag_auto_key=TAG_RECORD.tag_auto_key).delete()
        db.session.commit()
        return True
    return False

def load_tag_information(**kwargs):
    Dict = {}
    CATEGORY_WHERE = ''
    if 'CATEGORY' in kwargs:
        CATEGORY_WHERE = 'where TAG.CATEGORY = :CATEGORY'
        Dict['CATEGORY'] = kwargs.get('CATEGORY', None)

    TAGS = convert_all_to_dict(db.session.execute(sql_text(
        f"select TAG.*,count(TAIM.TAG_AUTO_KEY) \"COUNT\" from {Schema}.TAGS TAG inner join {Schema}.TAGGED_IMAGES TAIM on TAG.TAG_AUTO_KEY = TAIM.TAG_AUTO_KEY {CATEGORY_WHERE} group by TAG.TAG_AUTO_KEY"),
        Dict))
    sortedData = {}
    for tag in TAGS:
        if tag['category'] not in sortedData:
            sortedData[tag['category']] = {'NAME': tag['category'], 'COUNT': 0, 'CHILDREN': {}}
        currTag = sortedData[tag['category']]
        if tag['detail'] not in currTag['CHILDREN']:
            currTag['CHILDREN'][tag['detail']] = tag
            currTag['COUNT'] += tag['COUNT']
    return sortedData

def create_new_tag(tag_category,tag_detail):
    TAG = imagesorter_db.TAGS.query.filter_by(category=tag_category.upper(), detail=tag_detail.upper()).first()
    if not TAG:
        TAG = imagesorter_db.TAGS(category=tag_category.upper().strip(),
                                  detail=tag_detail.upper().strip(),
                                  timestamp=dt.datetime.now(),
                                  last_used=dt.datetime.now(),
                                  notes='')
        db.session.add(TAG)
        db.session.commit()
        return True
    else: return False

