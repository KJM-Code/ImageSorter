import datetime as dt
import json

import cv2
from PIL import Image
from flask_login import UserMixin
from sqlalchemy import JSON as sqlalchemy_JSON
from tqdm import tqdm
import os

from imports.database import db
from .base_settings import Schema
from . import methods

class IMAGES(UserMixin, db.Model):
    """
    Used for files currently in the system. As they're introduced, they're then linked to IMAGE_FILES (IMFI)
    """
    __bind_key__ = Schema
    __table_args__ = {"schema": Schema}
    img_auto_key = db.Column(db.Integer, primary_key=True)
    file_name = db.Column(db.String, unique=False, nullable=False)
    path = db.Column(db.String, unique=True, nullable=False)
    original_date = db.Column(db.DateTime, nullable=False)
    file_extension = db.Column(db.String, unique=False, nullable=False)
    source_folder = db.Column(db.String, unique=False, nullable=False)
    mimetype = db.Column(db.String, unique=False, nullable=False)
    type = db.Column(db.String, unique=False, nullable=True)
    category = db.Column(db.String, unique=False, nullable=False)
    available_file = db.Column(db.Boolean, nullable=False)
    has_thumbnail = db.Column(db.Boolean, nullable=False)  # Default 0/False
    imdir_auto_key = db.Column(db.Integer, nullable=False)


class IMAGE_FILES(UserMixin, db.Model):
    __bind_key__ = Schema
    __table_args__ = {"schema": Schema}
    imfi_auto_key = db.Column(db.Integer, primary_key=True)
    file_name = db.Column(db.String, unique=True, nullable=False)
    notes = db.Column(db.String, unique=False, nullable=False)
    available_file = db.Column(db.Boolean, nullable=False)
    date_added = db.Column(db.DateTime, nullable=False)
    original_date = db.Column(db.DateTime, nullable=False)
    category = db.Column(db.String, unique=False, nullable=False)
    file_count = db.Column(db.Integer, nullable=False)
    private = db.Column(db.Boolean, nullable=False)
    path = db.Column(db.String, unique=False, nullable=False)
    mimetype = db.Column(db.String, unique=False, nullable=False)
    frames = db.Column(db.Integer, nullable=True)  # Use this to determine if its animated (images)
    duration = db.Column(db.Float, nullable=True)  # Use for videos
    frame_information = db.Column(sqlalchemy_JSON, nullable=True)
    file_size = db.Column(db.BigInteger,nullable=True)
    dimension_width = db.Column(db.Integer,
                                nullable=True)  # Use dimensions to prevent loading really large images without approval from the user
    dimension_height = db.Column(db.Integer,
                                 nullable=True)
    imdir_auto_key = db.Column(db.Integer, nullable=False)
    last_tag_update = db.Column(db.DateTime, nullable=True)


class IMAGE_FILE_NOTES(UserMixin, db.Model):
    __bind_key__ = Schema
    __table_args__ = {'sqlite_autoincrement': True,
                      "schema": Schema}
    imno_auto_key = db.Column(db.Integer, primary_key=True)
    imfi_auto_key = db.Column(db.Integer, nullable=False)
    x_coord = db.Column(db.Integer, nullable=False, default=0)
    y_coord = db.Column(db.Integer, nullable=False, default=0)
    note_width = db.Column(db.Integer, nullable=False, default=0)
    note_height = db.Column(db.Integer, nullable=False, default=0)
    note_body = db.Column(db.String, nullable=False, default='')
    rotation = db.Column(db.Float, nullable=False, default=0)
    timestamp = db.Column(db.DateTime, unique=False, nullable=False)
    comments = db.Column(db.String, nullable=True, default='')


class IMAGE_DIRS(UserMixin, db.Model):
    __bind_key__ = Schema
    __table_args__ = {'sqlite_autoincrement': True,
                      "schema": Schema}
    imdir_auto_key = db.Column(db.Integer, primary_key=True)
    path = db.Column(db.String, unique=True, nullable=False)
    main_category = db.Column(db.String, unique=False, nullable=False)
    sub_category = db.Column(db.String, unique=False, nullable=False)
    last_updated = db.Column(db.DateTime, nullable=False)
    sim_check = db.Column(db.Boolean, nullable=False, default=False)
    dupe_check = db.Column(db.Boolean, nullable=False, default=False)
    available_dir = db.Column(db.Boolean, nullable=False, default=False)


class IMAGE_NOTES(UserMixin, db.Model):
    __bind_key__ = Schema
    __table_args__ = {'schema': Schema}
    img_note_auto_key = db.Column(db.Integer, primary_key=True)
    imfi_auto_key = db.Column(db.Integer, nullable=False)
    timestamp = db.Column(db.DateTime, unique=False, nullable=False)
    notes = db.Column(db.String, unique=False, nullable=False)


class TAGGED_IMAGES(UserMixin, db.Model):
    __bind_key__ = Schema
    __table_args__ = {'sqlite_autoincrement': True,
                      "schema": Schema}
    imtag_auto_key = db.Column(db.Integer, primary_key=True)
    imfi_auto_key = db.Column(db.Integer, nullable=False)
    tag_auto_key = db.Column(db.Integer, unique=False, nullable=False)
    timestamp = db.Column(db.DateTime, unique=False, nullable=False)



class TAGS(UserMixin, db.Model):
    """
    User/System created tags
    """
    __bind_key__ = Schema
    __table_args__ = {"schema": Schema}
    tag_auto_key = db.Column(db.Integer, primary_key=True)
    category = db.Column(db.String, unique=False, nullable=False)
    detail = db.Column(db.String, unique=False, nullable=False)
    timestamp = db.Column(db.DateTime, unique=False, nullable=False)
    last_used = db.Column(db.DateTime, unique=False, nullable=False)
    notes = db.Column(db.String, unique=False, nullable=False)


class TAG_DIRECTORY_CROSSOVER(UserMixin, db.Model):
    """
    A precalculated table that lets you search up tags by MAIN_CATEGORY,SUB_CATEGORY,TAG
    """
    __bind_key__ = Schema
    __table_args__ = {"schema": Schema}
    tadicr_auto_key = db.Column(db.Integer, primary_key=True)
    tag_auto_key = db.Column(db.Integer, nullable=False)
    imdir_auto_key = db.Column(db.Integer, nullable=False)
    total_count = db.Column(db.Integer, nullable=False, default=0)




class SEQUENCE(UserMixin, db.Model):
    __bind_key__ = Schema
    __table_args__ = {'sqlite_autoincrement': True,
                      "schema": Schema}
    se_auto_key = db.Column(db.Integer, primary_key=True)
    sequence_name = db.Column(db.String, nullable=False)
    sequence_number = db.Column(db.Integer, nullable=False)
    imfi_auto_key = db.Column(db.Integer, nullable=False)


class TAGGED_IMAGES_PRECALC(UserMixin, db.Model):
    """
    This table is used to save on calculation time for the search_files function
    """
    __bind_key__ = Schema
    __table_args__ = {"schema": Schema}
    precalc_imtag_ak = db.Column(db.Integer, primary_key=True)
    imfi_auto_key = db.Column(db.Integer, nullable=False)
    tags = db.Column(db.Text, nullable=True)
    tag_auto_keys = db.Column(db.String, nullable=True)
    tag_count = db.Column(db.Integer, nullable=False)


from sqlalchemy import text as sql_text

def update_imtag_precalc(IMFI_AK_LIST):
    # Get the records
    def process_IMFI(IMFI_AK):

        db.session.execute(sql_text(f"call {Schema}.update_imtag_precalc_imfi_ak(:imfi_ak);commit;"),
                                   {'imfi_ak': IMFI_AK})

        return False

    if IMFI_AK_LIST:
        if len(IMFI_AK_LIST) > 0:
            print("\nUpdating IMTAG_PRECALC\n")
            pbar = tqdm(IMFI_AK_LIST)
            for IMFI_AK in pbar:
                process_IMFI(IMFI_AK)
        else:
            for IMFI_AK in IMFI_AK_LIST:
                process_IMFI(IMFI_AK)


def update_imtag_precalc_mismatched():
    query = f"""
    select MAIN.IMFI_AUTO_KEY,imtag_precalc.TAGS,MAIN."TAG" from (
    select
        IMFI_AUTO_KEY,FILE_COUNT,coalesce(string_agg("BASE_TAG",'|*|'),'') "TAG",coalesce(string_agg(TAG_AUTO_KEY::character varying,'|*|'),'') "TAG_AUTO_KEY",count(IMTAG_AUTO_KEY) "TAG_COUNT"
         from (
        select IMFI.*,
        IMTAG.IMTAG_AUTO_KEY,'|'||TAG.CATEGORY||':'||TAG.DETAIL||'|' "BASE_TAG",TAG.TAG_AUTO_KEY,IMFI.MIMETYPE 
        from {Schema}.IMAGE_FILES IMFI
        left outer join {Schema}.TAGGED_IMAGES IMTAG on IMTAG.IMFI_AUTO_KEY = IMFI.IMFI_AUTO_KEY
        left outer join {Schema}.TAGS TAG on TAG.TAG_AUTO_KEY = IMTAG.TAG_AUTO_KEY
        order by IMFI.IMFI_AUTO_KEY desc,TAG.CATEGORY asc,TAG.DETAIL asc
        ) SUB
        group by FILE_NAME,SUB.IMFI_AUTO_KEY,SUB.FILE_COUNT
        order by SUB.IMFI_AUTO_KEY desc
        ) MAIN
    inner join {Schema}.tagged_images_precalc IMTAG_PRECALC on IMTAG_PRECALC.IMFI_AUTO_KEY = MAIN.IMFI_AUTO_KEY
    where coalesce(IMTAG_PRECALC.TAGS,'') != coalesce(MAIN."TAG",'')
    
    
    """
    all_mismatched = methods.convert_all_to_dict(db.session.execute(sql_text(query)))
    all_mismatched_imfi_ak = [x['imfi_auto_key'] for x in all_mismatched]
    update_imtag_precalc(all_mismatched_imfi_ak)

    ##Also add missing IMFI's

    missing_precalc = f"""
    select IMFI.IMFI_AUTO_KEY from {Schema}.image_files IMFI
    left join {Schema}.tagged_images_precalc IMTAG_PRECALC on IMFI.IMFI_AUTO_KEY = IMTAG_PRECALC.IMFI_AUTO_KEY
    where IMTAG_PRECALC.IMFI_AUTO_KEY is NULL
    """
    missing_imfi = methods.convert_all_to_dict(db.session.execute(sql_text(missing_precalc)))
    missing_imfi_ak = [x['imfi_auto_key'] for x in missing_imfi]
    update_imtag_precalc(missing_imfi_ak)


def update_missing_imfi_or_imtag_precalc():
    missing_IMFI = methods.convert_all_to_dict(db.session.execute(sql_text(f"""
                SELECT distinct IMG.FILE_NAME from {Schema}.IMAGES IMG
                left join {Schema}.IMAGE_FILES IMFI on IMG.FILE_NAME = IMFI.FILE_NAME
                WHERE IMFI.IMFI_AUTO_KEY is NULL
                and IMG.AVAILABLE_FILE = True
                """)))
    multi_load = []

    if missing_IMFI:
        print("Missing Files Update:")
        for missing_file in tqdm(missing_IMFI, total=len(missing_IMFI)):
            selectedFile = IMAGES.query.filter_by(file_name=missing_file['file_name']).first()
            multi_load.append(
                IMAGE_FILES(file_name=selectedFile.file_name, notes='', available_file=True,
                            private=False,
                            original_date=selectedFile.original_date, category=selectedFile.category,
                            file_count=1, date_added=dt.datetime.now(), path=selectedFile.path,
                            mimetype=selectedFile.mimetype,
                            imdir_auto_key=selectedFile.imdir_auto_key))
        if multi_load:
            db.session.bulk_save_objects(multi_load)
            # imports.imagesorter_engine.execute("commit")
            db.session.commit()
            update_all_file_data()

    missing_precalc = f"""
        select IMFI.IMFI_AUTO_KEY from {Schema}.image_files IMFI
        left join {Schema}.tagged_images_precalc IMTAG_PRECALC on IMFI.IMFI_AUTO_KEY = IMTAG_PRECALC.IMFI_AUTO_KEY
        where IMTAG_PRECALC.IMFI_AUTO_KEY is NULL
        """
    missing_imfi = methods.convert_all_to_dict(db.session.execute(sql_text(missing_precalc)))
    missing_imfi_ak = [x['imfi_auto_key'] for x in missing_imfi]
    update_imtag_precalc(missing_imfi_ak)


def update_all_imtag_precalc():
    # return #Debug Tool
    query = f"""select imfi_auto_key from {Schema}.image_files"""
    results = [result['imfi_auto_key'] for result in methods.convert_all_to_dict(db.session.execute(sql_text(query)))]
    update_imtag_precalc(results)





def update_file_data(imfi_auto_key):
    fileRecord = IMAGE_FILES.query.filter_by(imfi_auto_key=imfi_auto_key).first()
    extension = fileRecord.path.split('.')[-1].lower()
    try:
        ## if not in VIDEO FORMATS
        if extension not in ['mp4','webm','wav']:
            IMG = Image.open(fileRecord.path)
            INFO = []
            try:
                if extension in ['gif']:
                    frames = IMG.n_frames
                elif extension in ['webp']:
                    frameNumber = 1
                    try:
                        while True:
                            IMG.seek(frameNumber)
                            frameNumber += 1
                    except Exception as e:
                        frames = frameNumber

                else:
                    frames = 1
            except Exception as e:
                frames = 1
            if frames > 1 and extension == 'gif':
                for i in range(frames):
                    IMG.seek(i)
                    INFO.append({'duration': IMG.info['duration'], 'frame': i + 1})

            updateDict = {'imfi_auto_key': fileRecord.imfi_auto_key,
                          'dimension_width': IMG.size[0],
                          'dimension_height': IMG.size[1],
                          'frames': frames,
                          'duration': 0,
                          'frame_info_json': json.dumps(INFO)
                          }

        else:
            video = cv2.VideoCapture(fileRecord.path)

            dimensions = [int(video.get(cv2.CAP_PROP_FRAME_WIDTH)), int(video.get(cv2.CAP_PROP_FRAME_HEIGHT))]


            total_frames = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
            if total_frames < 0:
                total_frames = -1
            fps = int(video.get(cv2.CAP_PROP_FPS))
            if fps < 0:
                fps = -1
            duration = total_frames / fps

            updateDict = {'imfi_auto_key': fileRecord.imfi_auto_key,
                          'dimension_width': dimensions[0],
                          'dimension_height': dimensions[1],
                          'frames': total_frames,
                          'duration': duration,
                          'frame_info_json': json.dumps([])
                          }




        if updateDict:
            updateDict['file_size'] = os.path.getsize(fileRecord.path)

            db.session.execute(sql_text(f"""
                            UPDATE {Schema}.IMAGE_FILES
                                SET frames = :frames,
                                    duration = :duration,
                                    dimension_width = :dimension_width,
                                    dimension_height = :dimension_height,
                                    frame_information = :frame_info_json,
                                    file_size = :file_size
                                where imfi_auto_key = :imfi_auto_key
                        """), updateDict)

    except Exception as e:
        if Image.DecompressionBombError:
            pass
        else:
            raise e


def update_all_file_data():
    # return #Debug
    print("______")
    print("Updating file data in IMAGE_FILES...")


    pbar = tqdm(db.session.execute(sql_text(f"""
        SELECT IMFI_AUTO_KEY,PATH FROM {Schema}.IMAGE_FILES
        WHERE FRAMES IS NULL OR DIMENSION_WIDTH IS NULL OR DIMENSION_HEIGHT IS NULL or DURATION is null or FILE_SIZE is null
        
    """)).fetchall())
    for xy,record in enumerate(pbar):
        currFile = record[1].split("\\")[-1]
        pbar.set_description(f'file: {currFile}')
        update_file_data(record[0])
        if xy % 1000 == 0 and xy > 0:
            db.session.commit()
    db.session.commit()


def remove_duplicate_tags():
    # return ##Debug
    # get all duplicate tag_auto_keys in TAGGED_IMAGES
    query = f"""
        select imtag.*--,tag.* 
    from (
    select imtag.imfi_auto_key,max(imtag.tag_auto_key) "tag_auto_key",count(imtag.tag_auto_key) "COUNT" from {Schema}.tagged_images imtag
    group by imtag.imfi_auto_key,imtag.tag_auto_key
    having count(imtag.tag_auto_key) > 1
    ) imtag
    --inner join {Schema}.tags tag on tag.tag_auto_key = imtag.tag_auto_key
    --inner join {Schema}.image_files imfi on imfi.imfi_auto_key = imtag.imfi_auto_key"""
    cursor = db.session.execute(sql_text(query))
    batch_size = 1000

    tqdm_Files = tqdm()
    while True:
        batch = methods.convert_fetch_to_dict(cursor,batch_size)
        if not batch:
            break
        removedFiles = False
        for row in batch:
            tqdm_Files.set_description(
                f'Removing Duplicate Tags:IMFI_AK:{row["imfi_auto_key"]} TAG_AK:{row["tag_auto_key"]}')
            tqdm_Files.update(1)
            IMTAG_AK_MULTI = TAGGED_IMAGES.query.filter_by(imfi_auto_key=row['imfi_auto_key'],
                                                           tag_auto_key=row['tag_auto_key']).all()
            if len(IMTAG_AK_MULTI) > 1:  # Keeps the first one, as to not delete all the tags.
                for imtag_index, IMTAG in enumerate(IMTAG_AK_MULTI):
                    if imtag_index > 0:
                        TAGGED_IMAGES.query.filter_by(imtag_auto_key=IMTAG.imtag_auto_key).delete()
                        removedFiles = True
        if removedFiles:
            db.session.commit()



def full_tag_directory_crossover_refresh(tag_auto_key=None):
    print(f"Updating tag_directory_crossover")

    for query in tqdm([f"""insert into {Schema}.tag_directory_crossover(tag_auto_key,imdir_auto_key,total_count)
                    select * from (
                    select tag.tag_auto_key,imdir.imdir_auto_key,count(tag.tag_auto_key) "total_count" from {Schema}.image_dirs imdir
                    inner join {Schema}.image_files imfi on imfi.imdir_auto_key = imdir.imdir_auto_key 
                    inner join {Schema}.tagged_images imtag on imtag.imfi_auto_key = imfi.imfi_auto_key 
                    inner join {Schema}.tags tag on tag.tag_auto_key = imtag.tag_auto_key
                    where not exists (
                        select 1 from {Schema}.tag_directory_crossover tdc
                        where tag_auto_key = tag.tag_auto_key
                        and imdir_auto_key = imdir.imdir_auto_key
                    )
                    group by tag.tag_auto_key,imdir.imdir_auto_key
                    ) MAIN;""",
                       f"""
                    update {Schema}.tag_directory_crossover 
                    set total_count = subquery.total_count
                    from (
                        select tag.tag_auto_key,imdir.imdir_auto_key,count(tag.tag_auto_key) "total_count" from {Schema}.image_dirs imdir
                        inner join {Schema}.image_files imfi on imfi.imdir_auto_key = imdir.imdir_auto_key 
                        inner join {Schema}.tagged_images imtag on imtag.imfi_auto_key = imfi.imfi_auto_key 
                        inner join {Schema}.tags tag on tag.tag_auto_key = imtag.tag_auto_key
                         
                        group by tag.tag_auto_key,imdir.imdir_auto_key
                    ) as subquery
                    where {Schema}.tag_directory_crossover.imdir_auto_key = subquery.imdir_auto_key
                    and {Schema}.tag_directory_crossover.tag_auto_key = subquery.tag_auto_key
                    """
                          ,
                       f"""delete from {Schema}.tag_directory_crossover
                    where (tag_auto_key,imdir_auto_key) in
                    (select tadicr.tag_auto_key,tadicr.imdir_auto_key from {Schema}.tag_directory_crossover tadicr
                    where not exists(	
                        select 1 from {Schema}.image_dirs imdir 
                        inner join {Schema}.image_files imfi on imfi.imdir_auto_key = imdir.imdir_auto_key 
                        inner join {Schema}.tagged_images imtag on imtag.imfi_auto_key = imfi.imfi_auto_key 
                        inner join {Schema}.tags tag on tag.tag_auto_key = imtag.tag_auto_key and tadicr.tag_auto_key = tag.tag_auto_key 
                        where tadicr.tag_auto_key = tag.tag_auto_key and imdir.imdir_auto_key = tadicr.imdir_auto_key
                        )
                    );"""]):
        db.session.execute(sql_text(query))
        db.session.commit()


