from imports.database import db
try:
   from .base_settings import Schema
except:
   from base_settings import Schema
from sqlalchemy import text as sql_text

extensions = [f"create extension if not exists pg_trgm"]

procedures = [
f"""
CREATE OR REPLACE PROCEDURE {Schema}.update_imtag_precalc_imfi_ak(IN imfi_ak integer)
 LANGUAGE plpgsql
AS $procedure$
	BEGIN
			update {Schema}.tagged_images_precalc 
					set 
						tags = MAIN."TAG",
		                tag_auto_keys = MAIN."TAG_AUTO_KEY",
		                tag_count = MAIN."TAG_COUNT"
					
					from (  select
						    	SUB.IMFI_AUTO_KEY,FILE_COUNT,coalesce(string_agg("BASE_TAG",'|*|' order by "BASE_TAG" asc),'') "TAG",coalesce(string_agg(TAG_AUTO_KEY::character varying,'|*|' order by "BASE_TAG" asc),'') "TAG_AUTO_KEY",count(IMTAG_AUTO_KEY) "TAG_COUNT"
						     from (
						    select IMFI.*,
						    IMTAG.IMTAG_AUTO_KEY,'|'||TAG.CATEGORY||':'||TAG.DETAIL||'|' "BASE_TAG",TAG.TAG_AUTO_KEY,IMFI.MIMETYPE
						
						    from {Schema}.IMAGE_FILES IMFI
						    left outer join {Schema}.TAGGED_IMAGES IMTAG on IMTAG.IMFI_AUTO_KEY = IMFI.IMFI_AUTO_KEY
						    left outer join {Schema}.TAGS TAG on TAG.TAG_AUTO_KEY = IMTAG.TAG_AUTO_KEY
						    order by IMFI.ORIGINAL_DATE desc,TAG.CATEGORY asc,TAG.DETAIL asc
						    ) SUB
						    WHERE SUB.IMFI_AUTO_KEY = imfi_ak
						    group by FILE_NAME,SUB.IMFI_AUTO_KEY,SUB.FILE_COUNT) as MAIN
				     where {Schema}.tagged_images_precalc.imfi_auto_key = MAIN.IMFI_AUTO_KEY;
				     
	END;
$procedure$
""",
f"""
CREATE OR REPLACE PROCEDURE {Schema}.procedure_update_all_imfi_in_tag_ak(IN _tag_ak integer)
 LANGUAGE plpgsql
AS $procedure$
	begin
		
			update {Schema}.tagged_images_precalc 
					set 
								tags = MAIN."TAG",
				                tag_auto_keys = MAIN."TAG_AUTO_KEY",
				                tag_count = MAIN."TAG_COUNT"
					
						from (    select
							    SUB.IMFI_AUTO_KEY,FILE_COUNT,coalesce(string_agg("BASE_TAG",'|*|' order by "BASE_TAG" asc),'') "TAG",coalesce(string_agg(TAG_AUTO_KEY::character varying,'|*|' order by "BASE_TAG" asc),'') "TAG_AUTO_KEY",count(IMTAG_AUTO_KEY) "TAG_COUNT"
							     from (
							    select IMFI.*,
							    IMTAG.IMTAG_AUTO_KEY,'|'||TAG.CATEGORY||':'||TAG.DETAIL||'|' "BASE_TAG",TAG.TAG_AUTO_KEY,IMFI.MIMETYPE
							
							    from {Schema}.IMAGE_FILES IMFI
							        inner join {Schema}.tagged_images imtag_with_tag_ak on imtag_with_tag_ak.IMFI_AUTO_KEY = IMFI.IMFI_AUTO_KEY and imtag_with_tag_ak.tag_auto_key = _tag_ak
							    left outer join {Schema}.TAGGED_IMAGES IMTAG on IMTAG.IMFI_AUTO_KEY = imtag_with_tag_ak.IMFI_AUTO_KEY
							    left outer join {Schema}.TAGS TAG on TAG.TAG_AUTO_KEY = IMTAG.TAG_AUTO_KEY
							    order by IMFI.ORIGINAL_DATE desc,TAG.CATEGORY asc,TAG.DETAIL asc
							    ) SUB
							    group by FILE_NAME,SUB.IMFI_AUTO_KEY,SUB.FILE_COUNT) as MAIN
				     where {Schema}.tagged_images_precalc.imfi_auto_key = MAIN.IMFI_AUTO_KEY;
		
	END;
$procedure$
"""
]
functions = [
f"""
CREATE OR REPLACE FUNCTION {Schema}.dot_product(vector1 double precision[], vector2 double precision[])
 RETURNS double precision
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN(SELECT sum(mul) FROM (SELECT v1e*v2e as mul FROM unnest(vector1, vector2) AS t(v1e,v2e)) AS denominator);
END;
$function$
    """,
f"""
CREATE OR REPLACE FUNCTION {Schema}.function_new_imfi()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO
        {Schema}.tagged_images_precalc(imfi_auto_key,tags,tag_auto_keys,tag_count) 
        VALUES(new.imfi_auto_key,'','',0);
        RETURN new;
END;
$function$
""",
f"""
CREATE OR REPLACE FUNCTION {Schema}.hamming_distance(hash1 text, hash2 text)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    bit1 BIT(4);
    bit2 BIT(4);
    dist INTEGER := 0;
BEGIN
    IF length(hash1) != length(hash2) THEN
        RAISE EXCEPTION 'Hashes must be of equal length';
    END IF;

    FOR i IN 1..length(hash1) LOOP
        bit1 := ('x' || substr(hash1, i, 1))::bit(4);
        bit2 := ('x' || substr(hash2, i, 1))::bit(4);
        dist := dist + bit_count(bit1 # bit2);
    END LOOP;

    RETURN dist;
END;
$function$
""",
f"""
CREATE OR REPLACE FUNCTION {Schema}.trigger_update_imtag_precalc_on_tag_update()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin
			if tg_op = 'UPDATE' then
				call {Schema}.procedure_update_all_imfi_in_tag_ak(new.tag_auto_key);
				return new;
			elseif tg_op = 'INSERT' then
				call {Schema}.procedure_update_all_imfi_in_tag_ak(new.tag_auto_key);
				return new;
			elseif tg_op = 'DELETE' then
				call {Schema}.procedure_update_all_imfi_in_tag_ak(old.tag_auto_key);
				return old;
			else
				return null;
			end if;
END;
$function$
""",
f"""
CREATE OR REPLACE FUNCTION {Schema}.trigger_update_imtag_precalc_on_modify_imtag()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin
			if tg_op = 'UPDATE' then
				call {Schema}.update_imtag_precalc_imfi_ak(new.imfi_auto_key);
				return new;
			elseif tg_op = 'INSERT' then
				call {Schema}.update_imtag_precalc_imfi_ak(new.imfi_auto_key);
				return new;
			elseif tg_op = 'DELETE' then
				call {Schema}.update_imtag_precalc_imfi_ak(old.imfi_auto_key);
				return old;
			else return null;
			end if;
END;
$function$
""",
f"""
CREATE OR REPLACE FUNCTION {Schema}."trigger_on_image_insert_-_insert_imfi_if_needed"()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
	begin
		--if imfi_ak does not exist for filename, create imfi_ak
		if (select 1 from {Schema}.image_files where file_name = new.file_name limit 1)  is null then
			insert into {Schema}.image_files(file_name,notes,available_file,original_date,category,file_count,private,path,mimetype,date_added,imdir_auto_key)
			values(new.file_name,'',true,new.original_date,new.category,1,false,new.path,new.mimetype,now(),new.imdir_auto_key);
  			 return new;
  		else return new;
		end if;
	
		
	END;
$function$
""",
f"""CREATE OR REPLACE FUNCTION {Schema}.vector_norm(vector double precision[])
 RETURNS double precision
 LANGUAGE plpgsql
AS $function$
BEGIN
     RETURN(SELECT SQRT(SUM(pow)) FROM (SELECT POWER(e,2) as pow from unnest(vector) as e) as norm);
     --RETURN(SELECT AVG(pow) FROM (SELECT POWER(e,2) as pow from unnest(vector) as e) as norm);
END;
$function$
""",
f"""
CREATE OR REPLACE FUNCTION {Schema}.trigger_tags_before_update_or_insert_to_tags_keep_capitalized()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin
			if tg_op = 'UPDATE' then
				new.category := upper(new.category);
				new.detail := upper(new.detail);
				return new;
			elseif tg_op = 'INSERT' then
				new.category := upper(new.category);
				new.detail := upper(new.detail);
				return new;
			end if;
END;
$function$
""",
f"""
CREATE OR REPLACE FUNCTION {Schema}.trigger_crossover_update___imfi_update_imdir()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare 
	record_data RECORD;
    cur_cursor CURSOR FOR SELECT tag_auto_key from {Schema}.tagged_images where imfi_auto_key = new.imfi_auto_key;
begin
			if tg_op = 'UPDATE' then
				if old.imdir_auto_key != new.imdir_auto_key then
					OPEN cur_cursor;
				    LOOP
				        FETCH cur_cursor INTO record_data;
				        EXIT WHEN NOT FOUND; -- Exit the loop when there are no more records to fetch
				        -- Process the current record_data here
					        --Create new tadicr record if needed
				        IF EXISTS (SELECT 1 FROM {Schema}.tag_directory_crossover tadicr 
					    where imdir_auto_key = old.imdir_auto_key and tag_auto_key = record_data.tag_auto_key
					    ) then
					      update {Schema}.tag_directory_crossover 
					        set total_count = {Schema}.tag_directory_crossover.total_count-1
					        where imdir_auto_key = old.imdir_auto_key and tag_auto_key = record_data.tag_auto_key;
				        end if;
				        if not EXISTS (SELECT 1 FROM {Schema}.tag_directory_crossover tadicr 
					    where imdir_auto_key = new.imdir_auto_key and tag_auto_key = record_data.tag_auto_key
					    ) then
					       --create the record if not exists
					       insert into {Schema}.tag_directory_crossover(tag_auto_key,imdir_auto_key,total_count)
					       values(record_data.tag_auto_key,new.imdir_auto_key,0);
					    end if;
				      update {Schema}.tag_directory_crossover 
				        set total_count = {Schema}.tag_directory_crossover.total_count+1
				        where imdir_auto_key = new.imdir_auto_key and tag_auto_key = record_data.tag_auto_key;
				        --RAISE NOTICE 'Column1: %, Column2: %', record_data.column1, record_data.column2;
				    END LOOP;
				    CLOSE cur_cursor;
				end if;
				return new;
			end if;
END;
$function$
""",
f"""
CREATE OR REPLACE FUNCTION {Schema}.trigger_crossover_update___imtag_update_insert_or_delete_tags()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare 
	d_old_imdir_auto_key INTEGER;
	d_imdir_auto_key INTEGER;
begin	
	if tg_op = 'UPDATE' or tg_op = 'INSERT' then
		d_imdir_auto_key := (select imdir_auto_key from {Schema}.image_files where imfi_auto_key = new.imfi_auto_key);
		-- Check if a relevant record exists in the destination_table
	    IF not EXISTS (SELECT 1 FROM {Schema}.tag_directory_crossover tadicr 
	    where imdir_auto_key = d_imdir_auto_key and tag_auto_key = new.tag_auto_key
	    ) then
	       --create the record if not exists
	       insert into {Schema}.tag_directory_crossover(tag_auto_key,imdir_auto_key,total_count)
	       values(new.tag_auto_key,d_imdir_auto_key,0);
        end if;
		if tg_op = 'INSERT' then
			--add 1 point to the new tag
			update {Schema}.tag_directory_crossover
					set total_count = {Schema}.tag_directory_crossover.total_count+1
					where imdir_auto_key = d_imdir_auto_key and tag_auto_key = new.tag_auto_key;
		elseif tg_op = 'UPDATE' then
			if old.tag_auto_key != new.tag_auto_key or old.imfi_auto_key != new.imfi_auto_key then
				d_old_imdir_auto_key:= (select imdir_auto_key from {Schema}.image_files where imfi_auto_key = old.imfi_auto_key);	
				IF not EXISTS (SELECT 1 FROM {Schema}.tag_directory_crossover tadicr 
				    where imdir_auto_key = d_old_imdir_auto_key and tag_auto_key = old.tag_auto_key
				    ) then
				       --create the record if not exists
				       insert into {Schema}.tag_directory_crossover(tag_auto_key,imdir_auto_key,total_count)
				       values(new.tag_auto_key,d_old_imdir_auto_key,0);
				end if;
				if d_old_imdir_auto_key != d_imdir_auto_key then
					update {Schema}.tag_directory_crossover
						set total_count = {Schema}.tag_directory_crossover.total_count-1
						where imdir_auto_key = d_old_imdir_auto_key and tag_auto_key = new.tag_auto_key;
				end if;
				update {Schema}.tag_directory_crossover
					set total_count = {Schema}.tag_directory_crossover.total_count+1
					where imdir_auto_key = d_imdir_auto_key and tag_auto_key = new.tag_auto_key;
			end if;
				--remove 1 point from the original tag, add 1 point to the new tag (unless they're the same)
		end if;
		delete from {Schema}.tag_directory_crossover where total_count = 0 
		and tag_auto_key = new.tag_auto_key and imdir_auto_key = d_old_imdir_auto_key; 
		return new;
	elseif tg_op = 'DELETE' then
		--remove a point from the record if it exists
		d_imdir_auto_key := (select imdir_auto_key from {Schema}.image_files where imfi_auto_key = old.imfi_auto_key);
		IF EXISTS (SELECT 1 FROM {Schema}.tag_directory_crossover tadicr 
	    where imdir_auto_key = d_imdir_auto_key and tag_auto_key = old.tag_auto_key) then
	    	update {Schema}.tag_directory_crossover
				set total_count = {Schema}.tag_directory_crossover.total_count-1
				where imdir_auto_key = d_imdir_auto_key and tag_auto_key = old.tag_auto_key;
			delete from {Schema}.tag_directory_crossover where total_count = 0
			and imdir_auto_key = d_imdir_auto_key and tag_auto_key = old.tag_auto_key;
		end if;
		return old;
	else
		return null;
	end if;
END;
$function$
""",
f"""
CREATE OR REPLACE FUNCTION {Schema}.trigger_imfi_tag_update_timestamp___imfi_tag_update()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare 
	d_imfi_auto_key INTEGER;
begin
	if tg_op = 'UPDATE' or tg_op = 'INSERT' then
		d_imfi_auto_key := new.imfi_auto_key;
	elseif tg_op = 'DELETE' then
		d_imfi_auto_key := old.imfi_auto_key;
	end if;
	update {Schema}.image_files
	set last_tag_update = NOW()
	where imfi_auto_key = d_imfi_auto_key;
	if tg_op = 'UPDATE' or tg_op = 'INSERT' then
		return new;
	elseif tg_op = 'DELETE' then
		return old;
	else
		return null;
	end if;
END;
$function$
""",
 """
 CREATE OR REPLACE FUNCTION prevent_tag_colon_insert()
   RETURNS TRIGGER AS $$
   BEGIN
     IF NEW.category LIKE '%:%' OR NEW.detail LIKE '%:%' THEN
       RAISE EXCEPTION 'Text cannot contain a colon (:)';
     END IF;
     RETURN NEW;
   END;
   $$ LANGUAGE plpgsql;
 """,
 """
 CREATE OR REPLACE FUNCTION tags_keep_capitalized()
   RETURNS TRIGGER AS $$
   BEGIN
     NEW.CATEGORY = upper(NEW.CATEGORY);
     NEW.DETAIL = upper(NEW.DETAIL);
     RETURN NEW;
   END;
   $$ LANGUAGE plpgsql;
 """,
r"""
 CREATE OR REPLACE FUNCTION keep_filepaths_clean()
   RETURNS TRIGGER AS $$
   BEGIN
    IF NEW.PATH ~ '/|\\\\' THEN
        RAISE EXCEPTION 'Path contains invalid characters';
    END IF;
    RETURN NEW;
   END;
   $$ LANGUAGE plpgsql;
"""
]
triggers = [
f'CREATE OR REPLACE TRIGGER create_imtag_precalc_record_on_new_imfi AFTER INSERT ON {Schema}.image_files FOR EACH ROW EXECUTE FUNCTION {Schema}.function_new_imfi()',
 f'CREATE OR REPLACE TRIGGER trigger_on_tag_update AFTER UPDATE ON {Schema}.tags FOR EACH ROW WHEN ((((old.category)::text IS DISTINCT FROM (new.category)::text) OR ((old.detail)::text IS DISTINCT FROM (new.detail)::text))) EXECUTE FUNCTION {Schema}.trigger_update_imtag_precalc_on_tag_update()',
 f'CREATE OR REPLACE TRIGGER "trigger_on_update_-_update_imtag_precalc" AFTER INSERT OR DELETE OR UPDATE ON {Schema}.tagged_images FOR EACH ROW EXECUTE FUNCTION {Schema}.trigger_update_imtag_precalc_on_modify_imtag()',
 f'CREATE OR REPLACE TRIGGER trigger_on_insert_create_imfi_if_needed AFTER INSERT ON {Schema}.images FOR EACH ROW EXECUTE FUNCTION {Schema}."trigger_on_image_insert_-_insert_imfi_if_needed"()',
 f'CREATE OR REPLACE TRIGGER uppercase_tag_trigger BEFORE INSERT OR UPDATE ON {Schema}.tags FOR EACH ROW EXECUTE FUNCTION {Schema}.trigger_tags_before_update_or_insert_to_tags_keep_capitalized()',
 f'CREATE OR REPLACE TRIGGER update_tag_dir_crossover BEFORE INSERT OR DELETE OR UPDATE ON {Schema}.tagged_images FOR EACH ROW EXECUTE FUNCTION {Schema}.trigger_crossover_update___imtag_update_insert_or_delete_tags()',
 f'CREATE OR REPLACE TRIGGER update_tag_dir_crossover_on_imdir_update BEFORE UPDATE ON {Schema}.image_files FOR EACH ROW EXECUTE FUNCTION {Schema}.trigger_crossover_update___imfi_update_imdir()',
 f'CREATE OR REPLACE TRIGGER update_imfi_tag_update_timestamp BEFORE INSERT OR DELETE OR UPDATE ON {Schema}.tagged_images FOR EACH ROW EXECUTE FUNCTION {Schema}.trigger_imfi_tag_update_timestamp___imfi_tag_update()',
 f'CREATE OR REPLACE TRIGGER check_tag_for_colon BEFORE INSERT OR UPDATE ON {Schema}.tags FOR EACH ROW EXECUTE FUNCTION prevent_tag_colon_insert();',
 f'CREATE OR REPLACE TRIGGER check_tag_for_colon BEFORE INSERT OR UPDATE ON {Schema}.tags FOR EACH ROW EXECUTE FUNCTION tags_keep_capitalized();',
 f'CREATE OR REPLACE TRIGGER check_file_paths BEFORE INSERT OR UPDATE ON {Schema}.image_files FOR EACH ROW EXECUTE FUNCTION keep_filepaths_clean();',
 f'CREATE OR REPLACE TRIGGER check_file_paths BEFORE INSERT OR UPDATE ON {Schema}.images FOR EACH ROW EXECUTE FUNCTION keep_filepaths_clean();',

]

indexes = [
 f'CREATE UNIQUE INDEX IF NOT EXISTS  tag_directory_crossover_pkey ON {Schema}.tag_directory_crossover USING btree (tadicr_auto_key)',
 f'CREATE INDEX IF NOT EXISTS  tag_directory_crossover_tag_auto_key_idx ON {Schema}.tag_directory_crossover USING btree (tag_auto_key, imdir_auto_key)',
 f'CREATE UNIQUE INDEX IF NOT EXISTS  image_file_notes_pkey ON {Schema}.image_file_notes USING btree (imno_auto_key)',
 f'CREATE INDEX IF NOT EXISTS  index_image_files_original_date_desc ON {Schema}.image_files USING btree (original_date DESC)',
 f'CREATE INDEX IF NOT EXISTS  index_images_files ON {Schema}.image_files USING btree (imfi_auto_key)',
 f'CREATE UNIQUE INDEX IF NOT EXISTS  image_files_pkey ON {Schema}.image_files USING btree (imfi_auto_key)',
 f'CREATE UNIQUE INDEX IF NOT EXISTS  image_files_file_name_key ON {Schema}.image_files USING btree (file_name)',
 f'CREATE INDEX IF NOT EXISTS  image_files_imdir_auto_key_idx ON {Schema}.image_files USING btree (imdir_auto_key)',
 f'CREATE INDEX IF NOT EXISTS  index_image_files_last_tag_update_desc ON {Schema}.image_files USING btree (last_tag_update DESC)',
 f'CREATE UNIQUE INDEX IF NOT EXISTS  image_dirs_pkey ON {Schema}.image_dirs USING btree (imdir_auto_key)',
 f'CREATE UNIQUE INDEX IF NOT EXISTS  image_dirs_path_key ON {Schema}.image_dirs USING btree (path)',
 f'CREATE INDEX IF NOT EXISTS  index_images ON {Schema}.images USING btree (img_auto_key, original_date DESC, file_name)',
 f'CREATE INDEX IF NOT EXISTS  images_path_for_upload_checks_idx ON {Schema}.images USING btree (path)',
 f'CREATE UNIQUE INDEX IF NOT EXISTS  images_un_path ON {Schema}.images USING btree (path)',
 f'CREATE UNIQUE INDEX IF NOT EXISTS  images_pkey ON {Schema}.images USING btree (img_auto_key)',
 f'CREATE UNIQUE INDEX IF NOT EXISTS  image_notes_pkey ON {Schema}.image_notes USING btree (img_note_auto_key)',
 f'CREATE INDEX IF NOT EXISTS  index_tagged_images_tag_ak ON {Schema}.tagged_images USING btree (tag_auto_key)',
 f'CREATE INDEX IF NOT EXISTS  index_tagged_images ON {Schema}.tagged_images USING btree (imfi_auto_key, tag_auto_key)',
 f'CREATE INDEX IF NOT EXISTS  tagged_images_imfi_auto_key_idx ON {Schema}.tagged_images USING btree (imfi_auto_key)',
 f'CREATE UNIQUE INDEX IF NOT EXISTS  tagged_images_imfi_auto_key_imtag_auto_key_key ON {Schema}.tagged_images USING btree (imfi_auto_key, imtag_auto_key)',
 f'CREATE INDEX IF NOT EXISTS  tagged_images_tag_auto_key_idx ON {Schema}.tagged_images USING btree (tag_auto_key)',
 f'CREATE UNIQUE INDEX IF NOT EXISTS  tagged_images_pkey ON {Schema}.tagged_images USING btree (imtag_auto_key)',
 f'CREATE UNIQUE INDEX IF NOT EXISTS  sequence_pkey ON {Schema}.sequence USING btree (se_auto_key)',
 f'CREATE INDEX IF NOT EXISTS  index_tags_category_detail_asc ON {Schema}.tags USING btree (category, detail)',
 f'CREATE INDEX IF NOT EXISTS  index_tags ON {Schema}.tags USING btree (tag_auto_key)',
 f'CREATE INDEX IF NOT EXISTS  index_tag_combo ON {Schema}.tags USING btree (((((category)::text || \':\'::text) || (detail)::text)))',
 f'CREATE UNIQUE INDEX IF NOT EXISTS  tags_pkey ON {Schema}.tags USING btree (tag_auto_key)',
 f'CREATE INDEX IF NOT EXISTS  index_precalc_imtag_imfi ON {Schema}.tagged_images_precalc USING btree (imfi_auto_key)',
 f'CREATE UNIQUE INDEX IF NOT EXISTS  tagged_images_precalc_pkey ON {Schema}.tagged_images_precalc USING btree (precalc_imtag_ak)',
 f'CREATE INDEX IF NOT EXISTS  tagged_images_precalc_tags_indx ON {Schema}.tagged_images_precalc USING gin (tags gin_trgm_ops)',
]


def create_required_sql():
    for create_list in [extensions,functions,procedures,triggers,indexes]:
        for creation_statement in create_list:
            db.session.execute(sql_text(str(creation_statement)))
    db.session.commit()
