import yaml
import os
## This file has no dependencies on the server and can be imported separately.


def get_yaml_config():
    #Creates default YAML if needed
    try:
        yaml_path = f'{os.path.dirname(__file__)}/config.yaml'
        print("YAML PATH:::", yaml_path)
        if os.path.isfile(yaml_path):
            with open(yaml_path, 'r') as FILE:
                config_data = yaml.safe_load(FILE)
                if config_data is None:
                    config_data = {}
        else:
            print('Config file is unavailable. Creating a new one.')
            raise Exception()
    except:
        config_yaml = """### Server needs to be restarted after changing any of these settings.\n""" \
                      """#thumbnail_sizes: [200,400,800] #The view sizes on the main page of ImageSorter. 3 Sizes are required. Default - [200,400,800]\n""" \
                      """#max_thumbnail_size: 2000 #If the full image size is greater than this size, it saves then loads a thumbnail off to reduce future load times. Default - 2000\n""" \
                      """#disable_thumbnail_generation: False #Disables thumbnails from being saved to disk. #Default - False\n""" \
                      """#pil_max_image_pixels: 89478485 #Allows images of this pixel size to load into ram. Higher number requires more resources. Otherwise throws an error if too large. Default - 89478485\n""" \
                      """## Folders - by default these folders are in the /static/ folder, but you can set a location on your system to have it save files elsewhere. Must be the full path. Example:#C:/Sample_Folder/Like_This/\n""" \
                      """#pending_removal_folder: \n""" \
                      """#pending_removal_dupes_folder:\n""" \
                      """#thumbnails_folder:\n""" \
                      """#user_data_folder:\n""" \
                      """#disabled_extensions: #List of extensions to disable, exact folder name in the extensions folder -[List] - String (Folder Name)\n""" \
                      """## If you wish to add a new instance of imagesorter, you'll need a new copy of the 'imagesorter' module and then update both of the following parameters:\n"""
        """#append_schema: #Appends the alternate schema onto 'imagesorter'. IE: 'landscape' would be 'imagesorter_landscape'.\n""" \
        """#append_route:  #Appends the alternate route onto 'imagesorter'. IE: 'landscape' would be '/imagesorter_landscape/' instead of '/imagesorter/'.""" \
        """## Keep in mind that this may cause issues within the 'templates/' folders. Any changes made to an instance of ImageSorter templates should be copied across all instances to ensure it will work right."""
        yaml_path = f'{os.path.dirname(__file__)}/config.yaml'
        # If it does not exist, create a new one.
        if os.path.isfile(yaml_path) is False:
            with open(yaml_path, 'w') as FILE:
                FILE.write(config_yaml)
        config_data = yaml.safe_load(config_yaml)
        if config_data is None:
            config_data = {}
    return config_data