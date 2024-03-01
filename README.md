# ImageSorter

A Flask blueprint module for tagging and filtering Image and Video files.

> :warning: **This base module can modify your system by user-input with relocating and renaming files linked to the module. Extensions may or may not change this. Please be aware of this when using it.**

ImageSorter is designed to help users efficiently manage and retrieve their personal images and videos. This program simplifies the organization and accessibility of the users uploaded content by allowing users to tag, sequence, rename, and search their files.

This is intended to be a single-user application, but extensions have the potential to change this.

## Table of contents
* [Showcase Video](#Showcase Video)
* [Screenshots](#screenshots)
* [Features](#features)
* [Documentation](#documentation)
* [Installation](#installation)
* [Updating the module](#updating-the-module)
* [Running the module](#running-the-module)
* [Config File](#config-file)

## Showcase Video

* [Video Link](https://www.youtube.com/watch?v=t79a7t0Tg_U) - This showcase does not go in-depth into everything, but is a good starting point. Refer to the built-in help menu on the main /ImageSorter/ page for more information. 

## Screenshots
|                                                                              |                                                          |
|------------------------------------------------------------------------------|----------------------------------------------------------|
 ![Overview](sample_images/01%20-%20Overview.jpg)                             | ![Multi Select](sample_images/02%20-%20Multi-Select.jpg) 
 ![Wide UI](sample_images/03%20-%20Zoom%20-%20Alt%20UI%20-%20Added%20Tag.jpg) | ![UI Menu](sample_images/04%20-%20UI%20Menu.jpg)         





## Features
* Tagging System: Users can apply customized tags to their files. Tags serve as keywords, making it easier to categorize and find specific content later.
* Hotkeys: Users can apply tags to their files efficiently using hotkeys, making it easy to categorize and find specific content quickly.
* Multi-File Tagging: The module allows users to tag multiple files simultaneously, streamlining the tagging process and saving time.
* Search Functionality: The module offers an in-depth search tool that enables users to quickly locate files based on keywords, tags, and other search criteria.
* File Sequencing: Users have the ability to link their files in a specific order, good for any files that follow a specific sequence such as comics, events, etc.
* File Renaming: The module allows for renaming of files, helping users maintain consistent naming conventions and improving file organization.
* Relocation: Users can easily relocate their tagged files to a different folder in the system based on the provided file search, enhancing file organization and management.
* Separate files into designated categories to prevent overlap.
    * EG: Have photos separate from artwork.
* Extension support


## Documentation
#### General Use
* Information on how to use the page will be included on each page itself within the help menu. 
#### Extensions
* Extensions can be easily installed, but require a server restart before they work.
* Sample extensions will be provided in the near future.
> :warning: Always be cautious when installing an extension as it can execute arbitrary code. Be sure to inspect its code before use.


## Installation
The following requirements need to be fulfilled before running this module:
* Flask Server is connected to a POSTGRES database.
* POSTGRES extension 'pg_trgm' will be installed automatically.
* Schema 'imagesorter' will be created automatically. See [ImageSorter Instances](#config---separate-imagesorter-instances) if this is a separate instance.


If using [HomeWeb](https://github.com/KJM-Code/Homeweb), use the provided `setup.py` file in your Homeweb/ folder. It will register the module, create the schema, and bind the database, etc

If using your own flask server, put the module in the folder of your choice and import the blueprint list object, 'blueprints', from the 'base_settings.py' file and register all of them to your application. At this time this will include the Main blueprint, and the API blueprint. The API blueprint is still incomplete, but you're free to use it for the time being by enabling it in the config.

By default the schema and database_binding are the same: 'imagesorter'. If changing the 'append_schema' config down below, it will be 'imagesorter_{append_schema}', with '{append_schema}' being the value you put in the parameter.

Example for registering the blueprints:
```python
from modules.imagesorter.base_settings import blueprints as imagesorter_blueprints,Schema as imagesorter_Schema,database_binding as imagesorter_db_binding

for blueprint in imagesorter_blueprints:
    app.register_blueprint(blueprint)
```
then add the database binding and create the schema however is preferred to your application.

### Updating the module
Please use the provided 'update.bat' file located within the main folder. Be sure to activate your environment first.


## Running the module
To run the module, initiate the server that the ImageSorter module is being hosted on. It is recommended to run the server in HTTPS mode, but is not necessary to use. Without HTTPS, some features such as copying filenames will not work.

-If using the (LINK HERE) HomeWeb application, please go to [HomeWeb](https://github.com/KJM-Code/Homeweb#Usage) to learn how to run it.

## Config File
After running the ImageSorter module for the first time it will generate a config.yaml file. Here is a list of the parameters and default values, feel free to adjust them.
* Requires server restart before changes take effect.

| Parameter                    | Default       | Description                                                                                                                               | Type                                  |
|------------------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| thumbnail_sizes              | [200,400,800] | The thumbnail view sizes on the main page of ImageSorter. 3 Sizes are required.                                                           | List of integers                      |
| max_thumbnail_size           | 2000          | If the full image size is greater than this size, it saves then loads a thumbnail off to reduce future load times.                        | Integer                               |
| disable_thumbnail_generation | False         | Disables thumbnails from being saved to disk.                                                                                             | Boolean                               |
| pil_max_image_pixels         | 89478485      | Allows images of this pixel size to load, otherwise throws an error if greater than the provided number. Higher numbers require more ram. | Pixels - Integer                      |
| disabled_extensions          |               | List of extensions to disable, exact folder name in the extensions folder                                                                 | List - String (Extension folder name) |


### Config - Folder Path Parameters
* by default these folders are in the /static/imagesorter/ folder, but you can set a location on your system to have it save files elsewhere. Must be the full path.
* example: C:/Sample_Folder/Like_This/

| Parameter | Default | Description | Type |
|-----|---|-------|-----|
| pending_removal_folder | static/imagesorter/pending_removal/ | Folder for removed files to go in. | Path - String |
| pending_removal_dupes_folder | static/imagesorter/pending_removal_dupes | Folder for removed duplicate (by name) files to go in. | Path - String
| thumbnails_folder | static/imagesorter/thumbnails | Folder for generated thumbnails to go in. | Path - String |
| user_data_folder | static/imagesorter/user_info | Folder for user information to go in, such as saved custom searches | Path - String |

### Config - Separate ImageSorter instances
* If you wish to add a new instance of imagesorter, you'll need a new copy of the 'imagesorter' module and then update both of the following parameters:

| Parameter | Default | Description                                                                                                                   | Type |
|-----|---|-------------------------------------------------------------------------------------------------------------------------------|-----|
| append_schema | | Appends the alternate schema onto 'imagesorter'. IE: 'landscape' would be 'imagesorter_landscape'.                            | String |
| append_route  | | Appends the alternate route onto 'imagesorter'. IE: 'landscape' would be '/imagesorter_landscape/' instead of '/imagesorter/'. | String |

> :warning: All ImageSorter instances have common 'templates' and 'static' folders. The above specified folders are expected to remain separate; however, for the other folders and files within the static/templates directories, any modifications to the other files will need to be copied to all ImageSorter module instances to prevent issues.