import os
import json
from DBConfig import conn, READABLE_PATH, DATA_PATH
from tqdm import tqdm

def load_document_config():
    """
    Loads DocumentExcelConfigData.json and creates a mapping from localization ID to titleTextMapHash.
    """
    doc_path = os.path.join(DATA_PATH, "ExcelBinOutput", "DocumentExcelConfigData.json")
    if not os.path.exists(doc_path):
        print(f"Document config not found: {doc_path}")
        return {}

    try:
        with open(doc_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading DocumentExcelConfigData.json: {e}")
        return {}

    # Map localization ID (in questIDList) to titleTextMapHash
    loc_id_to_title_hash = {}
    for entry in data:
        title_hash = entry.get('titleTextMapHash')
        quest_id_list = entry.get('questIDList', [])
        for loc_id in quest_id_list:
            loc_id_to_title_hash[loc_id] = title_hash
            
    return loc_id_to_title_hash

def load_localization_config(loc_id_to_title_hash):
    """
    Loads LocalizationExcelConfigData.json and creates a mapping from filename to info (titleTextMapHash, readableId).
    """
    loc_path = os.path.join(DATA_PATH, "ExcelBinOutput", "LocalizationExcelConfigData.json")
    if not os.path.exists(loc_path):
        print(f"Localization config not found: {loc_path}")
        return {}

    try:
        with open(loc_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading LocalizationExcelConfigData.json: {e}")
        return {}

    # Map filename to info
    filename_to_info = {}
    
    # Keys that might contain paths
    path_keys = [
        "dePath", "enPath", "esPath", "frPath", "idPath", "itPath", 
        "jpPath", "krPath", "ptPath", "ruPath", "tcPath", "thPath", 
        "trPath", "viPath", "EDPAFDDJJNM", "FNIFOPDJMMG"
    ]

    for entry in data:
        loc_id = entry.get('id')
        if loc_id not in loc_id_to_title_hash:
            continue
            
        title_hash = loc_id_to_title_hash[loc_id]
        
        for key in path_keys:
            if key in entry:
                path = entry[key]
                if isinstance(path, str) and "Readable" in path:
                    # Extract filename from path
                    # Path example: "ART/UI/Readable/DE/Poem1_DE"
                    filename = os.path.basename(path)
                    filename_to_info[filename] = {
                        'titleHash': title_hash,
                        'readableId': loc_id
                    }

    return filename_to_info

def importReadable():
    print("Loading document and localization configs...")
    loc_id_to_title_hash = load_document_config()
    filename_to_info = load_localization_config(loc_id_to_title_hash)
    print(f"Loaded {len(filename_to_info)} readable file mappings.")

    cursor = conn.cursor()
    sql = "insert or replace into readable(fileName, lang, content, titleTextMapHash, readableId) values (?,?,?,?,?)"
    
    if not os.path.exists(READABLE_PATH):
        print(f"Readable path not found: {READABLE_PATH}")
        return

    langs = os.listdir(READABLE_PATH)
    for lang in langs:
        langPath = os.path.join(READABLE_PATH, lang)
        if not os.path.isdir(langPath):
            continue
            
        files = os.listdir(langPath)
        print(f"Importing readable for {lang}...")
        for fileName in tqdm(files):
            filePath = os.path.join(langPath, fileName)
            if not os.path.isfile(filePath):
                continue
            
            name_without_ext = os.path.splitext(fileName)[0]
            
            info = filename_to_info.get(name_without_ext)
            
            # If not found, try with the full filename just in case
            if info is None:
                 info = filename_to_info.get(fileName)

            title_hash = None
            readable_id = None
            if info:
                title_hash = info['titleHash']
                readable_id = info['readableId']

            try:
                with open(filePath, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Replace actual newlines with literal \n sequence to match TextMap format
                    content = content.replace('\n', '\\n')
                cursor.execute(sql, (fileName, lang, content, title_hash, readable_id))
            except Exception as e:
                print(f"Error reading {fileName}: {e}")
                
    cursor.close()
    conn.commit()