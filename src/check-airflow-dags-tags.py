import os
import re
import json
 
def find_dag_files(directory):
    dag_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                dag_files.append(os.path.join(root, file))
    return dag_files
 
def find_emr_tags_in_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        tags_pattern = re.compile(r'["\'](?:Tags|tags)["\']\s*:\s*\[([^]]*)\]', re.MULTILINE)
        matches = tags_pattern.findall(content)
        return matches
 
def find_tags_in_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        tags_pattern = re.compile(r'tags\s*=\s*\[([^\]]*)\]', re.IGNORECASE)
        matches = tags_pattern.findall(content)
        if not matches:
            tags_pattern = re.compile(r'tags\s*=\s*\(([^\)]*)\)', re.IGNORECASE)
            matches = tags_pattern.findall(content)
        return matches
 
def find_sec_conf_in_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
 
        security_config_pattern = re.compile(r'"SecurityConfiguration"\s*:\s*([^\n,]*)')
        match = security_config_pattern.search(content)
 
        if match:
            security_config_value = match.group(1).strip().strip('\'"')
            print(f"SecurityConfiguration: {security_config_value}")
            return security_config_value
        else:
            print("SecurityConfiguration not found in the file.")
            return None
 
directory = os.environ.get('DIRECTORY', '')
emr_tags_str = os.environ.get('EMR_TAGS', '')
 
emr_tags = json.loads(emr_tags_str)
 
dag_files = find_dag_files(directory)
 
empty_value_found = False
emr_tags_not_found = []
 
if not dag_files:
    print(f"::error::No DAG files found in the specified directory: {directory}")
 
for dag_file in dag_files:
    emr_tags_str_in_file = find_emr_tags_in_file(dag_file)
    combined_tags_str = ''.join(emr_tags_str_in_file)
    print(f"EMR Tags in {dag_file} (raw): {combined_tags_str}")
    emr_tags_in_file = json.loads(combined_tags_str) if combined_tags_str else []
    formatted_emr_tags = json.dumps(emr_tags_in_file, indent=2, ensure_ascii=False)
    print(f"EMR Tags in {dag_file}:\n{formatted_emr_tags}")
 
    tags = find_tags_in_file(dag_file)
    print(f"Tags in {dag_file}: {tags}")
 
    found_sec_conf = find_sec_conf_in_file(dag_file)
 
    if (not emr_tags_str_in_file or not any(emr_tags_in_file)) or (not tags or not any(tags)) or not found_sec_conf:
        empty_value_found = True
        break
 

    for emr_tag in emr_tags:
        if emr_tag not in emr_tags_in_file:
            emr_tags_not_found.append(emr_tag)
 
if empty_value_found or emr_tags_not_found:
    if empty_value_found:
        print("::error::At least one empty value found in Airflow DAG files.")
 
    if emr_tags_not_found:
        print(f"::error::EMR Tags not found in Airflow DAG files: {emr_tags_not_found}")
 
    exit(1)