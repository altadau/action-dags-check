import os
import re
import json
import argparse
 
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
 
parser = argparse.ArgumentParser(description='Check Airflow DAG files for empty values.')
parser.add_argument('--directory', required=True, help='Path to the directory containing DAG files')
parser.add_argument('--emr_tags', nargs='+', help='List of EMR Tags to check for')
 
args = parser.parse_args()
 
dag_directory = args.directory
dag_files = find_dag_files(dag_directory)
 
empty_value_found = False
 
if not dag_files:
    print(f"::error::No DAG files found in the specified directory: {dag_directory}")
 
for dag_file in dag_files:
    emr_tags_in_file = find_emr_tags_in_file(dag_file)
    formatted_emr_tags = json.dumps(emr_tags_in_file, indent=2, ensure_ascii=False)
    print(f"EMR Tags in {dag_file}:\n{formatted_emr_tags}")
 
    tags = find_tags_in_file(dag_file)
    print(f"Tags in {dag_file}: {tags}")
 
    found_sec_conf = find_sec_conf_in_file(dag_file)
     
    if args.emr_tags and set(args.emr_tags).intersection(emr_tags_in_file):
        print("::error::EMR Tags in the file do not match the provided values.")
        empty_value_found = True
        break
 
    if (not tags or not any(tags)) or not found_sec_conf:
        empty_value_found = True
        break
 
if empty_value_found:
    print("::error::At least one empty value found in Airflow DAG files.")
    exit(1)