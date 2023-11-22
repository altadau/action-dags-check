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
        else:
            print("SecurityConfiguration not found in the file.")
         
dag_directory = "orchestration/dags"
 
dag_files = find_dag_files(dag_directory)
 
for dag_file in dag_files:
    emr_tags = find_emr_tags_in_file(dag_file)    
    formatted_emr_tags = json.dumps(emr_tags, indent=2, ensure_ascii=False)
    print(f"EMR Tags in {dag_file}:\n{formatted_emr_tags}")
    tags = find_tags_in_file(dag_file)
    print(f"Tags in {dag_file}: {tags}")
    find_sec_conf_in_file(dag_file)

if not find_emr_tags_in_file:     
    print("No EMR Tags found in any of the Airflow DAG files.")
if not find_tags_in_file:     
    print("No Airflow Tags found in any of the Airflow DAG files.")    
if not find_sec_conf_in_file:     
    print("SecurityConfiguration not found in any of the Airflow DAG files.")    
