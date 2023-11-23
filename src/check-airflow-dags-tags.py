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
 
with open('inputsemrtags.txt', 'r') as file:
    input_emr_tags_str = file.read()
    print(f"Contents of inputsemrtags.txt: {input_emr_tags_str}")
    
try:
    input_emr_tags = json.loads(input_emr_tags_str)
except json.decoder.JSONDecodeError as e:
    print(f"Error decoding JSON from inputsemrtags.txt: {e}")
    input_emr_tags = []
 
directory = os.environ.get('DIRECTORY', '')
dag_files = find_dag_files(directory)
empty_value_found = False
emr_tags_not_found = []
 
if not dag_files:
    print(f"::error::No DAG files found in the specified directory: {directory}")
 
with open('airflowemrtags.txt', 'w') as airflow_emr_tags_file:
    json.dump([], airflow_emr_tags_file, indent=2)
 
for dag_file in dag_files:
    emr_tags_str_in_file = find_emr_tags_in_file(dag_file)
    combined_tags_str = ''.join(emr_tags_str_in_file)
    combined_tags_str = combined_tags_str.replace("'", "\"")
    print(f"EMR Tags in {dag_file} (raw): {combined_tags_str}")
    emr_tags_in_file = combined_tags_str if combined_tags_str else []
    print(f"EMR Tags in {dag_file}: {emr_tags_in_file}")
    formatted_emr_tags = json.dumps(emr_tags_in_file, indent=2, ensure_ascii=False)
    print(f"EMR Tags in {dag_file}:\n{formatted_emr_tags}")
 
    if input_emr_tags and set(input_emr_tags).difference(emr_tags_in_file):
        emr_tags_not_found.extend(set(input_emr_tags).difference(emr_tags_in_file))
 
with open('inputsemrtags.txt', 'w') as file:
    json.dump(input_emr_tags, file, indent=2)
 
with open('airflowemrtags.txt', 'r') as file:
    airflow_emr_tags_str = file.read()
 
try:
    airflow_emr_tags = json.loads(airflow_emr_tags_str)
except json.decoder.JSONDecodeError as e:
    print(f"Error decoding JSON from airflowemrtags.txt: {e}")
    airflow_emr_tags = []
 
if input_emr_tags and set(input_emr_tags).difference(airflow_emr_tags):
    emr_tags_not_found.extend(set(input_emr_tags).difference(airflow_emr_tags))
 
if empty_value_found or emr_tags_not_found:
    if empty_value_found:
        print("::error::At least one empty value found in Airflow DAG files.")
 
    if emr_tags_not_found:
        print(f"::error::EMR Tags not found in Airflow DAG files: {emr_tags_not_found}")
 
    exit(1)