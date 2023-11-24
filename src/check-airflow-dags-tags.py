import os
import re
import json
import sys
 
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
        print(f"Content of {file_path}: {content}")
        tags_pattern = re.compile(r'["\'](?:Tags|tags)["\']\s*:\s*\[([^]]*)\]', re.MULTILINE)
        matches = tags_pattern.findall(content)
        return json.loads(matches[0]) if matches else []
 
def find_security_configuration(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        security_pattern = re.compile(r'["\']SecurityConfiguration["\']\s*:\s*["\'](.*?)["\']', re.MULTILINE)
        matches = security_pattern.findall(content)
        return matches[0] if matches else None
 
directory = os.environ.get('DIRECTORY', '')
dag_files = find_dag_files(directory)
empty_value_found = False
emr_tags_not_found = []
 
security_configurations = set()
airflow_emr_tags = set()
 
if not dag_files:
    print(f"::error::No DAG files found in the specified directory: {directory}")
 
for dag_file in dag_files:
    emr_tags_in_file = find_emr_tags_in_file(dag_file)
    security_configuration = find_security_configuration(dag_file)
 
    if security_configuration:
        security_configurations.add(security_configuration)
 
    print(f"EMR Tags in {dag_file} (raw): {emr_tags_in_file}")
    print(f"EMR Tags in {dag_file}: {json.dumps(emr_tags_in_file, indent=2)}")
    print(f"Security Configuration in {dag_file}: {security_configuration}")
 
    if emr_tags_in_file:
        with open('airflowemrtags.txt', 'w') as airflow_emr_tags_file:
            json.dump(emr_tags_in_file, airflow_emr_tags_file, indent=2)
 
        airflow_emr_tags.update(set(emr_tags_in_file))
 
if empty_value_found or emr_tags_not_found:
    if empty_value_found:
        print("::error::At least one empty value found in Airflow DAG files.")
 
    if emr_tags_not_found:
        print(f"::error::EMR Tags not found in Airflow DAG files: {emr_tags_not_found}")
 
    sys.exit(1)
 
input_emr_tags = [
    {"Key": "ssmmanaged", "Value": "no see CSRC_DBC_933_EC2_SSM_MANAGED"},
    {"Key": "CSRC_DBC_933", "Value": "CSRC_DBC_933_EC2_SSM_MANAGED"}
]
 
missing_tags = [tag for tag in input_emr_tags if tag not in airflow_emr_tags]
 
print(f"Security Configurations found: {security_configurations}")
print(f"Missing EMR Tags in Airflow DAG: {missing_tags}")