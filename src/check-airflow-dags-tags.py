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
 
dag_directory = os.environ.get('DIRECTORY', '')
dag_files = find_dag_files(dag_directory)
 
found_values = []
 
input_emr_tags = [ 
    {"Key": "ssmmanaged", "Value": "no see CSRC_DBC_933_EC2_SSM_MANAGED"},
    {"Key": "CSRC_DBC_933", "Value": "CSRC_DBC_933_EC2_SSM_MANAGED"}
]

formatted_input_json = json.dumps(input_emr_tags, separators=(',', ': '), indent=4)
print(f"Required EMR Tags: {formatted_input_json}")

emr_tags_data = {}
 
for dag_file in dag_files:
    emr_tags = find_emr_tags_in_file(dag_file)
    combined_tags_str = ''.join(emr_tags)
    combined_tags_str = "[{}]".format(combined_tags_str.replace("'", "\""))
    print(f"EMR Tags in {dag_file}: {combined_tags_str}")

    list_var1 = json.loads(formatted_input_json)
    list_var2 = json.loads(combined_tags_str)

    if all(item in list_var2 for item in list_var1):
        print("Required tags found")
    else:
        print("Required tags not found")
        exit(1)

 
    tags = find_tags_in_file(dag_file)
    print(f"Tags in {dag_file}: {tags}")
 
    found_sec_conf = find_sec_conf_in_file(dag_file)
 
    found_values.extend(filter(None, [emr_tags, tags, found_sec_conf]))
 
if not dag_files:
    print("::error::No DAG files found in the specified directory.")
    exit(1)
 
if not any(found_values):
    print("::error::No non-empty values found in any of the Airflow DAG files.")
    exit(1)