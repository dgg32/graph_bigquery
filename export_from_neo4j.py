
import yaml
import json
import os

from neo4j import GraphDatabase

with open("config.yaml", "r") as stream:
    try:
        PARAM = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)


driver = GraphDatabase.driver(PARAM["neo4j_url"], auth=(PARAM["neo4j_username"], PARAM["neo4j_password"]))

records, summary, keys = driver.execute_query(f"""
    CALL apoc.meta.schema()
    YIELD value RETURN value;
    """,
    database_="neo4j",
)
# Loop through results and do something with them
for record in records:
    schema = record.data()["value"]
    json_schema = json.dumps(record.data()["value"])
    #print (json_schema)

variable_name = "t"
output_directory = "tsv"

for node_type in schema.keys():
    if schema[node_type]['type'] == 'node':

        records, summary, keys = driver.execute_query(f"""
            MATCH ({variable_name}:{node_type})
            RETURN {variable_name}
            """,
            database_="neo4j",
        )
        
        header = list(schema[node_type]['properties'].keys())
        content = "\t".join(header) + "\n"
        for record in records:
            result = record.data()[f"{variable_name}"]
            
            for h in header:
                if h in result:
                    content += str(result[h]) + "\t"
                else:
                    content += "\t"
            content = content[:-1] + "\n"

        with open(os.path.join(output_directory, f"{node_type}.tsv"), 'w') as f:
            f.write(content)
