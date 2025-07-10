# Schema for schema_1 CSVs ("College_Placements_data")
# Note: When creating your own schema, name the JSON file as schema_{folder_number}.json
# For example: schema_1.json for schema_1 folder, schema_2.json for schema_2 folder

import json

def return_schema():
    from pyspark.sql.types import IntegerType, FloatType, StructType, BooleanType, StringType
    
    # Describe your schema here, example given below
    
    # schema = (StructType()
    #     .add("College_ID", StringType())
    #     .add("IQ", IntegerType())
    #     .add("Prev_Sem_Result", FloatType())
    #     .add("CGPA", FloatType())
    #     .add("Academic_Performance", FloatType())
    #     .add("Internship_Experience", StringType())
    #     .add("Extra_Curricular_Score", FloatType())
    #     .add("Communication_Skills", FloatType())
    #     .add("Projects_Completed", IntegerType())
    #     .add("Placement", StringType())
    # )

    schema_json = schema.json()
    schema_dict = json.loads(schema_json)
    
    with open("schema_1.json", "w") as fileWriter:
        json.dump(schema_dict, fileWriter)

return_schema()