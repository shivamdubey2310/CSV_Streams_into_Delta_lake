# Schema for schema_1 CSVs ("College_Placements_data")
import json

def return_schema():
    from pyspark.sql.types import IntegerType, FloatType, StructType, BooleanType, StringType
    schema = (StructType()
        .add("College_ID", StringType())
        .add("IQ", IntegerType())
        .add("Prev_Sem_Result", FloatType())
        .add("CGPA", FloatType())
        .add("Academic_Performance", FloatType())
        .add("Internship_Experience", StringType())
        .add("Extra_Curricular_Score", FloatType())
        .add("Communication_Skills", FloatType())
        .add("Projects_Completed", IntegerType())
        .add("Placement", StringType())
    )

    schema_json = schema.json()
    schema_dict = json.loads(schema_json)
    
    with open("schema_1.json", "w") as fileWriter:
        json.dump(schema_dict, fileWriter)

return_schema()