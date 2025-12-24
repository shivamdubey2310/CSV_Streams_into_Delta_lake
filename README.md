# CSV Streams into delta lake

A real-time data processing pipeline that automatically cleans CSV files and stores them in Delta Lake using Apache Spark Structured Streaming.

## 🚀 Features

- **Real-time Processing**: Automatically processes CSV files as they arrive
- **Data Cleaning**: Intelligent CSV cleaning and validation
- **Delta Lake Storage**: Stores processed data in Delta Lake format for ACID transactions
- **Spark Structured Streaming**: Leverages Apache Spark for scalable stream processing
- **Checkpointing**: Built-in fault tolerance and recovery capabilities

## 📁 Project Structure

```
CSV_Streams_into_Delta_lake/
├── Main.py                    # Main application entry point
├── README.md                  # Project documentation
├── requirements.txt           # Python dependencies
├── checkpointDir/            # Spark streaming checkpoints
│   ├── schema_1/             # Schema-specific checkpoint data
│   └── schema_2/
├── Delta_lake/               # Delta Lake storage
│   ├── schema_1/             # Partitioned Delta tables
│   └── schema_2/
├── Example/                  # Sample datasets for testing
│   ├── college_student_placement_dataset_*.csv
│   └── schema.py             # Example schema definition
└── Raw_CSVs/                 # Input directory for raw CSV files
    ├── schema_1/             # Schema 1 folder
    │   ├── schema.py         # Schema definition for this folder
    └── schema_2/             # Schema 2 folder
        ├── schema.py         # Schema definition for this folder
```

## 🛠️ Requirements

- Python 3.7+
- Apache Spark 3.0+
- Delta Lake
- PySpark

### Installation

1. **Clone the repository**
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## 📖 Usage

1. **Create a schema folder** in `Raw_CSVs/` (e.g., `Raw_CSVs/schema_1/`)
2. **Define your schema** by creating `schema.py` inside the schema folder
3. **Generate schema JSON** by running the schema.py file:
   ```bash
   cd Raw_CSVs/schema_1/
   python schema.py
   cd ../..
   ```
4. **Place CSV files** that follow the same schema in the same folder
5. **Run the application** (it will prompt for folder number):
   ```bash
   python Main.py
   ```
6. **Enter folder number** when prompted (e.g., `1` for schema_1)
7. **Access cleaned data** from the corresponding `Delta_lake/` directory

## 🔧 Configuration

### Schema Definition Structure

Each schema folder in `Raw_CSVs/` must contain:

1. **`schema.py`** - Defines the structure of CSV files in that folder _You can check `Example/schema.py` to know how to create `schema.py`_
2. **CSV files** - All files that follow the same schema

### Creating a New Schema

1. **Create a schema folder**:
   ```bash
   mkdir Raw_CSVs/schema_1
   ```

2. **Create `schema.py` inside the folder**:
   ```python
   # Raw_CSVs/schema_1/schema.py
   import json
   from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
   
   def return_schema():
       schema = (StructType()
           .add("column1", StringType())
           .add("column2", IntegerType())
           .add("column3", FloatType())
           # Add more columns as needed
       )
       
       # Save schema as JSON for Main.py to use
       schema_json = schema.json()
       schema_dict = json.loads(schema_json)
       
       with open("schema_1.json", "w") as fileWriter:
           json.dump(schema_dict, fileWriter)
   
   return_schema()
   ```

3. **Generate schema JSON file**:
   ```bash
   cd Raw_CSVs/schema_1/
   python schema.py
   cd ../..
   ```

4. **Place your CSV files** in the same folder as `schema.py`

### Schema Testing

To ensure your schema works correctly:

1. **Navigate to your schema folder**: `cd Raw_CSVs/schema_1/`
2. **Run the schema definition**: `python schema.py`
3. **Verify JSON generation**: Check that `schema_1.json` is created
4. **Place CSV files** in the same directory
5. **Test with main application**: 
   ```bash
   cd ../..
   python Main.py
   ```
   Enter `1` when prompted for folder number
6. **Verify results** in `Delta_lake/schema_1/`

**Note**: Main.py loads the schema from the generated JSON file (`schema_1.json`), not directly from `schema.py`. You must run `python schema.py` first to generate the JSON file.

## 📊 Data Flow

1. **Schema Setup**: Each `Raw_CSVs/schema_X/` folder contains a `schema.py` file
2. **Schema Generation**: User runs `python schema.py` to generate `schema_X.json`
3. **Ingestion**: CSV files are monitored in their respective schema folders
4. **Schema Loading**: Main.py loads the schema from the generated JSON file
5. **Processing**: Spark Structured Streaming applies schema-specific cleaning rules
6. **Storage**: Cleaned data is written to corresponding Delta Lake tables
7. **Checkpointing**: Progress is saved for fault tolerance

## 🚦 Getting Started

### Quick Setup

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Create your first schema folder**:
   ```bash
   mkdir Raw_CSVs/schema_1
   ```

3. **Copy the example schema**:
   ```bash
   cp Example/schema.py Raw_CSVs/schema_1/
   ```

4. **Modify the schema** in `Raw_CSVs/schema_1/schema.py` for your CSV structure

5. **Generate schema JSON file**:
   ```bash
   cd Raw_CSVs/schema_1/
   python schema.py
   cd ../..
   ```

6. **Place your CSV files** in `Raw_CSVs/schema_1/`

7. **Run the application**:
   ```bash
   python Main.py
   ```
   Enter `1` when prompted for the folder number

### Example Dataset

The project includes sample college student placement datasets in the `Example/` directory for testing:

- `college_student_placement_dataset_0.csv`
- `college_student_placement_dataset_1.csv` 
- `college_student_placement_dataset_2.csv`

### Testing with Sample Data

1. **Set up the schema folder**:
   ```bash
   mkdir -p Raw_CSVs/schema_1
   cp Example/schema.py Raw_CSVs/schema_1/
   ```

2. **Generate schema JSON**:
   ```bash
   cd Raw_CSVs/schema_1/
   python schema.py
   cd ../..
   ```

3. **Copy sample CSV files**:
   ```bash
   cp Example/college_student_placement_dataset_*.csv Raw_CSVs/schema_1/
   ```

4. **Execute the main application**:
   ```bash
   python Main.py
   ```
   Enter `1` when prompted for the folder number

5. **Check results** in `Delta_lake/schema_1/` for processed data

## 🔍 Monitoring

- **Spark UI**: Available at `http://localhost:4040` when running
- **Checkpoints**: Monitor progress in `checkpointDir/`
- **Delta Log**: Transaction logs in `Delta_lake/*//_delta_log/`
- **Logs**: Application logs are saved in `logs/schema_{folder_num}.log`