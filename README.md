# ETL with Spark RDD and Spark SQL on Big Dataset

## Table of Contents
- [Introduction](#introduction)
- [Dataset Description](#dataset-description)
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)

## Introduction
This project demonstrates an ETL (Extract, Transform, Load) pipeline using Apache Spark with RDD and Spark SQL to process a large dataset efficiently. The data is extracted, transformed for analytical purposes, and stored in a structured format for further analysis.

## Dataset Description
The dataset consists of event logs related to GitHub activities. Each record contains the following key attributes:

```json
{
    "id": "2615567681",
    "type": "ForkEvent",
    "actor": {
        "id": 1843574,
        "login": "jsonmurphy",
        "url": "https://api.github.com/users/jsonmurphy"
    },
    "repo": {
        "id": 7982472,
        "name": "clojure/tools.reader",
        "url": "https://api.github.com/repos/clojure/tools.reader"
    },
    "payload": {
        "forkee": {
            "id": 31503264,
            "name": "tools.reader",
            "full_name": "jsonmurphy/tools.reader",
            "owner": {
                "login": "jsonmurphy",
                "id": 1843574,
                "url": "https://api.github.com/users/jsonmurphy"
            },
            "html_url": "https://github.com/jsonmurphy/tools.reader",
            "description": "Clojure reader in Clojure"
        }
    },
    "public": true,
    "created_at": "2015-03-01T17:00:00Z"
}
```

This data will be processed to extract useful insights, such as the most active repositories and user engagement trends.

## Technologies Used
- **Python**: For scripting and orchestration.
- **Apache Spark**: To handle big data processing.
- **PySpark (RDD & Spark SQL)**: For data transformation and querying.
- **Virtual Environment**: To manage dependencies.

## Project Structure
```
project_root/
│── myenv/                   # Virtual environment for dependencies
│── rdd/                     # Spark RDD-based ETL scripts
│   ├── Action         
│   ├── Data    # Raw dataset files
│   ├── Key-Value
    | 
      # Save processed data
│── sparksql/                # Spark SQL-based ETL scripts
│   ├── sparkDataFrame.py         
│   ├── sparkSQL.py     
│   ├── sparkSqlWithBigdata.py  
│                
│── README.md                # Project documentation
```

## Setup Instructions
1. **Clone the Repository**
```sh
git clone https://github.com/yourusername/etl-with-sparkrdd-and-sparksql-on-bigdataset.git
cd etl-with-sparkrdd-and-sparksql-on-bigdataset
```

2. **Set Up Virtual Environment**
```sh
python -m venv myenv
source myenv/bin/activate   # On macOS/Linux
myenv\Scripts\activate      # On Windows
```

3. **Install Dependencies**
```sh
pip install -r requirements.txt
```

## Usage

### Run ETL with Spark RDD
```sh
python rdd/collect.py
python rdd/count.py
python rdd/...(smilar).py
```

### Run ETL with Spark SQL
```sh
python SparkSql/sparkDataFrame.py
python sparksql/sparkSql.py
python sparksql/sparkSqlWithBigdata.py
```

This project efficiently processes large datasets using Spark RDD and Spark SQL for scalable analytics.
