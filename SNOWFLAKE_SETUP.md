# Snowflake Setup Guide

This guide details the steps required to configure Snowflake and use the Python packages synced by the pipeline.

## 1. Create a Stage

You need an internal stage to store the zipped package files.

```sql
CREATE DATABASE IF NOT EXISTS MY_DB;
CREATE SCHEMA IF NOT EXISTS MY_DB.MY_SCHEMA;

CREATE OR REPLACE STAGE MY_DB.MY_SCHEMA.PACKAGES_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = 'NONE' RECORD_DELIMITER = 'NONE')
    COPY_OPTIONS = (ON_ERROR = 'SKIP_FILE');
```

## 2. Configure the Pipeline

Ensure your Azure DevOps pipeline or local environment has the following variables set to target this stage:

- `SNOWFLAKE_DATABASE`: `MY_DB`
- `SNOWFLAKE_SCHEMA`: `MY_SCHEMA`
- `SNOWFLAKE_STAGE`: `@MY_DB.MY_SCHEMA.PACKAGES_STAGE`

## 3. Using the Packages in a UDF

Once the pipeline runs, it will upload a zip file (e.g., `app_packages.zip`) to the stage. You can import this zip file into your Python UDFs.

### Example UDF

```sql
CREATE OR REPLACE FUNCTION MY_DB.MY_SCHEMA.TEST_FUNC()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = ('@MY_DB.MY_SCHEMA.PACKAGES_STAGE/app_packages.zip')
HANDLER = 'main.handler'
AS
$$
import sys
import os

# Add the zip file to sys.path to import modules from it
import_dir = sys._xoptions.get("snowflake_import_directory")
sys.path.append(os.path.join(import_dir, "app_packages.zip"))

# Now you can import your synced packages
# import my_custom_package 

def handler():
    return "Packages imported successfully!"
$$;
```

## 4. Troubleshooting

- **Import Errors**: Ensure the `sys.path` modification is correct. The zip file name in `sys.path.append` must match the name in the `IMPORTS` clause.
- **Platform Issues**: The pipeline installs packages for `manylinux2014_x86_64`. If you see architecture mismatch errors, ensure your packages are compatible with this platform.
