1. In a subfolder ml_jobs, create modular production grade code based on the train_deploy_monitor_ML_in_snowflake notebook. 
    - train.py
        - Script that will be run as an mljob on SPCS
        - Contains the following steps from the notebook:
            - dataset generation from the feature view
            - distributed HPO training
            - experiment tracking
            - log best model to the registry and set as default
    - infer.py
        - Script that will be run as an mljob on SPCS
        - Contains the following steps from the notebook:
            - Run inference on default model from registry
            - Save to inference table
            - Create model monitoring table and model monitor if not exists
    - utils.py 
        - Any shared utilities between the scripts

2. In the main directory, create modular, production grade code
    - orchestration.sql 
        - uploads ml_jobs directory to a stage subdirectory
        - creates 2 task dags (no schedule) that submit train.py and infer.py as mljobs using submit_from_stage

