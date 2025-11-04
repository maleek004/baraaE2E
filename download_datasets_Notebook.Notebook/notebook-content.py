# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3a15daa0-1517-4964-82d7-ea00507b8389",
# META       "default_lakehouse_name": "Baraa_LH",
# META       "default_lakehouse_workspace_id": "9bb22313-ca99-4235-bcf0-16cf2704e122",
# META       "known_lakehouses": [
# META         {
# META           "id": "3a15daa0-1517-4964-82d7-ea00507b8389"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%pip install semantic-link==0.11.1 semantic-link-labs==0.11.2
import pandas as pd
import requests 
import sempy_labs as labs
import sempy.fabric as semfabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

lakehouse_name = 'Baraa_LH6'
crm_data_relative_path = "datasets/source_crm"
erp_data_relative_path = "datasets/source_erp"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

crm_dataset_urls = {"cust_info.csv":"https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets/source_crm/cust_info.csv"
                    ,"prd_info.csv":"https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets/source_crm/prd_info.csv"
                    ,"sales_details.csv":"https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets/source_crm/sales_details.csv"}

erp_dataset_urls = {"CUST_AZ12.csv":"https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets/source_erp/CUST_AZ12.csv"
                    ,"LOC_A101.csv":"https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets/source_erp/LOC_A101.csv"
                    ,"PX_CAT_G1V2.csv":"https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets/source_erp/PX_CAT_G1V2.csv"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# creating a new lakehouse where our data will be downladed will be downloaded into and will alsoe be used as the siver layer

lakehouse = notebookutils.lakehouse.create(lakehouse_name)    
lakehouse_id = lakehouse['id']
workspace_id = lakehouse['workspaceId']
abfs_path = lakehouse.get('properties',{}).get('abfsPath')
abfs_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

warehouse = notebookutils.warehouse.create('WH1')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# creating directories in our destination lakehouse (that this notebook is not attached to, hence the use of abfs_path) 
notebookutils.fs.mkdirs(f'{abfs_path}/Files/{crm_data_relative_path}')
notebookutils.fs.mkdirs(f'{abfs_path}/Files/{erp_data_relative_path}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

##  mount the Files section of the new lakehouse 
files_directory = abfs_path + '/Files'
mount_point = "/mnt/lakehouse/" + lakehouse_name + "/Files"
notebookutils.fs.mount(files_directory, mount_point)
base_dir_local_path = notebookutils.fs.getMountPath(mount_point)
base_dir_local_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

crm_data_full_local_path = f'{base_dir_local_path}/{crm_data_relative_path}'
erp_data_full_local_path = f'{base_dir_local_path}/{erp_data_relative_path}'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


def download_datasets(dataset_urls, save_dir="."):
    for filename, url in dataset_urls.items():
        print(f"Downloading {filename}...")
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()  # Raise exception for HTTP errors
            with open(f"{save_dir}/{filename}", "wb") as f:
                f.write(response.content)
            print(f"✅ {filename} downloaded successfully.")
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to download {filename}: {e}")


download_datasets(crm_dataset_urls, crm_data_full_local_path)
download_datasets(erp_dataset_urls, erp_data_full_local_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

silver_notebook_url = 'https://raw.githubusercontent.com/maleek004/auto-fabric-setup-baraa/refs/heads/main/Create_silver_layer_Notebook.ipynb'
silver_notebook_name = 'silver_layer_processing_notebook'

def import_notebook(notebook_import_name, githuburl, workspace_id, lakehouse_name) -> str:
    #import notebook and return notebookid
    result = labs.import_notebook_from_web(notebook_name = notebook_import_name, url = githuburl)        
        
    #update the default lakehouse        
    notebookutils.notebook.updateDefinition(name = notebook_import_name, workspaceId = workspace_id, defaultLakehouse = lakehouse_name, defaultLakehouseWorkspace= workspace_id)
    
    notebook_id = semfabric.resolve_item_id(item_name = notebook_import_name, type = "Notebook")
    print(f"notebookname: {notebook_import_name}, notebook_id: {notebook_id}")
    return notebook_id


#import notebooks and get Notebook Ids for all 3 notebooks to be used in subsequent steps
silver_layer_notebook_id = import_notebook(silver_notebook_name, silver_notebook_url, workspace_id, lakehouse_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#notebookutils.notebook.run(silver_notebook_name,90,{'useRootDefaultLakehouse': True})
notebookutils.notebook.run('silver_layer_processing_notebook',90,{'useRootDefaultLakehouse': True})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
