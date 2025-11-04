# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%pip install semantic-link==0.12.1 semantic-link-labs==0.9.10
import sempy_labs as labs



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

notebook_import_name = 'migrateToDirectLake'
githuburl = 'https://github.com/microsoft/semantic-link-labs/blob/main/notebooks/Migration%20to%20Direct%20Lake.ipynb'
def import_notebook(notebook_import_name, githuburl) -> str:
    #import notebook and return notebookid
    
    #if item_exists(notebook_import_name, "Notebook"):
        #print(f'{notebook_import_name} already exists so skipping import')
    #else:
        print(f'{notebook_import_name} does not exist so importing from {githuburl}')
        result = labs.import_notebook_from_web(notebook_name = notebook_import_name, url = githuburl)        
        
        #update the default lakehouse        
        #notebookutils.notebook.updateDefinition(name = notebook_import_name, workspaceId = workspace_id, defaultLakehouse = lakehouse_name, defaultLakehouseWorkspace= workspace_id)
    
    #notebook_id = semfabric.resolve_item_id(item_name = notebook_import_name, type = "Notebook")
        print(f"notebookname: {notebook_import_name} imported successfully")#, notebook_id: {notebook_id}")

import_notebook(notebook_import_name, githuburl)  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
