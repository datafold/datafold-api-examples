Useful examples for Datafold GraphQL API
----------------------------------------

Preparation steps

    # Create and activate python virtual environment
    virtualenv .venv
    . .venv/bin/activate
    
    # Install dependencies
    pip install -r requirements.txt

    # Set env variable with Datafold key
    # You can get it here: https://app.datafold.com/users/me
    export DATAFOLD_API_KEY=<token-that-you-can-get-in-datafold-ui>


## Impact analysis

The script shows all downstream columns for a given table. You can also
trace a subset of columns of the table.

First argument is data source id. You can look it up by clicking on data source
here: https://app.datafold.com/data_sources and looking up its id in URL.

    ./analyze_impact.py 4233 DB.SCHEMA.TABLE_NAME COLUMN_NAME COLUMN_NAME2

