Useful examples for Datafold GraphQL API
----------------------------------------

Preparation steps

    # Create and activate python virtual environment
    virtualenv .venv
    . .venv/bin/activate
    
    # Install dependencies
    pip install -r requirements.txt

    # If you are using a single-tenant install, set its base URL
    export DATAFOLD_HOST=https://app.datafold.com

    # Set env variable with Datafold key
    # You can get it here: https://app.datafold.com/users/me
    export DATAFOLD_API_KEY=<token-that-you-can-get-in-datafold-ui>



## Impact analysis

The script shows all downstream columns for a given table. You can also
trace a subset of columns of the table.

First argument is data source id. You can look it up by clicking on data source
here: https://app.datafold.com/data_sources and looking up its id in URL.

`./analyze_impact.py 4233 DB.SCHEMA.TABLE_NAME COLUMN_NAME COLUMN_NAME2`

## Usage counters
Shows how to use column-level access counters. It prints out all columns of a table and number of times they were accessed by different users.

`./usage_counters.py 1234 DATABASE SCHEMA TABLE`

1234 here is the datasource_id, this is found in the URL on the catalog. Ex: /catalog/profile/table/1234/

## Propogate tag
Propagates a tag through connected columns. One of the main usecases is to propagate PII tags to all downstream tables.

To add tags:
`./propagate_tag.py 1234 DATABASE SCHEMA TABLE tagName --set-tag`

To remove tags:
`./propagate_tag.py 1234 DATABASE SCHEMA TABLE tagName --no-set-tag`
