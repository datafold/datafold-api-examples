Useful examples for Datafold GraphQL API
----------------------------------------

## Preparation steps

Make sure you have Python ⩾ 3.10 installed.

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

## Try this out

Issue the following command to print available commands:

```shell
j
```

…and follow the white rabbit.

> For instance, `j analyze-impact` will help you analyze downstream impact of a table or its columns.

# Commands not wrapped under `j`

Might be slightly out of date.

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

## Get lineage
Gets and prints lineage for a table. Default depth is 1 upstream, 1 downstream and can be changed
with `-u` and `-d` optional arguments.

`./get_lineage.py 1234 DATABASE.SCHEMA.TABLENAME`
