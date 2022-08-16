#!/usr/bin/env python3
import argparse
from collections import defaultdict
import datetime
import sys
from typing import Tuple, List

from common import client, load_query, sql_quote_path, NotFoundError



def print_table_data(data_source_id: int, table_path: Tuple[str, ...]):
    table_data = client.execute(
        load_query('queries/get_modified_at_for_table.gql'),
        {'path': sql_quote_path((str(data_source_id),) + table_path)}
    )
    if table_data['table'] is None:
        raise NotFoundError('Table "%s" not found in data source #%d' % (
            '.'.join(table_path),
            data_source_id,
        ))
    data = table_data['table']
    print('.'.join(table_path), data['lastModifiedAt'])


def main():
    parser = argparse.ArgumentParser(description='Analyze downstream dependencies')
    parser.add_argument('data_source_id', type=int)
    parser.add_argument('full_table_names', type=str, nargs='*',
                        help='db.schema.name format, case-sensitive')

    args = parser.parse_args()

    # fast and dirty, doesn't handle quotes
    table_paths = [tuple(path.split('.')) for path in args.full_table_names]

    for path in table_paths:
        print_table_data(args.data_source_id, path)


if __name__ == '__main__':
    main()
