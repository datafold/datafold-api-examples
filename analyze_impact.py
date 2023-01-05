#!/usr/bin/env python3
import argparse
from collections import defaultdict
import datetime
import sys
from typing import Tuple, List

from common import client, load_query, sql_quote_path, NotFoundError



def get_linked_columns(
        data_source_id: int,
        table_path: Tuple[str, ...],
        columns: List[str]):

    # First we need to get uids of the table and its columns
    table_data = client.execute(
        load_query('queries/table_by_path.gql'),
        {'path': sql_quote_path((str(data_source_id),) + table_path)}
    )
    if table_data['table'] is None:
        raise NotFoundError('Table "%s" not found in data source #%d' % (
            '.'.join(table_path),
            data_source_id,
        ))
    table_uid = table_data['table']['uid']
    column_name_to_uid = {
        c['prop']['name']: c['uid'] for c in table_data['table']['columns']
    }

    column_uids = []
    for c in columns:
        uid = column_name_to_uid.get(c)
        if uid is None:
            raise NotFoundError('Column %s is not found' % c)
        column_uids.append(uid)

    # print('table uid:', table_uid)
    # print('column uid:', column_uids)

    # Now let's get all downstreams
    lineage_data = client.execute(load_query('queries/lineage_analyze_impact.gql'), {
        'primaryUid': table_uid,
        'depthDownstream': 1000,
        'depthUpstream': 0,
        'popularity': [0, 4],
        'biLastUsedDays': 90,
        'allowedList': column_uids,
    })

    columns_to_ignore = set(column_name_to_uid.values()) - set(column_uids)
    connected_columns = set()
    for edge in lineage_data['lineage']['edges']:
        if edge['sourceUid'] is None or edge['destinationUid'] is None:
            continue # this is an "off-chart" edge

        connected_columns.add(edge['sourceUid'])
        connected_columns.add(edge['destinationUid'])

    columns_per_table = defaultdict(set)
    for col in lineage_data['lineage']['entities']:
        uid = col.get('uid')
        if uid is None:
            continue  # that's not a column

        if uid not in connected_columns:
            continue

        table = col['table']['prop']['path']
        columns_per_table[table].add(col['prop']['name'])

    return columns_per_table


def main():
    parser = argparse.ArgumentParser(description='Analyze downstream dependencies')
    parser.add_argument('data_source_id', type=int)
    parser.add_argument('full_table_name', type=str,
                        help='db.schema.name format, case-sensitive')
    parser.add_argument('columns', metavar='column', type=str, nargs='*',
                        help='column name to trace; ommit to trace all columns')

    args = parser.parse_args()

    # fast and dirty, doesn't handle quotes
    table_path = tuple(args.full_table_name.split('.'))

    try:
        columns_per_table = get_linked_columns(
            args.data_source_id,
            table_path,
            args.columns,
        )
    except NotFoundError as e:
        print('ERROR:', e)
        print('Names are case sensitive')
        sys.exit(-1)

    for table, column_set in sorted(columns_per_table.items()):
        for column in sorted(column_set):
            # that's a hacky and incorrect way to unquote
            unquoted_table = table.replace('"', '')
            print(unquoted_table, column)

if __name__ == '__main__':
    main()
