#!/usr/bin/env python3
import argparse
from collections import defaultdict
import datetime
import sys
from typing import Tuple, List

from common import client, load_query, sql_quote_path, NotFoundError



def get_links(
        data_source_id: int,
        table_path: Tuple[str, ...],
        upstream_depth: int,
        downstream_depth: int
):
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

    # print('table uid:', table_uid)
    # print('column uid:', column_uids)

    # Now let's get a chunk of lineage
    lineage_data = client.execute(load_query('queries/get_lineage.gql'), {
        'primaryUid': table_uid,
        'depthDownstream': downstream_depth,
        'depthUpstream': upstream_depth,
    })

    edge_uid_pairs = []
    for edge in lineage_data['lineage']['edges']:
        if edge['sourceUid'] is None or edge['destinationUid'] is None:
            continue  # this is an "off-chart" edge

        edge_uid_pairs.append((edge['sourceUid'], edge['destinationUid']))


    uid_to_path = {}
    for obj in lineage_data['lineage']['entities']:
        if obj['__typename'] == 'Table':
            uid = obj.get('uid')
            table_path = obj['prop']['path']
            uid_to_path[uid] = (table_path, None)

        elif obj['__typename'] == 'Column':
            uid = obj.get('uid')
            if uid is None:
                continue  # that's not a column

            column_name = obj['prop']['name']
            table_path = obj['table']['prop']['path']
            uid_to_path[uid] = (table_path, column_name)

    return [
        (uid_to_path[src], uid_to_path[dst])
        for src, dst in edge_uid_pairs
    ]


def main():
    parser = argparse.ArgumentParser(description='Analyze downstream dependencies')
    parser.add_argument('-u', '--upstream-depth', type=int, default=1)
    parser.add_argument('-d', '--downstream-depth', type=int, default=1)
    parser.add_argument('data_source_id', type=int)
    parser.add_argument('full_table_name', type=str,
                        help='db.schema.name format, case-sensitive')

    args = parser.parse_args()

    # fast and dirty, doesn't handle quotes
    table_path = tuple(args.full_table_name.split('.'))

    try:
        links = get_links(
            args.data_source_id,
            table_path,
            args.upstream_depth,
            args.downstream_depth,
        )
    except NotFoundError as e:
        print('ERROR:', e)
        print('Names are case sensitive')
        sys.exit(-1)

    def format_path(path):
        table, column = path

        # A quick and dirty way to strip double quotes that you probably don't
        # want to see. If you tables somehow contain " in their names, you'd
        # need more advanced dequoting.
        formatted_table = table.replace('"', '')
        if column is None:
            return formatted_table
        else:
            return formatted_table + ' ' + column

    for src_path, dst_path in links:
        print(format_path(src_path), '->', format_path(dst_path))

if __name__ == '__main__':
    main()
