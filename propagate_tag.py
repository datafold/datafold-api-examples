#!/usr/bin/env python3
import argparse
import datetime
from common import client, load_query



def set_tag_on_column(data_source_id, database, schema, table, column, tag, set_tag):
    # --------------- find central table and get usage stats

    table_data = client.execute(
        load_query('queries/table_by_path.gql'),
        {'path': f'"{str(data_source_id)}"."{database}"."{schema}"."{table}"'}
    )


    table_uid = table_data['table']['uid']
    column_name_to_uid = {
        c['prop']['name']: c['uid'] for c in table_data['table']['columns']
    }
    column_uid = column_name_to_uid[column]
    print('table uid:', table_uid)
    print('column uid:', column_uid)


    # --------------- get lineage data

    tags_data = client.execute(load_query('queries/get_tags.gql'))
    tag_name_to_uid = {t['name']: t['uid'] for t in tags_data['tags']['items']}
    tag_uid = tag_name_to_uid[tag]
    print('tag uid:', tag_uid)


    lineage_data = client.execute(load_query('queries/lineage_1col_stripped.gql'), {
        'primaryUid': table_uid,
        'depthDownstream': 100,
        'depthUpstream': 100,
        'biPopularity': [0, 4],
        'biLastUsedDays': 90,
        'allowedList': [column_uid],
    })
    columns_to_ignore = set(column_name_to_uid.values()) - {column_uid}
    connected_columns = set()
    for e in lineage_data['lineage']['edges']:
        if e['sourceUid'] is None or e['destinationUid'] is None:
            continue
        connected_columns.add(e['sourceUid'])
        connected_columns.add(e['destinationUid'])


    tags_on_columns_data = client.execute(
        load_query('queries/get_columns.gql'),
        {'uids': list(connected_columns)}
    )
    columns_to_alter = []
    columns_noop_n = 0
    
    for col in tags_on_columns_data['columns']['items']:
        # some are None
        if col:
            uid = col['uid']
            col['tagIds']
            if (tag_uid in col['tagIds']) != set_tag:
                columns_to_alter.append(uid)
            else:
                columns_noop_n += 1

    print('Columns with no operation required:', columns_noop_n)
    print('Columns with operation completing:', len(columns_to_alter))

    # --------------- set/unset tags

    attach_tags_data = client.execute(load_query('queries/attach_tags.gql'), {
        'tagUid': tag_uid,
        'objectUids': columns_to_alter,
        'attach': set_tag,
    })

def main():
    parser = argparse.ArgumentParser(description='Analyze table usage by column')
    parser.add_argument('data_source_id', type=int)
    parser.add_argument('database', type=str,
                        help='database case-sensitive')
    parser.add_argument('schema', type=str,
                        help='schema case-sensitive')
    parser.add_argument('table', type=str,
                        help='table case-sensitive')
    parser.add_argument('column', type=str,
                        help='column case-sensitive')
    parser.add_argument('tag', type=str,
                        help='tag case-sensitive')
    parser.add_argument('--set-tag', action=argparse.BooleanOptionalAction)

    args = parser.parse_args()
    set_tag_on_column(args.data_source_id, args.database, args.schema, args.table, args.column, args.tag, args.set_tag)


if __name__ == '__main__':
    main()
