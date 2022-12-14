#!/usr/bin/env python3
import argparse
from collections import defaultdict

from common import client, load_query

def get_stats(data_source_id, database, schema, table):
    table_data = client.execute(
        load_query('queries/table_usage_counters.gql'),
        {'path': f'"{str(data_source_id)}"."{database}"."{schema}"."{table}"'}
    )
    for col in sorted(table_data['table']['columns'], key=lambda x: x['prop']['number']):
        total_counts_by_user = defaultdict(lambda: 0)
        for stat_item in col['usageStats']:
            total_counts_by_user[stat_item['userName']] += stat_item['count']

        print(col['prop']['number'], col['prop']['name'],
              'total:', sum(total_counts_by_user.values()))
        for user, count in sorted(total_counts_by_user.items()):
            print(f'    "{user}": {count}')

def main():
    parser = argparse.ArgumentParser(description='Analyze table usage by column')
    parser.add_argument('data_source_id', type=int)
    parser.add_argument('database', type=str,
                        help='case-sensitive')
    parser.add_argument('schema', type=str,
                        help='case-sensitive')
    parser.add_argument('table', type=str,
                        help='case-sensitive')

    args = parser.parse_args()

    get_stats(args.data_source_id, args.database, args.schema, args.table)


if __name__ == '__main__':
    main()
