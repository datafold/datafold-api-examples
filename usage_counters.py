#!/usr/bin/env python3
from collections import defaultdict
import datetime

from common import client, load_query

TABLE = '"1"."APP_BI"."PENTAGON"."DIM_USER"'

end = datetime.date.today()
start = end - datetime.timedelta(days=70)
def format_date(d):
    return f'{d.year}.{d.month}.{d.day}'

table_data = client.execute(
    load_query('queries/table_usage_counters.gql'),
    {'path': TABLE, 'statsStart': format_date(start), 'statsEnd': format_date(end)}
)
for col in sorted(table_data['table']['columns'], key=lambda x: x['prop']['number']):
    total_counts_by_user = defaultdict(lambda: 0)
    for stat_item in col['usageStats']:
        total_counts_by_user[stat_item['userName']] += stat_item['count']

    print(col['prop']['number'], col['prop']['name'],
          'total:', sum(total_counts_by_user.values()))
    for user, count in sorted(total_counts_by_user.items()):
        print(f'    "{user}": {count}')
