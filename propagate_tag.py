#!/usr/bin/env python3
import datetime
from common import client, load_query

TABLE = '"1"."APP_BI"."PENTAGON"."DIM_USER"'
COLUMN = 'CITY'
TAG = 'city'  # tag must already exist
ATTACH_TAG = True  # True to set tag, False to unset

# --------------- find central table and get usage stats

table_data = client.execute(
    load_query('queries/table_by_path.gql'),
    {'path': TABLE}
)


table_uid = table_data['table']['uid']
column_name_to_uid = {
    c['prop']['name']: c['uid'] for c in table_data['table']['columns']
}
city_column_uid = column_name_to_uid[COLUMN]
print('table uid:', table_uid)
print('column uid:', city_column_uid)


# --------------- get lineage data

tags_data = client.execute(load_query('queries/get_tags.gql'))
tag_name_to_uid = {t['name']: t['uid'] for t in tags_data['tags']['items']}
tag_uid = tag_name_to_uid[TAG]
print('tag uid:', tag_uid)


lineage_data = client.execute(load_query('queries/lineage_1col_stripped.gql'), {
    'primaryUid': table_uid,
    'depthDownstream': 100,
    'depthUpstream': 100,
    'biPopularity': [0, 4],
    'biLastUsedDays': 90,
    'allowedList': [city_column_uid],
})
columns_to_ignore = set(column_name_to_uid.values()) - {city_column_uid}
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
    uid = col['uid']
    if (tag_uid in col['tagIds']) != ATTACH_TAG:
        columns_to_alter.append(uid)
    else:
        columns_noop_n += 1

print('Columns with no op required:', columns_noop_n)
print('Columns with tags to alter:', len(columns_to_alter))

# --------------- set/unset tags

attach_tags_data = client.execute(load_query('queries/attach_tags.gql'), {
    'tagUid': tag_uid,
    'objectUids': columns_to_alter,
    'attach': ATTACH_TAG,
})
