#!/usr/bin/env python3
import argparse
from collections import defaultdict
import datetime
from rich.console import Console
import sys
from typing import Tuple, List, Annotated

import networkx
import rich
from typer import Argument, Option

from common import (
    client, load_query, sql_quote_path, NotFoundError,
    execute_stored_query, QUERIES,
)

console = Console()


def get_linked_columns(
    data_source_id: int,
    table_path: Tuple[str, ...],
    columns: List[str],
    columns_by_tag: str,
):
    # First we need to get uids of the table and its columns
    table_data = execute_stored_query(
        QUERIES / 'table_by_path.gql',
        path=sql_quote_path((str(data_source_id),) + table_path),
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

    if columns_by_tag:
        column_uids = [
            column['uid']
            for column in table_data['table']['columns']
            if columns_by_tag in {
                tag['name'] for tag in column['tags']
            }
        ]
        if not column_uids:
            raise ValueError(
                f'No columns found for tag `{columns_by_tag}`.',
            )

    # Now let's get all downstreams
    lineage_data = execute_stored_query(
        QUERIES / 'lineage_analyze_impact.gql',
        primaryUid=table_uid,
        depthDownstream=1000,
        depthUpstream=0,
        popularity=[0, 4],
        biLastUsedDays=90,
        allowedList=column_uids,
    )

    edges = [
        (source, destination)
        for edge in lineage_data['lineage']['edges']
        if (
            # Filter out "off-chart" edges
            (source := edge['sourceUid'])
            and (destination := edge['destinationUid'])
        )
    ]

    if column_uids:
        graph = networkx.DiGraph()
        graph.add_edges_from(edges)

        connected_columns = {
            connected_uid
            for allowed_column in column_uids
            for edge in networkx.dfs_edges(graph, source=allowed_column)
            for connected_uid in edge
        }

    else:
        connected_columns = {
            connected_uid
            for edge in edges
            for connected_uid in edge
        }

    for tabular_entity in lineage_data['lineage']['entities']:
        name = tabular_entity.get(
            'prop', {},
        ).get('path') or tabular_entity.get('name') or '???'
        entity_type = tabular_entity['__typename']

        if tableau_project_name := tabular_entity.get('projectName'):
            name = f'[i]{tableau_project_name}[/i]/{name}'

        rich.print(f'[purple]{entity_type}[/purple] [green]{name}[/green]')

        for col in tabular_entity.get('columns', []):
            uid = col.get('uid')
            if uid is None:
                continue  # that's not a column

            if uid not in connected_columns:
                continue

            column_name = col.get('prop', {}).get('name') or col.get('name') or uid
            rich.print(f'  • {column_name}')
            if tags := col.get('tags', []):
                console.print(f'    └ [i]🏷️  Tags:[/i] ', end='')
                for tag in tags:
                    color = tag['color']
                    console.print(
                        tag['name'],
                        style=color, end='',
                    )
                    console.print()


def analyze_impact(
    data_source_id: Annotated[int, Argument(help='Data source ID.')],
    table_path: Annotated[str, Argument(
        help='Full table path, case sensitive. Format: DB.SCHEMA.TABLE',
    )],
    columns: Annotated[str, Option(help=(
        'Print downstreams only for these columns of the table in question. '
        'Format: `ID,COL1,ANOTHER_COL`. Case sensitive.'
    ))] = '',
    columns_by_tag: Annotated[str, Option(help=(
        'Print downstreams only for the columns of the primary table which are '
        'tagged with the specified tag.'
    ))] = '',
):
    """
    Print downstream dependencies of a given table.

    How to obtain Data Source ID:

    * Open https://app.datafold.com,

    * Go to **Settings** → Data Sources,

    * Find your Data Source; the leftmost column in the Data Sources list is ID.
    """
    if columns_by_tag and columns:
        raise ValueError(
            'Please specify only one of `--columns-by-tag` & `--columns`.',
        )


    try:
        get_linked_columns(
            data_source_id=data_source_id,
            table_path=tuple(table_path.split('.')),
            columns=[column.strip() for column in columns.split(',') if column],
            columns_by_tag=columns_by_tag,
        )
    except NotFoundError as e:
        print('ERROR:', e)
        print('Names are case sensitive')
        sys.exit(-1)
