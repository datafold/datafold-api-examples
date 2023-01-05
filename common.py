from typing import Iterable
import os
import sys

from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

token = os.environ.get('DATAFOLD_API_KEY')
if token is None:
    print('Please set DATAFOLD_API_KEY environment variable')
    sys.exit(-1)

host = os.environ.get('DATAFOLD_HOST', 'https://app.datafold.com')

transport = AIOHTTPTransport(
    url=host + '/api/graphql',
    headers={'Authorization': 'Key ' + token},
    # headers={'Authorization': 'Key gVozB2TueGb8V20ckaj9jr8PRdNgIozHjaEsYnjD'}
)

client = Client(
    transport=transport,
    fetch_schema_from_transport=True,
    execute_timeout=20 * 60,
)


def load_query(filename):
    with open(filename) as f:
        return gql(f.read())


def sql_quote_path(path: Iterable[str]) -> str:
    """Quote table path in SQL-99 compliant way."""
    quoted = ['"{}"'.format(x.replace('"', '""')) for x in path]
    return ".".join(quoted)


class NotFoundError(Exception):
    pass
