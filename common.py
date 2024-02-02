import os
import sys
from pathlib import Path
from typing import Iterable

import funcy
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportServerError

QUERIES = Path(__file__).parent / 'queries'


env_file = Path(__file__).parent / '.env'
if env_file.is_file():
    lines = env_file.read_text().splitlines()
    for line in lines:
        name, value = line.split('=')
        os.environ[name] = value

token = os.environ.get('DATAFOLD_API_KEY')
if token is None:
    print('Please set DATAFOLD_API_KEY environment variable.\n')
    print(
        'You can do that by writing it into `.env` file '
        'in the root directory of this project. It will be .gitignore-d.'
    )
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


@funcy.retry(errors=TransportServerError, tries=3, timeout=1)
def execute_stored_query(
    file_path: Path,
    **variables: str | int | list[int] | None,
):
    return client.execute(
        gql(file_path.read_text()),
        variables,
    )


def sql_quote_path(path: Iterable[str]) -> str:
    """Quote table path in SQL-99 compliant way."""
    quoted = ['"{}"'.format(x.replace('"', '""')) for x in path]
    return ".".join(quoted)


class NotFoundError(Exception):
    pass
