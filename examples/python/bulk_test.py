import argparse
import json
import os
from base64 import b64decode
import pandas as pd

from acrawriter import create_acrastruct
from sqlalchemy import (Table, Column, Integer, MetaData, create_engine, Binary, Text)
from urllib.request import urlopen


def get_zone():
    """make http response to AcraServer API to generate new zone and return tuple
    of zone id and public key
    """
    response = urlopen('{}/getNewZone'.format(ACRA_CONNECTOR_API_ADDRESS))
    json_data = response.read().decode('utf-8')
    zone_data = json.loads(json_data)
    return zone_data['id'], b64decode(zone_data['public_key'])


def get_default(name, value):
    """return value from environment variables with name EXAMPLE_<name>
    or value"""
    return os.environ.get('EXAMPLE_{}'.format(name.upper()), value)


def write_data(data, connection):
    zone_id, key = get_zone()
    print("data: {}\nzone: {}".format(data, zone_id))

    # here we encrypt our data and wrap into AcraStruct
    encrypted_data = create_acrastruct(
        data.encode('utf-8'), key, zone_id.encode('utf-8'))

    connection.execute(
        test_table.insert(), data=encrypted_data,
        zone_id=zone_id.encode('utf-8'),
        raw_data=data)


def setup_df(table, n=1000000):
    zone_id, key = get_zone()
    print("data: {}\nzone: {}".format(data, zone_id))
    return pd.concat([pd.DataFrame([i, zone_id
                , create_acrastruct(('raw_'+i).encode('utf-8'), key, zone_id.encode('utf-8')), 'raw_'+i]
                , columns=table.__table__.columns) for i in range(n)], ignore_index = True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db_name', type=str,
                        default=get_default('db_name', 'test'),
                        help='Database name')
    parser.add_argument('--db_user', type=str,
                        default=get_default('db_user','test'),
                        help='Database user')
    parser.add_argument('--db_password', type=str,
                        default=get_default('db_password', 'test'),
                        help='Database user\'s password')
    parser.add_argument('--port', type=int,
                        default=get_default('port', 9494),
                        help='Port of database or AcraConnector')
    parser.add_argument('--host', type=str,
                        default=get_default('host', 'localhost'),
                        help='Host of database or AcraConnector')
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        default=get_default('verbose', False), help='verbose')
    args = parser.parse_args()

    ACRA_CONNECTOR_API_ADDRESS = get_default('acra_connector_api_address', 'http://127.0.0.1:9191')
    # default driver
    driver = 'postgresql'

    metadata = MetaData()
    test_table = Table(
        'test_bulk_insert', metadata,
        Column('id', Integer, primary_key=True, nullable=False),
        Column('zone_id', Binary, nullable=True),
        Column('data', Binary, nullable=False),
        Column('raw_data', Text, nullable=False),
    )
    engine = create_engine(
        '{}://{}:{}@{}:{}/{}'.format(
            driver, args.db_user, args.db_password, args.host, args.port,
            args.db_name),
        echo=bool(args.verbose))
    connection = engine.connect()
    metadata.create_all(engine)

    print('DB driver: {}'.format(driver))

    df = setup_df(test_table)

    df.to_sql(test_table.name, connection)
