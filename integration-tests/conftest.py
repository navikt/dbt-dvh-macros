import os
import pytest
from testcontainers.core import testcontainers_config
testcontainers_config.ryuk_disabled = True
from testcontainers.oracle import OracleDbContainer
import oracledb
from random import randbytes
from hashlib import sha256
from typing import NamedTuple


class ConnectionConfig(NamedTuple):
    host:str
    port:int
    service_name:str
    user:str
    password:str
    app_user:str
    app_pass:str


@pytest.fixture(autouse=True, scope="session")
def oracle_connection():
    """Fixture to start and provide an Oracle test container with a connection object."""
    config = ConnectionConfig(
        host="127.0.0.1",
        port=1521,
        service_name="FREEPDB1",
        user="system",
        password=sha256(randbytes(64)).hexdigest(),
        app_user="dbtuser",
        app_pass=sha256(randbytes(64)).hexdigest()

    )
    oracle = OracleDbContainer(
        oracle_password=config.password,
        dbname=config.service_name,
    )
    # Set the time zone to local, default is UTC which can cause issues when converting to local time
    oracle.with_env("TZ", "Europe/Oslo")
    oracle.with_env("ORA_SDTZ", "Europe/Oslo")
    oracle.with_env("APP_USER", config.app_user)
    oracle.with_env("APP_USER_PASSWORD", config.app_pass)

    # do not bind to 0.0.0.0 to avoid exposing the database to all interfaces
    oracle.with_bind_ports(1521, (config.host, config.port)) #type: ignore

    # fixed name for reuse and error messages should a container be running from before
    oracle.with_name("testcontainers-oracle-db")

    try:
        oracle.start()
        assert oracle.get_container_host_ip() != "0.0.0.0", "bad host"
        env_vars = {
            "DBT_USER": config.app_user,
            "DBT_ENV_SECRET_PASS": config.app_pass,
            "DBT_HOST": config.host,
            "DBT_PORT": str(config.port),
            "DBT_SERVICE": config.service_name,
            "DBT_DATABASE": config.service_name,
            "DBT_SCHEMA": config.app_user,
            "ORA_PYTHON_DRIVER_TYPE": "thin",
        }
        for k, v in env_vars.items():
            os.environ[k] = str(v)
        # Create a persistent connection for setup/teardown
        with oracledb.connect(
            user=config.app_user, # type: ignore
            password=config.app_pass,
            host=config.host,
            port=config.port,
            service_name=config.service_name
        ) as con:
            with con.cursor() as cur:
                yield con

        for k in env_vars:
            del os.environ[k]
    finally:
        oracle.stop()

