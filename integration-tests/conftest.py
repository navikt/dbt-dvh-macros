import os
import pytest
from testcontainers.oracle import OracleDbContainer


@pytest.fixture(scope="session")
def oracle_container():
    """Fixture to start and provide an Oracle test container."""
    with OracleDbContainer(
        username="testuser",
        password="testpass",
        dbname="testdb"
    ) as oracle:
        env_vars = {
            "DBT_ENV_SECRET_USER": oracle.username,
            "DBT_ENV_SECRET_PASS": oracle.password,
            "DBT_HOST": oracle.get_container_host_ip(),
            "DBT_PORT": oracle.get_exposed_port(oracle.port),
            "DBT_SERVICE":  oracle.dbname,
            "DBT_DATABASE": oracle.dbname,
            "DBT_SCHEMA": oracle.username,
            "ORA_PYTHON_DRIVER_TYPE": "thin",
        }
        for k, v in env_vars.items():
            os.environ[k] = str(v)
        yield
        for k in env_vars:
            del os.environ[k]

