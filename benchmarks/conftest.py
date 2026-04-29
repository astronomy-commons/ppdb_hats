import os

import lsdb
import pytest
import requests
from pyvo.dal import TAPService


@pytest.fixture(scope="session")
def tap():
    token = os.environ["RSP_TOKEN"]
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {token}"
    rsp_tap_url = "https://data-int.lsst.cloud/api/ppdbtap"
    return TAPService(rsp_tap_url, session=session)


@pytest.fixture(scope="session")
def query_tap(tap):
    def _query(sql):
        return tap.run_async(sql).to_table().to_pandas()

    return _query


@pytest.fixture(scope="session")
def ppdb():
    return lsdb.open_catalog("/sdf/data/rubin/shared/lsdb_commissioning/ppdb/dia_object_collection/")
