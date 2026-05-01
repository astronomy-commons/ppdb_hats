"""Benchmarks for PPDB using the TAP service"""


def test_query_schema(lbench, query_tap):
    """Query all existing tables."""
    lbench(
        lambda: query_tap(
            "SELECT * FROM tap_schema.columns WHERE table_name LIKE 'ppdb.%'",
        )
    )


def test_search_by_id(lbench, query_tap, object_cols):
    """
    Time ~4s

    Equivalent LSDB query:

    ppdb.id_search({"diaObjectId": 170028500619624509}).compute()
    """
    result = None

    def search_by_id():
        nonlocal result
        result = query_tap("SELECT * FROM ppdb.DiaObject WHERE diaObjectId=170028500619624509")

    lbench(search_by_id)
    assert len(result) == 147
    assert list(result.columns) == object_cols


def test_cone_search(lbench, query_tap):
    """
    Time ~9s

    Equivalent LSDB query:

    cone = ppdb.cone_search(ra=150.0, dec=4.7, radius_arcsec=1.0)
    cone[["diaObjectId", "ra", "dec"]].compute()
    """
    result = None

    def cone_search():
        nonlocal result
        result = query_tap(
            """
            SELECT diaObjectId, ra, dec
            FROM ppdb.DiaObject
            WHERE CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', 150.0, 4.7, 1)) = 1
            """
        )

    lbench(cone_search)
    assert len(result) == 66058
    assert list(result.columns) == ["diaObjectId", "ra", "dec"]


def test_crossmatch(lbench, query_tap):
    """
    Time ~8s

    Equivalent LSDB query:

    cone = ppdb.cone_search(ra=149, dec=2.5, radius_arcsec=0.2 * 3600)
    xmatch = cone.crossmatch(ppdb, radius_arcsec=0.001*3600, suffixes=("_1","_2"))
    xmatch = xmatch[xmatch["validityStartMjdTai_1"] == xmatch["validityStartMjdTai_2"]]
    xmatch[["diaObjectId_1", "diaObjectId_2", "_dist_arcsec"]].compute()
    """
    result = None

    def crossmatch():
        nonlocal result
        result = query_tap(
            """
            SELECT
                o1.diaObjectId AS id1,
                o2.diaObjectId AS id2,
                DISTANCE(POINT('ICRS', o1.ra, o1.dec), POINT('ICRS', o2.ra, o2.dec)) AS d
            FROM ppdb.DiaObject AS o1 JOIN ppdb.DiaObject AS o2
                ON o1.diaObjectId <> o2.diaObjectId AND o1.validityStartMjdTai = o2.validityStartMjdTai
            WHERE
                CONTAINS(POINT('ICRS', o1.ra, o1.dec), CIRCLE('ICRS', 149, 2.5, 0.2)) = 1
            AND
                DISTANCE(POINT('ICRS', o1.ra, o1.dec), POINT('ICRS', o2.ra, o2.dec)) < 0.001
            """
        )

    lbench(crossmatch)
    assert result.shape == (232, 3)
    assert list(result.columns) == ["id1", "id2", "d"]


def test_join_source(lbench, query_tap, object_cols, source_cols):
    """
    Time ~25s

    No direct LSDB equivalent, since data is already nested.
    """
    result = None

    def join_source():
        nonlocal result
        result = query_tap(
            """
            SELECT * FROM ppdb.DiaSource ds
            LEFT JOIN ppdb.DiaObject dob ON dob.diaObjectId = ds.diaObjectId
            WHERE dob.diaObjectId = 170028500619624509
            """
        )

    lbench(join_source)
    assert result.shape == (21609, len(source_cols) + len(object_cols))
    # Columns shared by object and source are renamed
    unique_cols = set(source_cols) | set(object_cols)
    assert set(result.columns).issuperset(unique_cols)


def test_join_forced_source(lbench, query_tap, object_cols, forced_source_cols):
    """
    Time ~1min

    No direct LSDB equivalent, since data is already nested.
    """
    result = None

    def join_forced_source():
        nonlocal result
        result = query_tap(
            """
            SELECT * FROM ppdb.DiaForcedSource dfs
            LEFT JOIN ppdb.DiaObject dob ON dob.diaObjectId = dfs.diaObjectId
            WHERE dob.diaObjectId = 170028500619624509
            """
        )

    lbench(join_forced_source)
    assert result.shape == (139356, len(forced_source_cols) + len(object_cols))
    # Columns shared by object and forced source are renamed
    unique_cols = set(forced_source_cols) | set(object_cols)
    assert set(result.columns).issuperset(unique_cols)


def test_table_scan(lbench, query_tap):
    """
    Time ~4s

    Equivalent LSDB query:

    query = ppdb.query("1090.0 <= r_psfFluxMean <= 1100.0")
    query[["diaObjectId", "ra", "dec"]].compute()
    """
    result = None

    def table_scan():
        nonlocal result
        result = query_tap(
            """
            SELECT diaObjectId, ra, dec
            FROM ppdb.DiaObject
            WHERE r_psfFluxMean BETWEEN 1090.0 AND 1100.0
            """
        )

    lbench(table_scan)
    assert len(result) == 12437
    assert list(result.columns) == ["diaObjectId", "ra", "dec"]
