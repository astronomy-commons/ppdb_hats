"""Benchmarks for PPDB using the TAP service"""


def test_query_schema(lbench, query_tap):
    """Query all existing tables."""
    lbench(
        lambda: query_tap(
            "SELECT * FROM tap_schema.columns WHERE table_name LIKE 'ppdb.%'",
        )
    )


def test_search_by_id(lbench, query_tap):
    """
    Equivalent LSDB query:

    ppdb.id_search({"diaObjectId": 170028500619624509}).compute()
    """
    lbench(lambda: query_tap("SELECT * FROM ppdb.DiaObject WHERE diaObjectId=170028500619624509"))


def test_cone_search(lbench, query_tap):
    """
    Equivalent LSDB query:

    cone = ppdb.cone_search(ra=150.0, dec=4.7, radius_arcsec=1.0)
    cone[["diaObjectId", "ra", "dec"]].compute()
    """
    lbench(
        lambda: query_tap(
            """
            SELECT diaObjectId, ra, dec
            FROM ppdb.DiaObject
            WHERE CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', 150.0, 4.7, 1.0)) = 1
            """
        )
    )


def test_crossmatch(lbench, query_tap):
    """
    Equivalent LSDB query:

    cone = ppdb.cone_search(ra=60, dec=-50, radius_arcsec=0.5 * 3600)
    xmatch = cone.crossmatch(ppdb, min_radius_arcsec=1e-5, radius_arcsec=5, suffixes=("_1","_2"))
    xmatch = xmatch[xmatch["validityStartMjdTai_1"] == xmatch["validityStartMjdTai_2"]]
    xmatch[["diaObjectId_1", "diaObjectId_2", "_dist_arcsec"]].compute()
    """
    lbench(
        lambda: query_tap(
            """
            SELECT
                o1.diaObjectId AS id1,
                o2.diaObjectId AS id2,
                DISTANCE(POINT('ICRS', o1.ra, o1.dec), POINT('ICRS', o2.ra, o2.dec)) AS d
            FROM ppdb.DiaObject AS o1 JOIN ppdb.DiaObject AS o2
                ON o1.diaObjectId <> o2.diaObjectId AND o1.validityStart = o2.validityStart
            WHERE
                CONTAINS(POINT('ICRS', o1.ra, o1.dec), CIRCLE('ICRS', 60, -50, 0.5)) = 1
            AND
                DISTANCE(POINT('ICRS', o1.ra, o1.dec), POINT('ICRS', o2.ra, o2.dec)) < 5/3600
            """
        )
    )


def test_join(lbench, query_tap):
    """No direct LSDB equivalent, since data is already nested."""
    lbench(
        lambda: query_tap(
            """
            SELECT * FROM ppdb.DiaSource ds
            LEFT JOIN ppdb.DiaObject dob ON dob.diaObjectId = ds.diaObjectId
            WHERE dob.diaObjectId = 170028500619624509
            """
        )
    )


def test_table_scan(lbench, query_tap):
    """
    Equivalent LSDB query:

    ppdb.query("1090.0 <= r_psfFluxMean <= 1100.0").compute()
    """
    lbench(lambda: query_tap("SELECT * FROM ppdb.DiaObject WHERE r_psfFluxMean BETWEEN 1090.0 AND 1100.0"))
