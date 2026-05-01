"""Benchmarks for PPDB HATS using LSDB"""


def test_search_by_id(lbench_dask, ppdb):
    """
    Equivalent TAP query:

    SELECT * FROM ppdb.DiaObject WHERE diaObjectId = 170028500619624509
    """

    def search_by_id():
        ppdb.id_search({"diaObjectId": 170028500619624509}).compute()

    lbench_dask(search_by_id)


def test_cone_search(lbench_dask, ppdb):
    """
    Equivalent TAP query:

    SELECT diaObjectId, ra, dec
    FROM ppdb.DiaObject
    WHERE CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', 150.0, 4.7, 1)) = 1
    """

    def cone_search():
        cone = ppdb.cone_search(ra=150.0, dec=4.7, radius_arcsec=1.0)
        cone[["diaObjectId", "ra", "dec"]].compute()

    lbench_dask(cone_search)


def test_crossmatch(lbench_dask, ppdb):
    """
    Equivalent TAP query:

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

    def crossmatch():
        cone = ppdb.cone_search(ra=149, dec=2.5, radius_arcsec=0.2 * 3600)
        xmatch = cone.crossmatch(ppdb, radius_arcsec=0.001 * 3600, suffixes=("_1", "_2"))
        xmatch = xmatch[xmatch["validityStartMjdTai_1"] == xmatch["validityStartMjdTai_2"]]
        xmatch[["diaObjectId_1", "diaObjectId_2", "_dist_arcsec"]].compute()

    lbench_dask(crossmatch)


def test_table_scan(lbench_dask, ppdb):
    """
    Equivalent TAP query:

    SELECT diaObjectId, ra, dec
    FROM ppdb.DiaObject
    WHERE r_psfFluxMean BETWEEN 1090.0 AND 1100.0
    """

    def table_scan():
        query = ppdb.query("1090.0 <= r_psfFluxMean <= 1100.0")
        query[["diaObjectId", "ra", "dec"]].compute()

    lbench_dask(table_scan)
