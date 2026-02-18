"""Weekly reimport and collection generation for PPDB.

Handles aggregation of daily data and reimport of the entire catalog
on a weekly cycle to maintain optimal data organization.
"""

import logging
from pathlib import Path

import hats_import.collection.run_import as collection_runner
from dask.distributed import Client
from hats_import import CollectionArguments, ImportArguments, pipeline_with_client

logger = logging.getLogger(__name__)


def reimport_catalog(
    client: Client,
    tmp_dir: Path,
    weekly_dir: Path,
    collection_id: str,
) -> None:
    """Reimport an aggregated catalog into a balanced collection.

    Parameters
    ----------
    client : dask.distributed.Client
        Dask client for the reimport pipeline.
    tmp_dir : pathlib.Path
        Directory containing the aggregated catalog to reimport.
    weekly_dir : pathlib.Path
        Base PPDB HATS directory where the reimported collection will be written.
    collection_id : str
        Collection identifier string (YYYYMMDD format).
    """
    logger.info("Reimporting catalog...")
    args = ImportArguments.reimport_from_hats(
        path=tmp_dir / "dia_object_lc",
        output_dir=weekly_dir / f"dia_object_collection_{collection_id}",
        output_artifact_name="dia_object_lc",
        byte_pixel_threshold=1 << 30,
        skymap_alt_orders=[2, 4, 6],
        npix_suffix="/",
    )
    pipeline_with_client(args, client)


def generate_collection(
    client: Client,
    weekly_dir: Path,
    collection_id: str,
) -> None:
    """Generate a final HATS collection with indexes and margins.

    Parameters
    ----------
    client : dask.distributed.Client
        Dask client used to run collection generation.
    weekly_dir : pathlib.Path
        Base PPDB HATS directory where the collection will be created.
    collection_id : str
        Collection identifier string (YYYYMMDD format).
    """
    logger.info("Generating collection...")

    collection_artifact_name = f"dia_object_collection_{collection_id}"
    catalog_path = weekly_dir / collection_artifact_name / "dia_object_lc"

    collection_args = (
        CollectionArguments(
            output_path=weekly_dir,
            output_artifact_name=collection_artifact_name,
        )
        .catalog(catalog_path=catalog_path)
        .add_margin(margin_threshold=10)
        .add_index(indexing_column="diaObjectId")
    )
    collection_runner.run(collection_args, client)
