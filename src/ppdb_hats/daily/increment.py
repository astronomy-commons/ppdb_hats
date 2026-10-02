"""Utilities to write increment data and metadata"""

import logging

import numpy as np
from hats.catalog import PartitionInfo
from hats.io import paths
from hats.io.file_io import write_fits_image
from hats.io.skymap import read_skymap, write_skymap
from hats.pixel_math.sparse_histogram import HistogramAggregator
from lsdb.io.common import new_provenance_properties
from lsdb.io.to_hats import (
    create_modified_catalog_structure,
    remove_done_files,
    remove_histogram_files,
)
from lsdb.io.to_hats import write_partitions as lsdb_write_partitions

logger = logging.getLogger(__name__)


def write_partitions(catalog, base_catalog, histogram_order, parquet_name, **kwargs):
    """Write all catalog partitions to disk as parquet files.

    Each partition is written to its leaf pixel directory as
    ``Npix=<pixel>/<parquet_name>`` with LSDB's partition writer, which also
    computes histograms for skymap generation.

    Parameters
    ----------
    catalog
        LSDB catalog with partitions to write.
    base_catalog
        Base HATS catalog for output path determination.
    histogram_order : int
        HEALPix order for histogram calculations.
    parquet_name : str
        Name of the parquet file in each pixel directory (e.g., "2025-10-01.parquet").
    **kwargs
        Additional keyword arguments passed to ``pyarrow.parquet.write_table``.

    Returns
    -------
    tuple
        Tuple of ``(new_pixels, new_counts, new_histograms)`` where:
        - ``new_pixels`` is a list of new HEALPix pixel objects written.
        - ``new_counts`` is a list of row counts for each partition.
        - ``new_histograms`` is a list of ``SparseHistogram`` objects for skymap updates.
    """
    logger.info("Writing partitions...")
    base_catalog_dir = base_catalog.catalog_base_dir
    try:
        return lsdb_write_partitions(
            catalog,
            base_catalog_dir,
            histogram_order,
            npix_suffix="/",
            npix_parquet_name=parquet_name,
            progress_bar=False,
            **kwargs,
        )
    finally:
        # LSDB leaves its resume state (histogram and done files) in the catalog directory
        remove_histogram_files(base_catalog_dir)
        remove_done_files(base_catalog_dir)


def update_skymaps(existing_catalog, histograms, histogram_order):
    """Update skymap FITS files with new partition histograms.

    Combines histograms from new partitions with existing catalog histogram
    and writes updated skymap FITS files for all configured orders.

    Parameters
    ----------
    existing_catalog
        Existing HATS catalog object.
    histograms : list
        List of ``SparseHistogram`` objects from new partitions.
    histogram_order : int
        Order used for histogram calculation.
    """
    logger.info("Updating skymaps...")
    catalog_base_dir = existing_catalog.catalog_path
    existing_histogram = read_skymap(existing_catalog, histogram_order)

    total_histogram = HistogramAggregator(histogram_order)
    for partition_hist in histograms:
        total_histogram.add(partition_hist)

    # Also add the existing histogram
    full_histogram = total_histogram.full_histogram + existing_histogram

    # Write skymaps to point_map.fits and skymap_*.fits
    map_file_path = paths.get_point_map_file_pointer(catalog_base_dir)
    write_fits_image(full_histogram, map_file_pointer=map_file_path)
    skymap_alt_orders = existing_catalog.catalog_info.skymap_alt_orders
    write_skymap(full_histogram, catalog_dir=catalog_base_dir, orders=skymap_alt_orders)


def update_metadata(existing_catalog, new_pixels, new_counts):
    """Update catalog metadata after partition writes.

    Updates partition_info.csv with new pixels and hats.properties
    with updated row counts and maximum rows per partition.

    Parameters
    ----------
    existing_catalog
        Existing HATS catalog object.
    new_pixels : list
        List of new HEALPix pixel objects added.
    new_counts : list
        List of row counts for each new partition.
    """
    logger.info("Updating metadata...")
    catalog_base_dir = existing_catalog.catalog_path

    # Delete _metadata, since its statistics are outdated
    paths.get_parquet_metadata_pointer(catalog_base_dir).unlink(missing_ok=True)

    # Update `partition_info.csv` with new set of pixels
    pixels = set(existing_catalog.get_healpix_pixels())
    pixels.update(new_pixels)
    partition_info = PartitionInfo.from_healpix(list(pixels))
    partition_info_file = paths.get_partition_info_pointer(catalog_base_dir)
    partition_info.write_to_file(partition_info_file)

    # Update the `hats.properties`
    old_properties = existing_catalog.catalog_info
    previous_total_rows = int(old_properties.total_rows)
    total_rows = previous_total_rows + int(np.sum(new_counts))
    previous_max_rows = int(old_properties.hats_max_rows)
    max_rows = max(previous_max_rows, max(new_counts))

    new_hc_structure = create_modified_catalog_structure(
        existing_catalog,
        catalog_base_dir,
        existing_catalog.catalog_name,
        total_rows=total_rows,
        hats_max_rows=max_rows,
        hats_order=partition_info.get_highest_order(),
        moc_sky_fraction=f"{partition_info.calculate_fractional_coverage():0.5f}",
        **new_provenance_properties(catalog_base_dir),
    )
    new_hc_structure.catalog_info.to_properties_file(catalog_base_dir)
