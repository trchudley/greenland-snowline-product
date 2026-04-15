# Workflow Scheme

Python scripts have the following functions:

 - `01*` - Download MODIS data and generate filtered and reprojected albedo files.
 - `02*` - Process albedo files into bare ice fraction files.
 - `03*` - Generate time-series snowline and bare-ice extent data for different scales of Greenland (total, regions, basins).

Whilst theoretically, running the workflow in order should generate the final dataset, there is a problem: the Microsoft Planetary Computer download (`01a*.py`) does not have working MOD10A1.061 data after 2023. Instead, an interim solution is provided in the `01b*.py` script using raw downloads from the NSIDC. To work this script, go to the [NSIDC downloads website](https://nsidc.org/data/data-access-tool/MOD10A1/versions/61) and draw a bounding box around Greenland, selecting dates between YYYY-05-01 and YYYY-09-30 for your chosen year. Approx ~1600 files should be selected. Click the 'download script' button in the download page. Download this python script into `./MOD10A1.061_raw/YYYY`, and run it do download the raw files. Then, `01b*.py` can be run to process these raw data into the filtered and reprojected geotiff files.