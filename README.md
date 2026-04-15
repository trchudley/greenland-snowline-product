# Greenland Snowlines Data Product

This is an experimental repository for testing the distribution of a Greenland-wide MODIS snowline dataset.

> [!CAUTION]
> This is an experimental data product being used for the purposes of testing data distribution methods. The snowline and bare-ice extent values have not been peer-reviewed and are not recommended to be used in published research.

## Method

Data has been produced from MODIS MOD10A1.061 snow albedo data, roughly following the method of [Ryan _et al._ (2019, *Science Advances*)](https://doi.org/10.1126/sciadv.aav3738), but using a set albedo threshold of 0.55 for snow rather than a random forest model.

## Website

For local testing, run `python -m http.server 8080` whilst in the root directory. The website will be available at [`http://localhost:8080/`](http://localhost:8080/).

## Contact

[Tom Chudley](https://trchudley.github.io/), [Bristol Glaciology Centre](https://bristol-glaciology.github.io/), University of Bristol. Email: [tom.chudley@bristol.ac.uk](mailto:tom.chudley@bristol.ac.uk).
