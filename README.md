# Dotutils

- [Dotutils](#dotutils)
  - [About](#about)
  - [⚙️ Functions](#️-functions)
  - [📦 Installation](#-installation)
      - [Conda Environment Setup](#conda-environment-setup)
      - [Package Install](#package-install)
  - [Usage](#usage)

## About

A list of convenience functions that are re-usable for a number of geospatial and related data wrangling operations

## ⚙️ Functions

| Class Name | Script | Purpose |
| --- | --- | --- |
| 📝  `Fuzzy` | [`fuzzy.py`](dotutils/fuzzy.py) | Fuzzy Matching, Joins on Dataframes |
| 🌏  `Geocoding` | [`geocoding.py`](dotutils/geocoding.py) | address, reverse, pandas geocoding |
| 🗺  `Geospatial` | [`geospatial.py`](dotutils/geospatial.py) | simple geospatial vector operation |
| 📍  `Places` | [`places.py`](dotutils/places.py) | Point-of-Interest data |
| 🧭  `Routing` | [`routing.py`](dotutils/routing.py) | Directions, routes, traffic |
| ☁️ `AWS` | [`aws.py`](dotutils/aws.py) | AWS programmtic functions |

## 📦 Installation

#### Conda Environment Setup

```bash
conda create --name dotutils python=3.9 -y
conda activate dotutils
```

#### Package Install

```bash
git clone https://github.com/dotlas/dotutils.git
cd dotutils
```

Then run either of the following commands:

```bash
pip install .
```

```bash
python setup.py install
```

## Usage

In your python source file:

```python
# import helpers module of legos
from dotutils.aws import Aws

# instantiate usage
aws = Aws()

# list available functions
dir(aws)
```

Directly use default instantiated objects

```python
import dotutils

s3_file = dotutils.aws.s3_download('<BUCKET NAME>', '<S3_KEY_WITH_PREFIX>')
geo_df = dotutils.geocoding.geocode_df(df, '<ADDRESS>', 'Google')
```

Use self instantiated objects

```python
import dotutils

s3_file = dotutils.AWS(
  access_key="XXX", access_code="YYY"
).s3_download('<BUCKET NAME>', '<S3_KEY_WITH_PREFIX>')

geo_response = dotutils.Geocoding(
  gmaps_api_key="XXX"
).geocode("<ADDRESS>")
```
