# fpp-dev

This repository (https://github.com/NigelThomas/fpp-dev.git) contains a feature engineering pipeline for the FPP project. It is intended to be combined with the 
existing FPP pipeline (held in Gemalto's own repository)


## Content

### Shell scripts
* `UnPivot.jar` - A jar file that is used in the FPP pipeline for Unpivot and Pivot operations (see git@bitbucket.org:nigel_cl_thomas/pivot.git for source code)
* `sqlstream-http.jar` - A jar file that includes the HttpPost UDX (see https://github.com/NigelThomas/sqlstream-http.git for source code)

### Feature engineering and scoring pipeline

This pipeline can be used with the test ingest pipeline, but in production will be combined with the Kafka ingest and delivery pipelines.

* `interface.sql` - the raw feature stream
* `normalization_data.sql` - reads data from fpp_normalize_min_max.txt
* `feature_engineering.sql` - the feature engineering pipeline (from "interface"."transactions")
* `scoring_pipeline.sql` - the scoring pipeline 

### Test ingest pipeline
* `ingest_json.sql` - reads data from (uncompressed) data/sample.json
* `cast_ingest_pump.sql` - pump from JSON file source to interface table
* `reset_pipeline.sql` - resets pump and stream
* `test_pipeline.sql` - creates the end to end test pipeline

#### Alternative format for interface data
* `typed_ingest_pump.sql`
* `typed_interface.sql`

### `data` directory - seed data
* `fpp_normalize_min_max.txt` - mean and standard deviation for 1140 features derived from training data

### `fpp-data` directory - sample test data (not included in repository)
* The `dockerrun.sh` script mounts a data volume (default name is $HOME/fpp-data) into /home/sqlstream/fpp-data
* This is used for the test pipeline only, and is expected to contain one or more JSON files

### `doc` directory
* `fpp_feature_list.txt` - the 1140 derived features in the order they will appear in the array calling DSS
* `raw_features_list.txt` - the 57 raw features
* `fpp_feature_engineering.py` - the python code used in DSS (will be moved to SQLstream shortly)

### Miscellaneous
* `README.md` - this file
* `GemaltoBPS.lic` - license file



## Data ingest

In the test pipeline, data is read from the sample data file. In production the data is extracted from two Kafka topics.


## Feature Prep

There are 57 raw features - see fpp_features_list.txt. These get mapped into the stream "interface"."transactions", and from there the 

## Unpivot

* See Pivot.jar (unpivot repository) and the feature_engineering.sql pipeline.
* Generates one row per transaction per raw feature - so 57 rows for each input row in "interface"."transactions".

## Feature engineering

* Generate "historic presence" and "velocity" for feature vs user and feature vs device
* 5 windows (1h, 6h, 1d, 7d, 30d) x 4 = 20 derived features per original feature

## Feature normalisation

### MinMax scaler

* The rescaled value for a feature E is calculated as: 

```
Rescaled(Ei) = (Ei – Emin)  /  (Emax – Emin).
For the case Emax == Emin Rescaled(ei)=0.5
```

* See fpp_normalize_min_max.txt for list of min and max values per feature


## Pivot
The Pivot UDX is used to combine the 57 rows into one. One column is generated from each feature row (so 57 columns in total). Each column contains the 20 derived
features. 

The order of features defined in `fpp_features_list.txt` does not put all features derived from the same raw feature together; instead it groups all the 'presence' features and then all the 'velocity' features. So we arrange that we concatenate the first 10 derived features comma separated, and then pipe-separate the presence features from the velocity features.

This allows us to re-order the features for the scoring.

### Scoring

The scoring is currently executed within Dataiku DSS; SQLstream calls the REST API. In a future version the python code will be executed directly within SQLstream.

* Takes 57 rows each with 5 x 4 = 20 derived features to make a 1140 element array - it must be in the correct order, which is as defined in `fpp_features_list.txt`.
* Executes the scoring algorithm
* Emits a single row containing the descriptor columns + score.
 
