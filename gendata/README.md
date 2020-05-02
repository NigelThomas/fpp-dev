# gendata

This directory contains a simple test data generator - `generate_data.py`.

# Usage

```
usage: generate_data.py [-h] [-c USER_COUNT] [-t OUTPUT_TIME]
                        [-r TRANSACTION_RATE] -F FEATURE_FILE [-k] [-n]

optional arguments:
  -h, --help            show this help message and exit
  -c USER_COUNT, --user_count USER_COUNT
                        number of users to be created
  -t OUTPUT_TIME, --output_time OUTPUT_TIME
                        time: number of hours of calls (default 24)
  -r TRANSACTION_RATE, --transaction_rate TRANSACTION_RATE
                        average number of transactions per second (default 10)
  -F FEATURE_FILE, --feature_file FEATURE_FILE
                        name of input file describing features and their
                        cardinality
  -k, --trickle         Trickle one second of data each second
  -n, --no_trickle      No trickling - emit data immediately
```

# Feature File Format

The feature file contains a definition of the fields to be generated. It contains a single JSON object containing two elements `"lovs"` and `"features"`.

## Field definitions

The `"features"` element contains a list describing the fields to be generated. Each field has a `"name"` and `"type"`. Types include:

Type | Meaning | Description | Further metadata required
--- | --- | --- | ---
cat | Categorical Feature | A list of string values is generated - in the form 001xxxxxx to 123xxxxxx | `length` and `cardinality`
lov | List of Values | A list of values is provided; multiple fields can be populated by the same LOV | `lovname`, `column` and `type`
int | Integer | An integer is randomly generated | `start` and `end`
float | Float | A flota is randomly generated | `start`, `end` and `format` (for rounding)
text | A string | A string is randomly generated in the form NNNxxxxxx - NNN is a zero-padded number | `length` 
bool | Boolean | True or False is generated | (none)

## LOV definitions

The "lovs" definition includes one or more LOV specifications; the specification can either include a reference to a `"filename"` or in-line `"lovdata"` as shown here: 

```
{ "lovs":[ {"lovName":"cities"
           ,"fileName":"worldcities.json"
           }
         , {"lovName":"screens"
           ,"lovdata":
                [{"h":200,"w":300}
                ,{"h":480,"w":600}
                ,{"h":600,"w":1000}
                ,{"h":1000,"w":1800}
                ]
            }
    ]
```




