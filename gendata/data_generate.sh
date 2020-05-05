# Example of calling the data generation

time python generate_data.py -F feature_file.csv -c 1000000 -n -t 6 | split --bytes=50M --filter='gzip > data/$FILE.gz' - data