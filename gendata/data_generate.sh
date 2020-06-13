# Example of calling the data generation
# Older OS (incl MacOS) split args need to be -l not --lines

time python generate_data.py -F feature_file.csv -c 1000000 -n -r 1 -t 740 | split -l 25000 --filter='gzip > data/$FILE.gz' - data
#time python generate_data.py -F feature_file.csv -c 1000000 -n -r 1 -t 740 | split -l 25000 - data
