# test data generation for kafka exactly once / restart-recovery / feature engineering testing
# TODO save the feature set as a file in JSON; allow to re-read the same feature set
# TODO save the users as a file in JSON; allow to re-read the same user set

import random
import string
import uuid
import json
import time
import argparse
import csv
import logging
from datetime import datetime



logger = logging.getLogger('generate_data')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)

parser = argparse.ArgumentParser()
parser.add_argument("-c","--user_count", type=int, default=1000, help="number of users to be created")
parser.add_argument("-t","--output_time", type=float, default=24.0, help="time: number of hours of calls (default 24)")
parser.add_argument("-r","--transaction_rate", type=int, default=10, help="average number of transactions per second (default 10)")

parser.add_argument("-F","--feature_file", type=str, default="none", required=True, help="name of input file describing features and their cardinality")
#parser.add_argument("-O","--output_file", type=str, default="none", required=True, help="name of output file containing data")

parser.add_argument( "-k", "--trickle", default=True, action='store_true', help="Trickle one second of data each second")
parser.add_argument( "-n", "--no_trickle", default=False, dest='trickle', action='store_false', help="No trickling - emit data immediately")


args = parser.parse_args()

max_trans_per_sec = args.transaction_rate * 2

# timer to use for records (for "timestamp" feature type)
record_time = 0

def randomstring(length):
    return (''.join([random.choice(string.ascii_letters) for i in xrange(length)]))

# TODO we are assuming length > number of dgits to manage cardinality (ie 3 for 1000)
def randomword(length, cardinality, idx):
   l = len(str(cardinality-1))
   baseval = str(idx).rjust(l, '0')
   return baseval+ randomstring(length-l)

# def str_result(value, coltype, context):
#     try:
#         if coltype == "text":
#             return value
#         else:
#             return str(value).decode('utf-8')
#     except UnicodeEncodeError:
#         logger.error("Unable to handle value "+context)
#         print(value)
#         print (type(value))

# 
def generate_lov_value(lovName, column, coltype):
    lovIndex = lov_names.index(lovName)
    lov_data = lov_descs[lovIndex]['lovdata']
    entryIdx = lovEntries[lovIndex]
    context = "generating lov for "+lovName+"."+column+" entryIdx="+str(entryIdx)+", coltype="+coltype
    logger.debug(context)
    return lov_data[entryIdx][column]

def generate_value(fdesc):
    ftype = fdesc['type']
    logger.debug("generating value for "+str(fdesc))

    try:
        if ftype == 'cat':
            fvalues = fdesc['fvalues']
            l = len(fvalues)
            # pick a random value for this feature
            return fvalues[random.randint(0,l-1)]

        elif ftype == 'seq':
            r = sequence_values[fdesc['name']]
            sequence_values[fdesc['name']] += 1
            return r

        elif ftype == 'timestamp':
            return record_time

        elif ftype == 'constant':
            return fdesc['value']

        elif ftype == 'int':
            return random.randint(fdesc['start'], fdesc['end'])

        elif ftype == 'float':
            return float(fdesc['format'].format(random.uniform(fdesc['start'], fdesc['end'])))
            #return random.uniform(fdesc['start'], fdesc['end'])

        elif ftype == 'bool':
            # ignore flength and fstart, return True or False
            return (random.randint(0,1) == 1)

        elif ftype == 'text':
            # generate a completely random string of required length
            return randomstring(fdesc['length'])

        elif ftype == 'lov':
            # TODO return the selected column of the selected row for this lov
            
            # get the randomly selected entry for this lov
            return generate_lov_value(fdesc['lovName'], fdesc['column'], fdesc['coltype'])

        elif ftype == 'uuid.hex':
            return uuid.uuid1().hex

        else:
            return "UNEXPECTED TYPE "+ftype
    except:
        logger.error("Exception in generate_value for "+str(fdesc))
        raise


# TODO this should be an argument
fileName = 'feature_file.json'

with open(fileName) as f:
    feature_defs = json.load(f);
    logger.info("loaded "+str(len(feature_defs['features']))+ " feature definitions from "+fileName)

lov_descs = feature_defs['lovs']
lov_names = []

for lov_desc in lov_descs:
    if 'fileName' in lov_desc:
        # read in the JSON values from specified file
        with open(lov_desc['fileName']) as lovf:
            lov_desc['lovdata'] = json.load(lovf)
            logger.info("loaded LOV "+lov_desc['lovName']+" from "+lov_desc['fileName']+" with "+str(len(lov_desc['lovdata'])) + " entries")
            logger.debug(json.dumps(lov_desc['lovdata'], indent=2))

    elif 'lovdata' in lov_desc:
        logger.info("LOV " + lov_desc['lovName'] + " is supplied in the feature file")

    else:
        # we don't have any LOV data either in the feature file or an external file
        logger.error("No LOV data supplied for "+lov_desc['lovName'])  
        exit(-1)

    # Record the name of the LOV in a convenient list
    lov_names.append(lov_desc['lovName'])


feature_descs = feature_defs['features']
feature_count = len(feature_descs)
feature_names = []

# A dict to contain one running sequence value per 'seq' feature
sequence_values = {}

for feature_desc in feature_descs:

    try:
        feature_names.append(feature_desc['name'])

        if feature_desc['type'] == "cat":
            feature_desc['coltype'] = "text"

        elif feature_desc['type'] == "seq":
            # initialise a sequence type
            sequence_values[feature_desc['name']] = feature_desc['start']

        elif feature_desc['type'] != 'lov':
            # inherit coltype from feature type exept for cat and lov
            # cat is always text, lov defines it already
            feature_desc['coltype'] = feature_desc['type']
        
        elif 'coltype' not in feature_desc:
            logger.error("no coltype for feature:"+feature_desc['name'])
            exit(-2);

        # Categorical features with generated lov
        if feature_desc['type'] == 'cat':
            fvalues = []
            logger.debug("preparing values for cat feature "+feature_desc['name'])
            # make a list of values for this feature
            for i in xrange(feature_desc['cardinality']):
                # start the value with the index (zero filled) then fill with random chars
                fvalues.append(randomword(feature_desc['length'],feature_desc['cardinality'], i))
            
            feature_desc['fvalues'] = fvalues
    except:
        logger.debug("preparing values for feature "+feature_desc['name'])
        raise
       

# TODO save the generated features (including values) for later re-use
# print json.dumps(features)

# just number the users
# 
# Make sure userId is a long number
startrange=1000000000000

users = []

# Generate the initial list of users and devices

for i in xrange(startrange, startrange+int(args.user_count)):

    device_count=int(random.triangular(1,10,2))
    user_devices = []
    for dev in range(device_count):
        user_devices.append(i * 100 + dev)

    user = { 'id':i, 'devices': user_devices }

    users.append(user)


# TODO save user set for re-use
# print json.dumps(users)

# Now generate calls for random users every second
# start time from now (rounded to second)
startsecs = int(time.time())



# Generate data for this many "seconds"

output_seconds = int(3600 * args.output_time)

for calltime in xrange(output_seconds):

    # Generate up to this many transactions per second

    for counter in xrange(random.randint(1,max_trans_per_sec)):
        concat_features = {}
        record_time = calltime+startsecs

#        if ((record_time % 3600) == 0):
#            now = datetime.now()
#            current_time = now.strftime("%H:%M:%S")
#            logger.info(str(counter/3600) + " hours data generated at "+current_time)

        # choose a random subscriber
        user = users[random.randrange(len(users))]
        userId = user['id'];
        deviceCount = len(user['devices'])
        deviceIdx = 0 if deviceCount == 1 else random.randint(0,deviceCount-1)
        deviceId = user['devices'][deviceIdx]


        lovEntries = []

        # get index to a random lov row for each listed lov
        for l in xrange(len(lov_descs)):
            card = len(lov_descs[l]['lovdata'])
            entry=random.randint(0,card-1)
            lovEntries.append(entry)

        for f in xrange(len(feature_descs)):
            fdesc = feature_descs[f]
            concat_features[fdesc['name']] = generate_value(fdesc)

        concat_features['userId'] = str(userId)
        concat_features['deviceId'] = str(deviceId)

        print json.dumps(concat_features)
   

    # If we want to trickle the data:
    if args.trickle:
        time.sleep(1)

