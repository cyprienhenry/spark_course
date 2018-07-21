# find the maximum temperatures from a series of weather stations
# data format is (station_id, date, measurement type, value in 1/10 Celcius degrees, ...)

from pyspark import SparkConf, SparkContext

# create a Spark conf
conf = SparkConf().setMaster('local').setAppName('MaxTemp')

# create a Spark context
sc = SparkContext(conf=conf)

def line_parser(line):
    """Parse a line from the csv file into a List

    """
    fields = line.split(',')
    station_id = fields[0]
    measurement_type = fields[2]
    temperature = float(fields[3]) / 10
    return (station_id, measurement_type, temperature)


lines = sc.textFile('file:///Users/cyprienhenry/Documents/SparkCourse/data/1800.csv')
parsed_lines = lines.map(line_parser)
max_temp = parsed_lines.filter(lambda x: 'TMAX' in x[1])  # keep only max temp measurements
station_temp = max_temp.map(lambda x: (x[0], x[2]))  # extract station_id and max temperature
max_temp = station_temp.reduceByKey(lambda x, y: max(x, y))  # max temp per station

results = max_temp.collect()
for result in results:
    print('Station ID: %s \t Max temp: %.2f deg C' % (result[0], result[1]))
