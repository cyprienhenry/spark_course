from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrderCounts")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///Users/cyprienhenry/Documents/SparkCourse/data/customer-orders.csv")

orders = lines.map(lambda x: x.split(","))

def parse_line(line):
    customer_id = int(line[0])
    order_amount = float(line[2])
    return(customer_id, order_amount)

rdd = orders.map(parse_line)
total_per_customer = rdd.reduceByKey(lambda x, y: x + y)
results = total_per_customer.collect()

for result in results:
    print("Customer %d spent %.2f" % (result[0], result[1]))
