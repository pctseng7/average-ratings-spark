from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


conf = SparkConf().setMaster("local").setAppName("AvgRatingByTags")
sc = SparkContext(conf = conf)

lines = sc.textFile("ml-latest-small/ratings.csv", use_unicode= False)
line2 = sc.textFile("ml-latest-small/tags.csv",use_unicode = False)
# remove header
header = lines.first()
lines = lines.filter(lambda line:line!= header)
header2 = line2.first()
line2 = line2.filter(lambda line:line!= header2)

# call function to map
rdd1 = lines.map(lambda line: line.split(',')).map(lambda line: (int(line[1]), float(line[2])))
rdd2 = line2.map(lambda line: line.split(',')).map(lambda line: (int(line[1]), line[2]))

# join two lines and then do calculation
totalsRating = rdd1.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).coalesce(1)
combine = totalsRating.join(rdd2).map(lambda line:(line[1][1],line[1][0])).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).coalesce(1)
averagesRating = combine.mapValues(lambda x: x[0] / x[1]).sortByKey(ascending=False)

# add header with parallelize()
unionHeaderRDD = averagesRating.map(lambda (k, v): "{0},{1}".format(k, v)).coalesce(1)
unionHeaderRDD = sc.parallelize( [('tag','rating_avg')])\
    .union(averagesRating).map(lambda (k, v): "{0},{1}".format(k, v)).coalesce(1)

#  collect 
results = unionHeaderRDD.collect()

# open a new file and write the results into the csv file
file = open('Po-Chuan_Tseng_result_task2_small.csv', 'w')
for result in results:
  file.write(result+ "\n")



