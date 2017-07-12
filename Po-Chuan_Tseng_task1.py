from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("AvgRatingsByMovieId")
sc = SparkContext(conf = conf)

def ratingLine(line):
    fields = line.split(',')
    movieId = int(fields[1])
    rating = float(fields[2])
    return movieId, rating

lines = sc.textFile("ml-latest-small/ratings.csv")

# remove header
header = lines.first()
lines = lines.filter(lambda line:line!=header)
rdd = lines.map(ratingLine)

# calculating average ratings by using mapValues, reduceByKey
totalsRating = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).coalesce(1)
averagesRating = totalsRating.mapValues(lambda x: x[0] / x[1]).coalesce(1)
aaa = averagesRating.sortByKey(ascending=True).map(lambda (k, v): "{0},{1}".format(k, v)).coalesce(1)
results = aaa.collect()

# create a new file and write the results into the txt file
file = open('Po-Chuan_Tseng_result_task1_small.txt', 'w')
for result in results:
	file.write(str(result) + "\n")