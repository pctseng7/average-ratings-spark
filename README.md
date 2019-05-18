# MovieLens_AverageRatings

Data Mining for average movie/tag ratings 

## Getting Started

Follow the instructions will get you familiar with how to do data mining on large datasets. The open source datasets can be reached in the [MovieLens | GroupLens](https://grouplens.org/datasets/movielens/). In this repository, I use the MovieLens 20M Dataset and MovieLens Latest Datasets for implementation. Both datasets provide movieID and the rating record for each single movie. Each movie can also be categortized by tags. The goal is to find average ratings via Spark(PySpark) for movieID and tag, separately.

## Data Mining

* Task1 - find average **movie** ratings

* Task2 - find average **tag** ratings

## How to run my program

I put MovieLen datasets and two of my python code inside the Spark Folder. As you can see the path in the code. (For example: "ml-latest-small/ratings.csv") The program will read the file when we use “sc.textFile”. If you want to test the different task, you can just simply change the path to “ml-20m/ratings.csv”.

### Before testing


1. Put the source code (.py) and both datasets (ml-20m / ml-latest-small) inside the Spark folder

2. Start testing steps below

### Task1 step

1.Open the source code(Po-Chuan_Tseng_task1.py)

2.Change the sc.textfile path depends on the dataset you want to test

3.Save the file and open the Terminal on Mac

4.Cd into the Spark Folder and type the following command
 
./bin/spark-submit Po-Chuan_Tseng_task1.py

5.After the program finishes task, the txt file will be generated inside the Spark folder.

6.Open the file and check the values.

### Task2 step

1.Open the source code(Po-Chuan_Tseng_task2.py)

2.Change the sc.textfile path depends on the dataset you want to test

3.Save the file and open the Terminal on Mac

4.Cd into the Spark Folder and type the following command

./bin/spark-submit Po-Chuan_Tseng_task2.py

5.After the program finishes task, the csv file will be generated inside the Spark folder.

6.Open the file with TextEdit.app and check the values

## Credits

This repository is credited to the course project of INF553 at USC 
