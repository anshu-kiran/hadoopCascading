# hadoopCascading

To run the project, first clone the repository locally.
Then run `mvn clean package`
Copy the input data to HDFS as such:

    hadoop fs -mkdir input
    hadoop fs -put test/resources/accidents_2017.csv input
Then run the MapReduce task as follows:

    hadoop jar hadoopCascading/target/hadoopCascading-1.0-SNAPSHOT.jar /input output local
After the task completes, view the output as:

    hadoop fs -cat output/*

The output can be seen as follows:

    Part of the day|count
    Afternoon|5082
    Morning|4067
    Night|1190
