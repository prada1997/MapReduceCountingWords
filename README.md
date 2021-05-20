# MapReduceCountingWords

Description:

It is MapReduce program to run it on Hadoop, we can run it on AWS EMR.

The Program is to perform various tasks:

Task 1 – Count words by lengths

To count Short Words (1-4 letters), medium words (5-7 letters) words,
long words (8-10 letters) and extra-long words (More than 10 letters).

Task 2 – Count words by the first character

Count number of words that begin with vowel or Consonant.

Task 3 – Count word with in-mapper combining

count the number of each word where the in-mapper combining is
implemented rather than an independent combiner.

Task 4 – Count word with partitioner

Using partitioner such that
- short words (1-4 letters) and extra-long words (More than 10 letters) are processed in one reducer,
- medium words (5-7 letters) and long words (8-10 letters) are processed in another reducer.

Requirements to run the program.
Maven
Java7
Hadoop
AWS EMR

Steps to run the program:

1.Create “BigData_Assignment-1.0-SNAPSHOT" jar file by using maven clean and then maven verify in build command.

2.Upload the jar in Hue account using website.

3.Create a folder in the files named “input”and upload all the input file that is “Melbourne-1” “RMIT-1” and “3littlepigs”

4.Download the jar file in to the Hadoop server by using “hadoop fs -copyToLocal /user/pradhuman/BigData_Assignment-1.0-SNAPSHOT.jar ~/" command.

5.To run the specific task, execute the specific command mentioned below:

Task 1:
hadoop jar BigData_Assignment-1.0-SNAPSHOT.jar edu.rmit.cosc2367.s3764267.Task1 /user/pradhuman/input /user/pradhuman/output1

Task 2: 
hadoop jar BigData_Assignment-1.0-SNAPSHOT.jar edu.rmit.cosc2367.s3764267.Task2 /user/pradhuman/input /user/pradhuman/output2

Task 3:
hadoop jar BigData_Assignment-1.0-SNAPSHOT.jar edu.rmit.cosc2367.s3764267.Task3 /user/pradhuman/input /user/pradhuman/output3

Task 4:
hadoop jar BigData_Assignment-1.0-SNAPSHOT.jar edu.rmit.cosc2367.s3764267.Task4 /user/pradhuman/input /user/pradhuman/output4

