#preparazione ambiente:
hdfs dfs -mkdir data
hdfs dfs -put london.csv data

#(pulizia per run successive)
hdfs dfs -rm -r temp1
hdfs dfs -rm -r temp2
hdfs dfs -rm -r dataout1
hdfs dfs -rm -r dataout2
hdfs dfs -rm -r outpy1
hdfs dfs -rm -r outpy2

#compilazione:
cd java
javac -Xdiags:verbose -classpath $(hadoop classpath) -d . JobOne.java
jar cf jobone.jar JobOne*.class
javac -Xdiags:verbose -classpat $(hadoop classpath) -d . JobTwo.java
jar cf jobtwo.jar JobTwo*.class
cd ..

#submit jobs:
hadoop jar java/jobone.jar JobOne data dataout1 london.csv outfile
hadoop jar java/jobtwo.jar JobTwo data dataout2 london.csv outfile
spark-submit python job1.py
spark-submit python job2.py

#get output:
hdfs dfs -getmerge dataout1/ out1.txt
hdfs dfs -getmerge dataout2/ out2.txt
hdfs dfs -getmerge outpy1/ outpy1.txt
hdfs dfs -getmerge outpy2/ outpy2.txt