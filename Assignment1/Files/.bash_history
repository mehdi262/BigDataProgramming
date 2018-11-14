whereami
ping
hdfs sdfs -ls 
hdfs dfs -ls
hdfs dfs -ls /usr/mnikkhah
hdfs dfs -ls /user/mnikkhah
pwd
cd /user/mnikkhah
pwd
cd /user
cd ..
cd ..
ls
cd /home/mnikkhah/
hdfs dfs -ls /user/mnikkhah
hdfs dfs -mkdir wordcount-2
hdfs dfs -copyFromLocal /home/bigdata/wordcount/* wordcount-2/
hdfs dfs -mkdir wordcount-1
hdfs dfs -cp wordcount-2/a* wordcount-1/
hdfs dfs -ls /user/mnikkhah
env
printenv
export JAVA_HOME=/usr/lib/jvm/java-8-oracle/
export HADOOP_HOME=/usr/shared/CMPT/big-data/hadoop-2.6.0
${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` WordCount.java
hdfs dfs -ls
hdfds dfs -cd wordcount-1
hdfs dfs -cd wordcount-1
printenv
vi wordcount.java
${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` WordCount.java
ls
${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` wordcount.java
javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` wordcount.java
javac -classpath `${HADOOP_HOME}` wordcount.java
pwd

javac -classpath `hadoop classpath` wordcount.java
vi WordCount.java
javac -classpath `hadoop classpath` WordCount.java
ls
ls -la
jar cf WordCount.jar WordCount*.class
ls -la
jar cf wordcount.jar WordCount*.class
yarn jar wordcount.jar WordCount wordcount-1 output-1
hdfs dfs -rm -r output-1
yarn jar wordcount.jar WordCount wordcount-1 output-1
ls -la
hdfs dfs -rm -r WordCount.jar
hdfs dfs -rm  WordCount.jar
ls -la
hdfs dfs -rm  WordCount.jar
hdfs dfs -rm  wordcount.java
rm wordcount.java
ls -la
hdfs dfs ls
hdfs dfs -ls
hdfs dfs -rm -r output-1
yarn jar wordcount.jar WordCount -D mapreduce.job.reduces=3 
hdfs dfs -ls
yarn jar wordcount.jar WordCount wordcount-1 output-1
hdfs dfs -cat output-1/part-r-00000 | less
hdfs dfs -ls output-1/*
hdfs dfs -cat output-1/part-r-00000 | less
hdfs dfs -rm -r output-1
yarn jar wordcount.jar WordCount -D mapreduce.job.reduces=3     wordcount-1 output-2
hdfs dfs -ls output-1
hdfs dfs -ls output-2
hdfs dfs -cat output-2/part-r-00000 | less
yarn jar wordcount.jar WordCount -D mapreduce.job.reduces=0     wordcount-1 output-3
hdfs dfs -ls output-3
hdfs dfs -cat output-3/part-m-0001
hdfs dfs -cat output-3/part-m-00001
vi WordCountImproved.java
vi WordCountImproved.java
vi WordCountImproved.java
rm WordCountImproved.java
ls
vi WordCountImproved.java
vi WordCountImproved.java
vi WordCountImproved.java
vi WordCountImproved.java
javac -classpath `hadoop classpath` WordCountImproved.java
vi WordCountImproved.java
javac -classpath `hadoop classpath` WordCountImproved.java
jar cf WordCountImproved.jar WordCountImproved*.class
yarn jar WordCountImproved.jar WordCount -D mapreduce.job.reduces=0     wordcount-1 output-Improved-3
yarn jar WordCountImproved.jar WordCountImproved -D mapreduce.job.reduces=0     wordcount-1 output-Improved-3
$ hdfs dfs -cat output-Improved-3/part-r-00000 | grep -i "^better"
hdfs dfs -cat output-Improved-3/part-r-00000 | grep -i "^better"
hdfs dfs -ls output-Improved-3
yarn jar WordCountImproved.jar WordCountImproved -D mapreduce.job.reduces=1     wordcount-1 output-Improved-1
yarn jar WordCountImproved.jar WordCountImproved -D mapreduce.job.reduces=1     wordcount-1 output-Improved-1
hdfs dfs -rm -r output-Improved-1
yarn jar WordCountImproved.jar WordCountImproved -D mapreduce.job.reduces=1     wordcount-1 output-Improved-1
hdfs dfs -cat output-Improved-1/part-r-00000 | grep -i "^better"
hdfs dfs -ls output-Improved-1
hdfs dfs -ls output-Improved-1
hdfs dfs -rm -r output-Improved-1
yarn jar WordCountImproved.jar WordCountImproved -D mapreduce.job.reduces=1     wordcount-1 output-Improved-1
hdfs dfs -rm -r output-Improved-1
yarn jar WordCountImproved.jar WordCountImproved -D mapreduce.job.reduces=1     wordcount-1 output-Improved-1
vi WordCountImproved.java
pwd
ls
hdfs dfs  -ls
hdfs dfs -mkdir my-codes
hdfs dfs -copyFromLocal /home/mnikkhah/ my-codes/
hdfs dfs my-codes/
hdfs dfs /my-codes
hdfs dfs -ls my-codes
hdfs dfs -ls my-codes/mnikkhah
hdfs dfs -ls 
hdfs dfs -ls my-codes/mnikkhah
ls
pwd
hdfs dfs -ls
pwd
hdfs dfs -ls
cd ../..
pwd
ls
cd usr/share/
ls
cdcd xs
d ..
cd ..
ls
cd ..
cd home/mnikkhah/
hdfs dfs /user/mn
hdfs dfs -ls /home/bigdata/
hdfs dfs -ls /home/bigdata/
ls /home/bigdata/
ls
hdfs dfs -ls
hdfs dfs -du [-s] [-h] wordcount-1
hdfs dfs -du wordcount-1
hdfs dfs -du -s -h wordcount-1
hdfs dfs -du -s -h wordcount-2
lkdir wordcount-2
mkdir wordcount-2
ls
hdfs dfs -copyToLocal wordcount-2/* wordcount-2/
mkdir wordcount-1
hdfs dfs -copyToLocal wordcount-1/* wordcount-1/
mkdir improved-code
cd improved-code/
ls
javac WordCountImproved.java 
export HADOOP_HOME=/home/me/hadoop-3.1.1
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
javac -classpath `hadoop classpath` WordCountImproved.java 
ls
jar cf wordcountImproved.jar WordCountImproved*.class
ls
yarn jar wordcountImproved.jar WordCount ../wordcount-1 outputImproved-1
cd ..
yarn jar improved-code/wordcountImproved.jar WordCount wordcount-1 outputImproved-111
yarn jar wordcount.jar WordCount wordcount-1 output-11
ls
yarn
yarn jar wordcount.jar 
yarn jar
yarn
yarn jar wordcount.jar WordCount wordcount-1 output-1
yarn jar wordcount.jar WordCount s
yarn jar wordcount.jar WordCount \    /courses/732/wordcount-1 output-1
yarn jar wordcount.jar WordCount   /courses/732/wordcount-1 output-1
ls
hdfs dfs -ls
hdfs dfs -ls
hdfs dfs -ls 
hdfs dfs -ls
exit
ls
hdfs dfs -ls
yarn jar improved-code/wordcountImproved.jar  WordCount   wordcount-1 outputImp-1
yarn jar improved-code/wordcountImproved.jar  WordCountImporoved   wordcount-1 outputImp-1
yarn jar improved-code/wordcountImproved.jar WordCountImproved   wordcount-1 outputImp-1
hdfs dfs -ls outputImp-1
hdfs dfs -cat outputImp-1/part-r-00000 | less
yarn jar improved-code/wordcountImproved.jar WordCountImproved  -D mapreduce.job.reduces=3 \ wordcount-1 outputImp-2
yarn jar improved-code/wordcountImproved.jar WordCountImproved  -D mapreduce.job.reduces=3   wordcount-1 outputImp-2
hdfs dfs -ls outputImp-2
hdfs dfs -cat outputImp-2/part-r-00000 | less
hdfs dfs -cat outputImp-2/part-r-00001 | less
hdfs dfs -cat outputImp-2/part-r-00000 | less
hdfs dfs -cat outputImp-1/part-r-00000 | grep -i "^better"
