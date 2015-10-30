# MapROne
Hadoop Map Reduce basics


ubuntu@ubuntu:~$ clear

ubuntu@ubuntu:~$ pwd
/home/ubuntu

ubuntu@ubuntu:~$ echo $HADOOP_INSTALL


- ubuntu@ubuntu:~$ jps
2306 
8421 NodeManager
8069 SecondaryNameNode
8262 ResourceManager
7718 NameNode
8489 Jps

- ubuntu@ubuntu:~$ start-all.sh
This script is Deprecated. Instead use start-dfs.sh and start-yarn.sh

- ubuntu@ubuntu:~$ stop-all.sh
This script is Deprecated. Instead use stop-dfs.sh and stop-yarn.sh

- to list HDFS directories ::
ubuntu@ubuntu:~$ hadoop fs -ls /


-----------------------------------------------------------------
Note: any daemon is not running then, format them respectively ::

when we any of the nodes, we see not up and running, when we use jps
then, goto location, where exactly notes are configured phisically.


<configuration>
<property>
  <name>dfs.replication</name>
  <value>1</value>
  <description>Default block replication.
  The actual number of replications can be specified when the file is created.
  The default is used if replication is not specified in create time.
  </description>
 </property>
 <property>
   <name>dfs.namenode.name.dir</name>
   <value>file:/usr/local/hadoop_store/hdfs/namenode</value>
 </property>
 <property>
   <name>dfs.datanode.data.dir</name>
   <value>file:/usr/local/hadoop_store/hdfs/datanode</value>
 </property>
</configuration>

in hdfs-site.xml


/usr/local/hadoop_store/datanode
/usr/local/hadoop_store/namenode

ubuntu@ubuntu:~$ hadoop namenode -format
ubuntu@ubuntu:~$ hadoop datanode -format


-------------------------------------------


ubuntu@ubuntu:~$ hadoop fs -ls /

ubuntu@ubuntu:~$ hadoop fs -mkdir /input

ubuntu@ubuntu:~$ hadoop fs -rm /input/*
ubuntu@ubuntu:~$ hadoop fs -rmdir /input

hadoop fs -rm -r /output




ubuntu@ubuntu:~$ hadoop fs -ls /input

ubuntu@ubuntu:~$ hadoop fs -copyFromLocal input/input.txt /input

ubuntu@ubuntu:~$ hadoop fs -copyFromLocal /home/ubuntu/abc.txt /input                                 (/user/ubuntu/ - current directory)



hadoop jar /home/ubuntu/WordCount.jar com.impetus.basic.mapper.WordCount /akovi/abc.txt /output


----------------------------------------------------------------------------
ubuntu@ubuntu:~$ hadoop fs -copyFromLocal input/input.txt   /hdfsInput.txt


ubuntu@ubuntu:~$ hadoop fs -rm -r /hdfsInput.txt
Deleted /hdfsInput.txt


ubuntu@ubuntu:~$ hadoop fs -cat   /hdfsInput.txt
--------------------------------------------------------------------

  ubuntu@ubuntu:~$ hadoop fs -rm -r /hdfsInput.txt
  ubuntu@ubuntu:~$ hadoop fs -copyFromLocal input/input.txt   /hdfsInput.txt
  ubuntu@ubuntu::~$ hadoop fs -rm -r /output
  ubuntu@ubuntu:~$ hadoop jar MaxTemparature.jar com.mapr.MaxTemparature /hdfsInput.txt /output
  ubuntu@ubuntu:~$ hadoop fs -cat   /output/*

----------------------------------------
Note :: 
http://hadooped.blogspot.in/2013/09/sample-code-for-secondary-sort.html



