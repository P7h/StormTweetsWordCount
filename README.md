# StormTweetsWordCount
----------

###<u>Note</u>: Updated to latest available `Storm` version i.e. v0.9.3 on 28<sup>th</sup> December, 2014. Storm package structure is a bit different now as it is a [TLP in Apache](https://blogs.apache.org/foundation/entry/the_apache_software_foundation_announces64) eff. 29<sup>th</sup> Sep, 2014.

## Introduction
This repository contains an application for demonstrating Apache Storm distributed framework by counting the words present in Tweets in real-time.

[Apache Storm](http://storm.apache.org) is an open source distributed real-time computation system, developed at BackType by Nathan Marz and team. It has been open sourced by Twitter [post BackType acquisition] in August, 2011. And became a top level project in Apache on 29<sup>th</sup> September, 2014.<br>
This application has been developed and tested with Storm v0.8.2 on Windows 7 in local mode; and was eventually updated and tested with Storm v0.9.3 on 28<sup>th</sup> December, 2014. Application may or may not work with earlier or later versions than Storm v0.9.3.<br>

This application has been tested in:<br>

+ Local mode on a CentOS virtual machine and even on Microsoft Windows 7 machine.
+ Cluster mode on a private cluster and also on Amazon EC2 environment of 4 machines and 5 machines respectively; with all the machines in private cluster running Ubuntu while EC2 environment machines were powered by CentOS.
	+ Recent update to Storm v0.9.3 has not been tested in a Cluster mode.

## Features
* Application retrieves tweets from Twitter stream (using [Twitter4J](http://twitter4j.org)) and in real-time processes only the English language tweets.<br>
* It splits each tweets with space as the delimiter and counts all the words present in tweets and finds list of words with highest count in every 30 seconds.
* After processing, the application logs the count and list of words grouped by their count to the console and also to a log file. The threshold of count after which the words are displayed can be configured by modifying [second value of this Constructor invocation](src/main/java/org/p7h/storm/wordcount/topology/WordCountTopology.java#L38).<br>
* In local mode, topology runs for 2 minutes and then shuts down. Topology run can be updated by modifying [this](src/main/java/org/p7h/storm/wordcount/topology/WordCountTopology.java#L50) value in `WordCountTopology.java`.<br>
* Also this project has been made compatible with both Eclipse IDE and IntelliJ IDEA. Import the project in your favorite IDE [which has Maven plugin installed] and you can quickly follow the code.

## Configuration
Please check the [`config.properties`](src/main/resources/config.properties) and add your own values and complete the integration of Twitter API to your application by looking at your values from [Twitter Developer Page](https://dev.twitter.com/apps).<br>
If you did not create a Twitter App before, then please create a new Twitter App where you will get all the required values of `config.properties` afresh and then populate them here without any mistake.<br>

## Dependencies
* Storm v0.9.3
* Twitter4J v4.0.2
* Google Guava v18.0
* Logback v1.1.2

Also, please check [`pom.xml`](pom.xml) for more information on the various dependencies of the project.<br>

## Requirements
This project uses Maven to build and run the topology.<br>
You need the following on your machine:

* Oracle JDK >= 1.8.x
* Apache Maven >= 3.2.3
* Clone this repo and import as an existing Maven project to either Eclipse IDE or IntelliJ IDEA.
* This application uses [Google Guava](https://code.google.com/p/guava-libraries) for making life simple while using Collections.
* Requires ZooKeeper, etc installed and configured in case of executing this project in distributed mode i.e. Storm Cluster.<br>
	- Follow the steps on [Storm Wiki](http://storm.apache.org/documentation/Setting-up-a-Storm-cluster.html) for more details on setting up a Storm Cluster.<br>

Rest of the required frameworks and libraries are downloaded by Maven as required in the build process, the first time the Maven build is invoked.

## Usage
To build and run this topology, you must use Java 1.8.

### Local Mode:
Local mode can also be run on Windows environment without installing any specific software or framework as such.<br>
*Note*: Please make sure to clean your temp folder as it adds lot of temporary files in every run.<br>
In local mode, this application can be run from command line by invoking:<br>

    mvn clean compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=org.p7h.storm.wordcount.topology.WordCountTopology

or

    mvn clean compile package && java -jar target/storm-tweets-wordcount-0.1-jar-with-dependencies.jar
	
### Distributed [or Cluster / Production] Mode:
Distributed mode requires a complete and proper Storm Cluster setup. Please check [Apache Storm wiki](http://storm.apache.org/documentation/Setting-up-a-Storm-cluster.html) for setting up a Storm Cluster.<br>
In distributed mode, after starting Nimbus and Supervisors on individual machines, this application can be executed on the master [or Nimbus] machine by invoking the following on the command line:

    storm jar target/storm-tweets-wordcount-0.1.jar org.p7h.storm.wordcount.topology.WordCountTopology WordCount

## Problems
If you find any issues, please report them either raising an [issue](https://github.com/P7h/StormTweetsWordCount/issues) here on Github or alert me on my Twitter handle [@P7h](http://twitter.com/P7h). Or even better, please send a [pull request](https://github.com/P7h/StormTweetsWordCount/pulls).<br>
Appreciate your help. Thanks!

## License
Copyright &copy; 2013-15 Prashanth Babu.<br>
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).