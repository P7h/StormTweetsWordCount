# StormTweetsWordCount
----------

## Introduction
This repository contains an application which is built to demonstrate an example of Storm distributed framework by counting the words present in Tweets in real-time.

[Storm](http://storm-project.net) is a free and open source distributed realtime computation system, developed at BackType by Nathan Marz and team. It has been open sourced by Twitter [post BackType acquisition] in August, 2011.<br>
This application has been developed and tested with Storm v0.8.2 on CentOS. Application may or may not work with earlier or later versions than Storm v0.8.2.<br>

This application has been tested in:<br>

+ Local mode on a CentOS virtual machine and even on Microsoft Windows 7 machine.
+ Cluster mode on a private cluster and also on Amazon EC2 environment of 4 machines and 5 machines respectively; with all the machines in private cluster running Ubuntu while EC2 environment machines were powered by CentOS.

## Features
* Application retrieves tweets from Twitter stream (using [Twitter4J](http://twitter4j.org)) and in real-time processes only the English language tweets.<br>
* It splits each tweets with space as the delimiter and counts all the words present in tweets and finds list of words with highest count in every 30 seconds.
* After processing, the application logs the count and list of words grouped by their count to the console and also to a log file.<br>
* As of current day, this codebase has very minimal comments. I will be adding more comments as and when I get time.
* Also this project has been made compatible with both Eclipse IDE and IntelliJ IDEA. Import the project in your favorite IDE [which has Maven plugin installed] and you can quickly follow the code.

## Configuration
Please check the [`config.properties`](src/main/resources/config.properties) and add your own values and complete the integration of Twitter API to your application by looking at your values from [Twitter Developer Page](https://dev.twitter.com/apps).<br>
If you did not create a Twitter App before, then please create a new Twitter App where you will get all the required values of `config.properties` afresh and then populate them here without any mistake.<br>

## Dependencies
* Storm v0.8.2
* Twitter4J v3.0.3
* Google Guava v14.0.1
* SLF4J v1.7.5
* Logback v1.0.13

Also, please check [`pom.xml`](pom.xml) for more information on the various dependencies of the project.<br>

## Requirements
This project uses Maven to build and run the topology.<br>
You need the following on your machine:

* Oracle JDK >= 1.7.x
* Apache Maven >= 3.0.5
* Clone this repo and import as an existing Maven project to either Eclipse IDE or IntelliJ IDEA.
* This application uses [Google Guava](https://code.google.com/p/guava-libraries) for making life simple while using Collections.
* Requires ZooKeeper, JZMQ, ZeroMQ installed and configured in case of executing this project in distributed mode i.e. Storm Cluster.<br>
	- Follow the steps mentioned [here](https://github.com/nathanmarz/storm/wiki/Setting-up-a-Storm-cluster) for more details on setting up a Storm Cluster.<br>

Rest of the required frameworks and libraries are downloaded by Maven as required in the build process, the first time the Maven build is invoked.

## Usage
To build and run this topology, you must use Java 1.7.

### Local Mode:
Local mode can also be run on Windows environment without installing any specific software or framework as such. *Note*: Please be sure to clean your temp folder as it adds lot of temporary files in every run.<br>
In local mode, this application can be run from command line by invoking:<br>

    mvn clean compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=org.p7h.storm.wordcount.topology.WordCountTopology

or

    mvn clean compile package && java -jar target/storm-tweets-wordcount-0.1-jar-with-dependencies.jar
	
### Distributed [or Cluster / Production] Mode:
Distributed mode requires a complete and proper Storm Cluster setup. Please refer this [wiki](https://github.com/nathanmarz/storm/wiki/Setting-up-a-Storm-cluster) for setting up a Storm Cluster.<br>
In distributed mode, after starting Nimbus and Supervisors on individual machines, this application can be executed on the master [or Nimbus] machine by invoking the following on the command line:

    storm jar target/storm-tweets-wordcount-0.1-jar-with-dependencies.jar org.p7h.storm.wordcount.topology.WordCountTopology WordCount

## Problems
If you find any issues, please report them either raising an [issue](https://github.com/P7h/StormTweetsWordCount/issues) here on Github or alert me on my Twitter handle [@P7h](http://twitter.com/P7h). Or even better, please send a [pull request](https://github.com/P7h/StormTweetsWordCount/pulls).
Appreciate your help. Thanks!

## License
Copyright &copy; 2013 Prashanth Babu.<br>
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).