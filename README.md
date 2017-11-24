# Enron-pyspark-analysis
A spark application that retrieves content from Enron's mail dataset and builds a Neo4j graph with some social network measurements

# The Project
Our goal is to take the enron emails dataset, made available after the company bankruptcy in 2002, analyze it using big-data tools and get some relevant social network insight, such as degree, corenness, and pagerank from all of the email adresses within the dataset.

The core of this project could be potentially used to analyze other mail datasets, such as your companhies' mail record, as long as the employees emails are tagged into an XML file with to and from fields tagged accordingly. 

# Tools used
For this project, we used:
* the enron-v2 dataset made available from this link: https://archive.org/details/edrm.enron.email.data.set.v2.xml (only the individual zip files of each employee);
* Apache Spark through pyspark library for python to create and manage RDDs;
* Neo4j graph database platform and neo4j.v1 and py2neo python libraries.

# What does the code do:
The main.py file is responsible for:
* Extracting all the xml files from each employees mail dataset zip folder and place it on a temporary folder;
* Parsing the xml files to get all the pairs to-from of each email (it permutates if there is more than one to recipient) and store this information on an RDD;
* Create two separate RDDs with the vertices(email adresses) and edges(emails) obtained from previous RDD;
* Writing the RDDs informations in two CSV files;
* Building a Neo4J database with the RDD information (a query loads both csv files to create the vertices and edges of the graph). 

The analise.py file is responsible for:
* Retrieving the 

# Assumptions
* 
