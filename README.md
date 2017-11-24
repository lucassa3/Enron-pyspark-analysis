# Enron-pyspark-analysis
A spark application that retrieves content from Enron's mail dataset and builds a Neo4j graph with some social network measurements

# The Project
Our goal is to take the enron emails dataset, made available after the company bankruptcy in 2002, analyze it using big-data tools and get some relevant social network insight, such as degree, betweenness, and pagerank from all of the email adresses within the dataset. The idea of using big-data tools is to make the project scalable for running in multiple clusters in order to be able to process the whole Enron data (and possibly even larger datasets with similar data format).

The core of this project could be potentially used to analyze other mail datasets, such as your companhies' mail record, as long as the employees emails are tagged into an XML file with to and from fields tagged accordingly. 

### Tools used
For this project, we used:
* The enron-v2 dataset made available from this link: https://archive.org/details/edrm.enron.email.data.set.v2.xml (only the individual zip files of each employee);
* Apache Spark through pyspark library for python to create and manage RDDs;
* Neo4j graph database platform and neo4j.v1 and py2neo python libraries.

### Methodology
To achieve the final result, an analysis of the Enron's internal social network, the raw data went through the process listed below:

Step 1 - extracting the data:


The main.py file is responsible for:
* Extracting all the xml files from each employees mail dataset zip folder and place it on a temporary folder;
* Parsing the xml files to get all the pairs to-from of each email (it permutates if there is more than one to recipient) and store this information on an RDD;
* Create two separate RDDs with the vertices(email adresses) and edges(emails) obtained from previous RDD;
* Writing the RDDs informations in two CSV files;
* Building a Neo4J database with the RDD information (a query loads both csv files to create the vertices and edges of the graph). 

The analise.py file is responsible for:
* Retrieving the Neo4J database and calculating degree, weighted degree, betweenness and pagerank values for the graph.


#
* 
