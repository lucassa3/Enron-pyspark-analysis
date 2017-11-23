import re
import zipfile
import os
import sys
import csv
import itertools
from os import listdir
from os.path import isfile, join
from xml.dom.minidom import parse, parseString
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from neo4j.v1 import GraphDatabase, basic_auth

sc = SparkContext("local", "Enron")

sqlContext = SQLContext(sc)

directory_to_source = 'enron_db'
directory_to_extract_to = '/xml'


def extractXML(path_to_zip_file):

    full_path_to_zip_file = join(directory_to_source,path_to_zip_file)
    zip_ref = zipfile.ZipFile(full_path_to_zip_file, 'r')

    log = open("xml/xml_names.txt" , "a")
    
    for f in zip_ref.namelist():
        if f.startswith('zl'):
            zip_ref.extract(f, directory_to_extract_to)
            log.writelines(f + "\n")

    log.close()


def getEmails(document, item):
    
    emails = []
    to = []
    frm = []

    tags = document.getElementsByTagName('Tag')
    for t in tags:
        if t.getAttribute('TagName') == '#To':
            l = t.getAttribute('TagValue')
            to = re.findall(r'[\w\.-]+@[\w\.-]+', l)
        
        if t.getAttribute('TagName') == '#From':
            l = t.getAttribute('TagValue')
            frm = re.findall(r'[\w\.-]+@[\w\.-]+', l)

    if to:
        if frm:
            emails.extend(itertools.product(to, frm))
    
    
    return emails


def xml_to_emails(item):
        
        emailed = []

        doc = parse(join(directory_to_extract_to,item))

        documents = doc.getElementsByTagName('Document') #pega todos os documentos (emails) indexados no xml

        for document in documents:
            
            res1 = getEmails(document, item) #extrai destinatarios de cada email
            
            if res1:
                emailed.extend(res1)
        
        return emailed


def generateEmailRDD():

    with open("xml/xml_names.txt") as f:
        content = f.readlines()
        content = [x.strip() for x in content] 


    raw_xml = sc.parallelize(content)

    rdd = raw_xml.flatMap(lambda x: xml_to_emails(x)).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    return rdd


def edge_format_csv(f):
    return [(f[0][0],f[0][1],f[1])]


def node_format_csv(f):
    return [(f[0][0],f[0][0]),(f[0][1],f[0][1])]

def generate_neo4j_graph():
    print("Building graph...")
    
    driver = GraphDatabase.driver("bolt://127.0.0.1", auth=("neo4j", "showdotomas"),encrypted=False)
    session = driver.session()

    clean = """MATCH (n)
    DETACH DELETE n
    """
    session.run(clean)

    load = """
    LOAD CSV WITH HEADERS FROM "file:///nodes/part-00000-cda26823-8d76-486c-a264-293824f3c566-c000.csv" AS csvLine
    CREATE (e:Employee {username: csvLine.name})
    """

    session.run(load)

    edge = """
    USING PERIODIC COMMIT 500
    LOAD CSV WITH HEADERS FROM "file:///edges/part-00000-a7c31948-b369-450a-b1f9-db1f810388a5-c000.csv" AS csvLine
    MATCH (src:Employee),(dst:Employee)
    WHERE src.username =csvLine.src AND dst.username=csvLine.dst
    CREATE (src)-[:EMAIL { weight: csvLine.weight }]->(dst)
    """

    session.run(edge)

    print("Completed.")
    
    session.close()



if __name__ == '__main__':
	
    if len(sys.argv) == 2:
        zfiles = sys.argv[1]
    else:
        zfiles = [f for f in os.listdir(directory_to_source) if f.endswith('xml.zip')]  #lista com todos os zip (um pra cada remetente de email)
    
    open("xml/xml_names.txt", 'w').close() #zera arquivo
    
    for zfile in zfiles: #extrair todos os xml para pasta
        extractXML(zfile)
    	
    rdd = generateEmailRDD()

    edge_rdd = rdd.flatMap(lambda x: edge_format_csv(x)).toDF(["src","dst","weight"])
    node_rdd = rdd.flatMap(lambda x: node_format_csv(x)).distinct().toDF(["id","name"])
    
    print("Numer of nodes: " +str(node_rdd.count()))
    print("Numer of edges: " +str(edge_rdd.count()))

    #edge_rdd.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save(r'../../../.config/Neo4j Desktop/Application/neo4jDatabases/database-9be871ed-d8ad-4ec3-b24a-71b5431b0fbd/current/import/edges')
    #node_rdd.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save(r'../../../.config/Neo4j Desktop/Application/neo4jDatabases/database-9be871ed-d8ad-4ec3-b24a-71b5431b0fbd/current/import/nodes')

    generate_neo4j_graph()

    