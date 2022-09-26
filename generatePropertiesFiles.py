import os
import boto3
from shutil import copyfile, move
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--region", help="region where the Cloudformation template was run")
parser.add_argument("--stackName", help="the name of Cloudformation stack ")
args = parser.parse_args()

cf_client = boto3.client('cloudformation', args.region)
s3_client = boto3.resource('s3')
msk_client = boto3.client('kafka', args.region)
stack_name = args.stackName
hostname = os.getenv('HOSTNAME')

zoo = False
kafka = False
producerProperty = False
schemaRegistry = False
mm = False

def handleReruns(fileName):
    if os.path.exists(fileName + "_bkup") == False:
        copyfile(fileName, fileName + "_bkup")
    else:
        copyfile(fileName + "_bkup", fileName)

def getMSKBootstrapBrokers(clusterArn):

    resp = msk_client.get_bootstrap_brokers(
    ClusterArn=clusterArn
    )
    return resp['BootstrapBrokerString']

def getMSKZookeeperConnectString(clusterArn):

    resp = msk_client.describe_cluster(
    ClusterArn=clusterArn
    )
    return resp['ClusterInfo']['ZookeeperConnectString']

def updateSchemaRegistryProperties(mskBootstrapBrokers):
    handleReruns("/tmp/kafka/schema-registry.properties")
    replaceText("/tmp/kafka/schema-registry.properties", "kafkastore.bootstrap.servers=", "kafkastore.bootstrap.servers=" + mskBootstrapBrokers)

def updateProducerProperties(confluentKafkaBootstrapBrokers, confluentSchemaRegistryUrl):
    handleReruns("/tmp/kafka/producer.properties")
    replaceText("/tmp/kafka/producer.properties", "BOOTSTRAP_SERVERS_CONFIG=", "BOOTSTRAP_SERVERS_CONFIG=" + confluentKafkaBootstrapBrokers)
    replaceText("/tmp/kafka/producer.properties", "SCHEMA_REGISTRY_URL_CONFIG=", "SCHEMA_REGISTRY_URL_CONFIG=" + confluentSchemaRegistryUrl)

def updateProducerPropertiesMsk(mskBootstrapBrokers, confluentSchemaRegistryUrl):
    handleReruns("/tmp/kafka/producer.properties_msk")
    replaceText("/tmp/kafka/producer.properties_msk", "BOOTSTRAP_SERVERS_CONFIG=", "BOOTSTRAP_SERVERS_CONFIG=" + mskBootstrapBrokers)
    replaceText("/tmp/kafka/producer.properties_msk", "SCHEMA_REGISTRY_URL_CONFIG=", "SCHEMA_REGISTRY_URL_CONFIG=" + confluentSchemaRegistryUrl)

def updateMMConsumerProperties(confluentKafkaBootstrapBrokers):
    handleReruns("/home/ec2-user/mm/mm-consumer.properties")
    replaceText("/home/ec2-user/mm/mm-consumer.properties", "bootstrap.servers=", "bootstrap.servers=" + confluentKafkaBootstrapBrokers)

def updateMMProducerProperties(mskBootstrapBrokers):
    handleReruns("/home/ec2-user/mm/mm-producer.properties")
    replaceText("/home/ec2-user/mm/mm-producer.properties", "bootstrap.servers=", "bootstrap.servers=" + mskBootstrapBrokers)

def updateMMConfluentKafkaConsumerGroups(confluentKafkaBootstrapBrokers):
    handleReruns("/home/ec2-user/mm/mm-confluent-kafka-consumer-groups.sh")
    replaceText("/home/ec2-user/mm/mm-confluent-kafka-consumer-groups.sh", "--bootstrap-server", "--bootstrap-server " + confluentKafkaBootstrapBrokers)

def updateMMConfluentKafkaConsumerGroupsRepointMSK(mskBootstrapBrokers):
    handleReruns("/home/ec2-user/mm/mm-confluent-kafka-consumer-groups-repoint-msk.sh")
    replaceText("/home/ec2-user/mm/mm-confluent-kafka-consumer-groups-repoint-msk.sh", "--bootstrap-server", "--bootstrap-server " + mskBootstrapBrokers)

def updateMMConsumerGroups(confluentKafkaBootstrapBrokers):
    handleReruns("/home/ec2-user/mm/mm-consumer-groups.sh")
    replaceText("/home/ec2-user/mm/mm-consumer-groups.sh", "--bootstrap-server", "--bootstrap-server " + confluentKafkaBootstrapBrokers)

def updateMMMSKKafkaConsumerGroups(mskBootstrapBrokers):
    handleReruns("/home/ec2-user/mm/mm-msk-kafka-consumer-groups.sh")
    replaceText("/home/ec2-user/mm/mm-msk-kafka-consumer-groups.sh", "--bootstrap-server", "--bootstrap-server " + mskBootstrapBrokers)

def updateMMConfluentKafkaConsoleAvroConsumer(confluentKafkaBootstrapBrokers, confluentSchemaRegistryUrl):
    handleReruns("/home/ec2-user/mm/mm-confluent-kafka-console-avro-consumer.sh")
    replaceText("/home/ec2-user/mm/mm-confluent-kafka-console-avro-consumer.sh", "--bootstrap-server", "--bootstrap-server " + confluentKafkaBootstrapBrokers)
    replaceText("/home/ec2-user/mm/mm-confluent-kafka-console-avro-consumer.sh", "schema.registry.url=", "schema.registry.url=" + confluentSchemaRegistryUrl)

def updateMMMSKKafkaConsoleAvroConsumer(mskBootstrapBrokers, confluentSchemaRegistryUrl):
    handleReruns("/home/ec2-user/mm/mm-msk-kafka-console-avro-consumer.sh")
    replaceText("/home/ec2-user/mm/mm-msk-kafka-console-avro-consumer.sh", "--bootstrap-server", "--bootstrap-server " + mskBootstrapBrokers)
    replaceText("/home/ec2-user/mm/mm-msk-kafka-console-avro-consumer.sh", "schema.registry.url=", "schema.registry.url=" + confluentSchemaRegistryUrl)

def updateMMConfluentKafkaConsoleAvroConsumerRepointMSK(mskBootstrapBrokers, confluentSchemaRegistryUrl):
    handleReruns("/home/ec2-user/mm/mm-confluent-kafka-console-avro-consumer-repoint-msk.sh")
    replaceText("/home/ec2-user/mm/mm-confluent-kafka-console-avro-consumer-repoint-msk.sh", "--bootstrap-server", "--bootstrap-server " + mskBootstrapBrokers)
    replaceText("/home/ec2-user/mm/mm-confluent-kafka-console-avro-consumer-repoint-msk.sh", "schema.registry.url=", "schema.registry.url=" + confluentSchemaRegistryUrl)

def updateMMConfluentKafkaCreateTopic(confluentKafkaZookeeperConnectString):
    handleReruns("/home/ec2-user/mm/mm-confluent-kafka-create-topic.sh")
    replaceText("/home/ec2-user/mm/mm-confluent-kafka-create-topic.sh", "--zookeeper", "--zookeeper " + confluentKafkaZookeeperConnectString)

def updateMMMSKKafkaCreateTopic(mskKafkaZookeeperConnectString):
    handleReruns("/home/ec2-user/mm/mm-msk-kafka-create-topic.sh")
    replaceText("/home/ec2-user/mm/mm-msk-kafka-create-topic.sh", "--zookeeper", "--zookeeper " + mskKafkaZookeeperConnectString)


def zooMoveAndCopyFile():
    try:
        move('/tmp/zk/zookeeper.properties', '/tmp/zk/zookeeper.properties_bkup')
    except IOError as e:
        print("Unable to move file. %s" % e)
        sys.exit(1)
    except:
        print("Unexpected error:", sys.exc_info())
        sys.exit(1)

    try:
        s3_client.meta.client.download_file('reinvent2019-msk-liftandshift', 'zookeeper.properties', '/tmp/zk/zookeeper.properties')
    except e:
        print("Unable to copy file from s3.")
        print(e)

def kafkaMoveAndCopyFile():
    try:
        move('/tmp/kafka/server.properties', '/tmp/kafka/server.properties_bkup')
    except IOError as e:
        print("Unable to move file. %s" % e)
        sys.exit(1)
    except:
        print("Unexpected error:", sys.exc_info())
        sys.exit(1)

    try:
        s3_client.meta.client.download_file('reinvent2019-msk-liftandshift', 'server.properties', '/tmp/kafka/server.properties')
    except e:
        print("Unable to copy file from s3.")
        print(e)


def copyFiles():

    try:
        move('/tmp/kafka/server.properties', '/tmp/kafka/server.properties_bkup')
        move('/tmp/zk/zookeeper.properties', '/tmp/zk/zookeeper.properties_bkup')
    except IOError as e:
        print("Unable to move file. %s" % e)
        sys.exit(1)
    except:
        print("Unexpected error:", sys.exc_info())
        sys.exit(1)

    try:
        s3_client.meta.client.download_file('reinvent2019-msk-liftandshift', 'server.properties', '/tmp/kafka/server.properties')
        s3_client.meta.client.download_file('reinvent2019-msk-liftandshift', 'zookeeper.properties', '/tmp/zk/zookeeper.properties')
    except e:
        print("Unable to copy file from s3.")
        print(e)

def replaceText(fileName, textToReplace, replacementText):
    filedata = None
    replacedData = None
    f = open(fileName)
    filedata = f.read()
    f.close()

    replacedData = filedata.replace(textToReplace, replacementText)

    f = open(fileName, 'w')
    f.write(replacedData)
    f.close()


result = cf_client.describe_stacks(StackName=stack_name)


with open("/tmp/stackoutputs.txt", "w") as file:

    

    for output in result['Stacks'][0]['Outputs']:
        file.write("{0} {1}\n".format(output['OutputKey'], output['OutputValue']))

        if  (output['OutputKey'] == "ConfluentKafka1PrivateDNS" and output['OutputValue'] == hostname):
            kafkaListener = "listeners=PLAINTEXT://" + output['OutputValue'] + ":9092"
            kafka = True

        if  (output['OutputKey'] == "ConfluentKafka2PrivateDNS" and output['OutputValue'] == hostname):
            kafkaListener = "listeners=PLAINTEXT://" + output['OutputValue'] + ":9092"
            kafka = True

        if  (output['OutputKey'] == "ConfluentKafka3PrivateDNS" and output['OutputValue'] == hostname):
            kafkaListener = "listeners=PLAINTEXT://" + output['OutputValue'] + ":9092"
            kafka = True


        if  (output['OutputKey'] == "Zoo1PrivateDNS" and output['OutputValue'] == hostname) or \
            (output['OutputKey'] == "Zoo2PrivateDNS" and output['OutputValue'] == hostname) or \
            (output['OutputKey'] == "Zoo3PrivateDNS" and output['OutputValue'] == hostname):
            zoo = True

        if (output['OutputKey'] == "MMEC2Instance1PrivateDNS" and output['OutputValue'] == hostname):
            schemaRegistry = True
            mm = True
        if (output['OutputKey'] == "MMEC2Instance2PrivateDNS" and output['OutputValue'] == hostname):
            producerProperty = True
            mm = True

        if output['OutputKey'] == "ConfluentKafkaZookeeperPropertiesServer1":
            kafkaZookeeperPropertiesServer1 = output['OutputValue'] + "\n"
        if output['OutputKey'] == "ConfluentKafkaZookeeperPropertiesServer2":
            kafkaZookeeperPropertiesServer2 = output['OutputValue'] + "\n"
        if output['OutputKey'] == "ConfluentKafkaZookeeperPropertiesServer3":
            kafkaZookeeperPropertiesServer3 = output['OutputValue'] + "\n"
        
        if output['OutputKey'] == "ConfluentKafkaZookeeperConnectString":
            kafkaZookeeperConnectString = "zookeeper.connect=" + output['OutputValue']

        if output['OutputKey'] == "ConfluentSchemaRegistryUrl":
            confluentSchemaRegistryUrl = output['OutputValue']
        if output['OutputKey'] == "ConfluentKafkaBootstrapbrokers":
            confluentKafkaBootstrapBrokers = output['OutputValue']
        if output['OutputKey'] == "MSKMMCluster1Arn":
            clusterArn = output['OutputValue']
        
    mskBootstrapBrokers = getMSKBootstrapBrokers(clusterArn)
    mskKafkaZookeeperConnectString = getMSKZookeeperConnectString(clusterArn)

    if kafka == True:
      kafkaMoveAndCopyFile()  
      replaceText("/tmp/kafka/server.properties", "listeners=", kafkaListener)
      replaceText("/tmp/kafka/server.properties", "zookeeper.connect=", kafkaZookeeperConnectString)

    if zoo == True:
      zooMoveAndCopyFile()  
      with open("/tmp/zk/zookeeper.properties", "a") as f:
          f.write(kafkaZookeeperPropertiesServer1)
          f.write(kafkaZookeeperPropertiesServer2)
          f.write(kafkaZookeeperPropertiesServer3)

    if schemaRegistry == True:
        updateSchemaRegistryProperties(mskBootstrapBrokers)

    if producerProperty == True:
        updateProducerProperties(confluentKafkaBootstrapBrokers, confluentSchemaRegistryUrl)
        updateProducerPropertiesMsk(mskBootstrapBrokers, confluentSchemaRegistryUrl)

    if mm == True:
        updateMMConsumerProperties(confluentKafkaBootstrapBrokers)
        updateMMProducerProperties(mskBootstrapBrokers)
        updateMMConfluentKafkaConsoleAvroConsumer(confluentKafkaBootstrapBrokers, confluentSchemaRegistryUrl)
        updateMMConfluentKafkaConsoleAvroConsumerRepointMSK(mskBootstrapBrokers, confluentSchemaRegistryUrl)
        updateMMConfluentKafkaConsumerGroups(confluentKafkaBootstrapBrokers)
        updateMMConfluentKafkaConsumerGroupsRepointMSK(mskBootstrapBrokers)
        updateMMConsumerGroups(confluentKafkaBootstrapBrokers)
        updateMMConfluentKafkaCreateTopic(kafkaZookeeperConnectString)
        updateMMMSKKafkaConsoleAvroConsumer(mskBootstrapBrokers, confluentSchemaRegistryUrl)
        updateMMMSKKafkaConsumerGroups(mskBootstrapBrokers)
        updateMMMSKKafkaCreateTopic(mskKafkaZookeeperConnectString)
