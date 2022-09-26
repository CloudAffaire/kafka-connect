import os
import boto3
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--region", help="region where the Cloudformation template was run")
parser.add_argument("--stackName", help="the name of Cloudformation stack ")
args = parser.parse_args()

region = args.region
cf_client = boto3.client('cloudformation', region)
msk_client = boto3.client('kafka', region)
stackName = args.stackName

try:
    os.remove("/tmp/kafka/startup.sh")
except FileNotFoundError as exp:
    pass

result = cf_client.describe_stacks(StackName=stackName)

def errorCheck(file):
    file.write("if [ $? -ne 0 ]\n")
    file.write("then\n")
    file.write("echo 'Encountered error. Exiting ..'\n")
    file.write("exit 1\n")
    file.write("fi\n")

def errorCheckEC2Instance1(file):
    file.write("grep \"'ExampleTopic' already exists.\" /tmp/source_cluster_create_topic_output.txt\n")
    file.write("if [ $? -eq 0 ]\n")
    file.write("then\n")
    file.write("echo \"ExampleTopic already exists...\"\n")
    file.write("else\n")
    file.write("echo 'Encountered error. Exiting ..'\n")
    file.write("exit 1\n")
    file.write("fi\n")


def zoo(zooSsh):
    with open("/tmp/kafka/startup.sh", "a") as file:
        file.write("echo 'Configuring and Starting Zookeeper node'\n")
        file.write("{0}<<EOF\n".format(zooSsh))
        file.write("cd /tmp/zk\n")
        file.write("python3 generatePropertiesFiles.py --region {0} --stackName {1}\n".format(region, stackName))
        errorCheck(file)
        file.write("sudo systemctl is-active confluent-zookeeper --quiet||sudo systemctl start confluent-zookeeper\n")
        errorCheck(file)
        file.write("sudo systemctl status confluent-zookeeper\n")
        file.write("EOF\n")
        file.write("\n")
        errorCheck(file)
        file.write("\n")

def confluentKafka(confluentKafkaSsh):
    with open("/tmp/kafka/startup.sh", "a") as file:
        file.write("echo 'Configuring and Starting Confluent Kafka node'\n")
        file.write("{0}<<EOF\n".format(confluentKafkaSsh))
        file.write("cd /tmp/kafka\n")
        file.write("python3 generatePropertiesFiles.py --region {0} --stackName {1}\n".format(region, stackName))
        errorCheck(file)
        file.write("sudo systemctl is-active confluent-kafka --quiet||sudo systemctl start confluent-kafka\n")
        errorCheck(file)
        file.write("sudo systemctl status confluent-kafka\n")
        file.write("EOF\n")
        file.write("\n")
        errorCheck(file)
        file.write("\n")

def mmEC2Instance1(mmEC2Instance1Ssh):
    with open("/tmp/kafka/startup.sh", "a") as file:
        file.write("echo 'Configuring MM scripts and starting schema registry'\n")
        file.write("{0}<<EOF\n".format(mmEC2Instance1Ssh))
        file.write("cd /tmp/kafka\n")
        file.write("touch /tmp/stackoutputs.txt\n")
        file.write("python3 generatePropertiesFiles.py --region {0} --stackName {1}\n".format(region, stackName))
        errorCheck(file)
        file.write("sudo systemctl is-active confluent-schema-registry --quiet||sudo systemctl start confluent-schema-registry\n")
        errorCheck(file)
        file.write("sudo systemctl status confluent-schema-registry\n")
        file.write("cd /home/ec2-user/mm\n")
        file.write("/home/ec2-user/mm/mm-confluent-kafka-create-topic.sh >/tmp/source_cluster_create_topic_output.txt\n")
        errorCheckEC2Instance1(file)
        file.write("echo 'LABROLE=\$(grep \`hostname\` /tmp/stackoutputs.txt|cut -d \" \" -f 1|grep MM|cut -c 1-14)' >> ~/.bash_profile\n")
        file.write("echo 'PS1=\"\$LABROLE [\\u@\\h \\W]\\$ \"' >> ~/.bash_profile\n")
        errorCheck(file)
        file.write("EOF\n")
        file.write("\n")
        errorCheck(file)
        file.write("\n")

def mmEC2Instance2(mmEC2Instance2Ssh):
    with open("/tmp/kafka/startup.sh", "a") as file:
        file.write("echo 'Configuring Kafka producer instance'\n")
        file.write("{0}<<EOF\n".format(mmEC2Instance2Ssh))
        file.write("cd /tmp/kafka\n")
        file.write("python3 generatePropertiesFiles.py --region {0} --stackName {1}\n".format(region, stackName))
        errorCheck(file)
        # file.write("echo 'Sleeping for 30 seconds ...'\n")
        # file.write("sleep 30\n")
        # file.write("java -jar KafkaClickstreamClient-1.0-SNAPSHOT.jar -t ExampleTopic -pfp /tmp/kafka/producer.properties -nt 8 -rf 60\n")
        #errorCheck(file)
        file.write("echo 'LABROLE=\$(grep \`hostname\` /tmp/stackoutputs.txt|cut -d \" \" -f 1|grep MM|cut -c 1-14)' >> ~/.bash_profile\n")
        file.write("echo 'PS1=\"\$LABROLE [\\u@\\h \\W]\\$ \"' >> ~/.bash_profile\n")
        errorCheck(file)
        file.write("EOF\n")
        file.write("\n")
        errorCheck(file)
        file.write("\n")




for output in result['Stacks'][0]['Outputs']:
    if  (output['OutputKey'] == "Zoo1PrivateDNS"):
        zoo1SSH = "ssh -A ec2-user@" + output['OutputValue']
    if  (output['OutputKey'] == "Zoo2PrivateDNS"):
        zoo2SSH = "ssh -A ec2-user@" + output['OutputValue']
    if  (output['OutputKey'] == "Zoo3PrivateDNS"):
        zoo3SSH = "ssh -A ec2-user@" + output['OutputValue']
    if  (output['OutputKey'] == "ConfluentKafka1PrivateDNS"):
        confluentKafka1Ssh = "ssh -A ec2-user@" + output['OutputValue']
    if  (output['OutputKey'] == "ConfluentKafka2PrivateDNS"):
        confluentKafka2Ssh = "ssh -A ec2-user@" + output['OutputValue']
    if  (output['OutputKey'] == "ConfluentKafka3PrivateDNS"):
        confluentKafka3Ssh = "ssh -A ec2-user@" + output['OutputValue']
    if  (output['OutputKey'] == "MMEC2Instance1PrivateDNS"):
        mmEC2Instance1Ssh = "ssh -A ec2-user@" + output['OutputValue']
    if  (output['OutputKey'] == "MMEC2Instance2PrivateDNS"):
        mmEC2Instance2Ssh = "ssh -A ec2-user@" + output['OutputValue']


zoo(zoo1SSH)
zoo(zoo2SSH)
zoo(zoo3SSH)
confluentKafka(confluentKafka1Ssh)
confluentKafka(confluentKafka2Ssh)
confluentKafka(confluentKafka3Ssh)
mmEC2Instance1(mmEC2Instance1Ssh)
mmEC2Instance2(mmEC2Instance2Ssh)
os.chmod("/tmp/kafka/startup.sh", 0o775)
    

