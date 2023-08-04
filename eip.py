import boto3
import yaml
from add_to_csv import add_to_csv
from prometheus_client import Gauge



# Load the config file
with open('config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)
    
region_name = config['region_name']

region_name = region_name
cloudwatch_client = boto3.client('cloudwatch', region_name = region_name)

region_metric = Gauge(
    'aws_region_name',
    'aws_region', 
    labelnames=['aws_region']
)

orphaned_eips_metric = Gauge(
    'orphaned_eips',
    'Number of orphaned Elastic IPs', 
    labelnames=['eip_id']
)
def get_orphaned_eips():

    print("\nFinding orphaned Elastic IPs...")
    ec2 = boto3.client('ec2', region_name= region_name)
    region_metric.labels(aws_region= region_name).set(1)

    # Retrieve all allocated Elastic IPs
    response = ec2.describe_addresses()
    allocated_eips = [address['PublicIp'] for address in response['Addresses']]
    
    # Retrieve all associated Elastic IPs
    orphan_eip = set()
    associated_eips = set()
    response = ec2.describe_instances()
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            if 'PublicIpAddress' in instance:
                associated_eips.add(instance['PublicIpAddress'])
    
    # Compare allocated and associated EIPs to find orphaned ones
    orphaned_eips = set(allocated_eips) - associated_eips

    
    for eip in orphaned_eips:
        orphan_eip.add(('EIP', eip))
        orphaned_eips_metric.labels(eip_id=eip).set(1)



    add_to_csv((orphan_eip))
    return orphaned_eips

