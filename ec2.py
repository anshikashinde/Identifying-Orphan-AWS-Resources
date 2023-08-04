import boto3
import datetime
import yaml
import time
from add_to_csv import add_to_csv
from prometheus_client import Gauge


# Load the config file
with open('config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)

region_name = config['region_name']
diskReadOps = config['diskReadOps']
diskWriteOps = config['diskWriteOps']
cpu_utilisation_threshold = config['cpu_utilisation_threshold']
diskReadBytes = config['diskReadBytes']
diskWriteBytes = config['diskWriteBytes']
min_statusCheckFailed = config['min_statusCheckFailed']   
 
cloudwatch_client = boto3.client('cloudwatch', region_name = region_name)


def get_instance_name(tags):
    for tag in tags:
        if tag['Key'] == 'Name':
            return tag['Value']
    return ''

def get_ec2_metrics(instance_id):
    end_time = datetime.datetime.utcnow()
    start_time = end_time - datetime.timedelta(days=1)

    # Check disk read ops
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/EC2',
        MetricName='DiskReadOps',
        Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
        StartTime=start_time,
        EndTime=end_time,
        Period=86400,
        Statistics=['Average']
    )
    read_ops = response['Datapoints'][0]['Average'] if response['Datapoints'] else 0

    # Check disk write ops
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/EC2',
        MetricName='DiskWriteOps',
        Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
        StartTime=start_time,
        EndTime=end_time,
        Period=86400,
        Statistics=['Average']
    )
    write_ops = response['Datapoints'][0]['Average'] if response['Datapoints'] else 0

    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/EC2',
        MetricName='CPUUtilization',
        Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
        StartTime=start_time,
        EndTime=end_time,
        Period=86400,
        Statistics=['Average']
    )
    if response['Datapoints']:
        datapoints = response['Datapoints']
        avg_cpu_utilization = sum([datapoint['Average'] for datapoint in datapoints]) / len(datapoints)

    response = cloudwatch_client.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='DiskReadBytes',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=86400,
                Statistics=['Average']
            )
    readBytes = response['Datapoints'][0]['Average'] if response['Datapoints'] else 0

    response = cloudwatch_client.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='DiskWriteBytes',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=86400,
                Statistics=['Average']
            )
    writeBytes = response['Datapoints'][0]['Average'] if response['Datapoints'] else 0

    # Check StatusCheckFailed_Instance metric
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/EC2',
        MetricName='StatusCheckFailed_Instance',
        Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
        StartTime=start_time,
        EndTime=end_time,
        Period=86400,
        Statistics=['Sum']
    )
    
    statusCheck = response['Datapoints'][0]['Sum'] if response['Datapoints'] else 0

    instance_metrics = dict()

    instance_metrics['instanceID'] = instance_id
    # instance_metrics['State'] = instance['State']['Name']
    instance_metrics['DiskReadOps'] = read_ops
    instance_metrics['DiskWriteOps'] = write_ops
    instance_metrics['CPU_Util'] = avg_cpu_utilization
    instance_metrics['DiskReadBytes'] = readBytes
    instance_metrics['DiskWriteBytes'] = writeBytes
    instance_metrics['StatusCheckFailed'] = statusCheck

    return instance_metrics

orphaned_ec2_instances_metric = Gauge(
            'orphaned_ec2_instances',
            'Number of orphaned EC2 instances',
            ['instance_id','DiskReadOps', 'DiskWriteOps', 'CPU_Util', 'DiskReadBytes', 'DiskWriteBytes']

            )

def detect_orphan_ec2_instances():

    ec2_client = boto3.client('ec2', region_name = region_name)
    print("\nFinding orphan EC2 Instances...")
    orphan_instances = set()

    # Get all EC2 instances
    response = ec2_client.describe_instances()
    instances = response['Reservations']
    orphan_instances=set()
    
    for reservation in instances:
        for instance in reservation['Instances']:
            instance_metrics = dict()
            instance_id = instance['InstanceId']
            instance_name = get_instance_name(instance.get('Tags', []))
            instance_metrics = get_ec2_metrics(instance_id)

        
            # Check instance status
            if instance['State']['Name'] == 'failed':
                orphan_instances.add(('EC2', instance_id, instance_metrics.values()))
                orphaned_ec2_instances_metric.labels(instance_id=instance_id,DiskReadOps=instance_metrics['DiskReadOps'] , DiskWriteOps=instance_metrics['DiskWriteOps'] , CPU_Util=instance_metrics['CPU_Util'], DiskReadBytes=instance_metrics['DiskWriteBytes'], DiskWriteBytes=instance_metrics['DiskWriteBytes']).set(1)

                
            elif instance_metrics['DiskReadOps'] == diskReadOps:
                # No disk read ops recorded in the last 5 minutes
                orphan_instances.add(('EC2', instance_id, instance_metrics.values()))
                orphaned_ec2_instances_metric.labels(instance_id=instance_id,DiskReadOps=instance_metrics['DiskReadOps'] , DiskWriteOps=instance_metrics['DiskWriteOps'] , CPU_Util=instance_metrics['CPU_Util'], DiskReadBytes=instance_metrics['DiskWriteBytes'], DiskWriteBytes=instance_metrics['DiskWriteBytes']).set(1)

            elif instance_metrics['DiskWriteOps'] == diskWriteOps:
                # No disk write ops recorded in the last 5 minutes
                    orphan_instances.add(('EC2', instance_id, instance_metrics.values()))
                    orphaned_ec2_instances_metric.labels(instance_id=instance_id,DiskReadOps=instance_metrics['DiskReadOps'] , DiskWriteOps=instance_metrics['DiskWriteOps'] , CPU_Util=instance_metrics['CPU_Util'], DiskReadBytes=instance_metrics['DiskWriteBytes'], DiskWriteBytes=instance_metrics['DiskWriteBytes']).set(1)

            elif  instance_metrics['CPU_Util'] < cpu_utilisation_threshold:
                # No CPU utilization recorded in the last 5 minutes\
                orphan_instances.add(('EC2', instance_id, instance_metrics.values()))
                orphaned_ec2_instances_metric.labels(instance_id=instance_id,DiskReadOps=instance_metrics['DiskReadOps'] , DiskWriteOps=instance_metrics['DiskWriteOps'] , CPU_Util=instance_metrics['CPU_Util'], DiskReadBytes=instance_metrics['DiskWriteBytes'], DiskWriteBytes=instance_metrics['DiskWriteBytes']).set(1)

            elif instance_metrics['DiskReadBytes'] == diskReadBytes:
                # No disk read bytes recorded in the last 5 minutes\
                orphan_instances.add(('EC2', instance_id, instance_metrics.values()))
                orphaned_ec2_instances_metric.labels(instance_id=instance_id,DiskReadOps=instance_metrics['DiskReadOps'] , DiskWriteOps=instance_metrics['DiskWriteOps'] , CPU_Util=instance_metrics['CPU_Util'], DiskReadBytes=instance_metrics['DiskWriteBytes'], DiskWriteBytes=instance_metrics['DiskWriteBytes']).set(1)
   
            elif instance_metrics['DiskWriteBytes'] == diskWriteBytes:
                # No disk write bytes recorded in the last 5 minutes
                orphan_instances.add(('EC2', instance_id, instance_metrics.values()))
                orphaned_ec2_instances_metric.labels(instance_id=instance_id,DiskReadOps=instance_metrics['DiskReadOps'] , DiskWriteOps=instance_metrics['DiskWriteOps'] , CPU_Util=instance_metrics['CPU_Util'], DiskReadBytes=instance_metrics['DiskWriteBytes'], DiskWriteBytes=instance_metrics['DiskWriteBytes']).set(1)

            elif instance_metrics['StatusCheckFailed'] >= min_statusCheckFailed:
                # StatusCheckFailed recorded in the last 5 minutes
                orphan_instances.add(('EC2', instance_id, instance_metrics.values()))
                orphaned_ec2_instances_metric.labels(instance_id=instance_id,DiskReadOps=instance_metrics['DiskReadOps'] , DiskWriteOps=instance_metrics['DiskWriteOps'] , CPU_Util=instance_metrics['CPU_Util'], DiskReadBytes=instance_metrics['DiskWriteBytes'], DiskWriteBytes=instance_metrics['DiskWriteBytes']).set(1)
               
              
    add_to_csv(orphan_instances)
    return orphan_instances


    

    





