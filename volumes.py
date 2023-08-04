import boto3
import datetime
import os
import yaml
from add_to_csv import add_to_csv
from prometheus_client import start_http_server, Gauge


os.environ['AWS_PROFILE'] = 'demo-nid'

# Load the config file
with open('config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)

region_name = config['region_name']
threshold1 = config['threshold1']
threshold2 = config['threshold2']

cloudwatch_client = boto3.client('cloudwatch', region_name = region_name)
orphan_volumes = set()
ec2_client = boto3.client('ec2', region_name = region_name)


def get_volume_name(volume_id, ec2_client):
    ec2_client = boto3.client('ec2')
    
    response = ec2_client.describe_volumes(VolumeIds=[volume_id])
    
    if 'Volumes' in response and len(response['Volumes']) > 0:
        volume = response['Volumes'][0]
        if 'Tags' in volume:
            for tag in volume['Tags']:
                if tag['Key'] == 'Name':
                    return tag['Value']
    
    return None

def get_volume_attachment_date(volume_id,ec2_client ):
    
    response = ec2_client.describe_volumes(VolumeIds=[volume_id])
    
    if 'Volumes' in response and len(response['Volumes']) > 0:
        volume = response['Volumes'][0]
        if 'Attachments' in volume and len(volume['Attachments']) > 0:
            attachments = volume['Attachments']
            # Sort attachments by 'AttachTime' in descending order
            sorted_attachments = sorted(attachments, key=lambda x: x['AttachTime'], reverse=True)
            last_attachment = sorted_attachments[0]
            return last_attachment['AttachTime']
    
    return None

def get_volume_metrics(volume_id, start_time, end_time):
    cloudwatch = boto3.client('cloudwatch')

    # Get ReadOps
    response = cloudwatch.get_metric_statistics(
        Namespace='AWS/EBS',
        MetricName='VolumeReadOps',
        Dimensions=[
            {
                'Name': 'VolumeId',
                'Value': volume_id
            },
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Sum']
    )
    read_ops = response['Datapoints'][0]['Sum'] if response['Datapoints'] else 0

    # Get Write Ops
    response = cloudwatch.get_metric_statistics(
        Namespace='AWS/EBS',
        MetricName='VolumeWriteOps',
        Dimensions=[
            {
                'Name': 'VolumeId',
                'Value': volume_id
            },
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Sum']
    )
    write_ops = response['Datapoints'][0]['Sum'] if response['Datapoints'] else 0

    # Get Idle Time
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/EBS',
        MetricName='VolumeIdleTime',
        Dimensions=[{'Name': 'VolumeId', 'Value': volume_id}],
        StartTime=start_time,
        EndTime=end_time,
        Period=86400,  # 1 day
        Statistics=['Average'],
        Unit='Seconds'
    )

    datapoints = response['Datapoints']

    if datapoints:
        average_idle_time = datapoints[0]['Average']
    else:
        average_idle_time = 0
    
    #  Get Burst Balance
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/EBS',
        MetricName='BurstBalance',
        Dimensions=[{'Name': 'VolumeId', 'Value': volume_id}],
        StartTime=start_time,
        EndTime=end_time,
        Period=86400,  # 1 day
        Statistics=['Average']
    )
    datapoints = response['Datapoints']

    if datapoints:
        average_burst_balance = datapoints[0]['Average']
    else:
        average_burst_balance = 0
    
    # Get in_use info
    if average_idle_time > threshold1 and average_burst_balance > threshold2:
        inuse = True
    else:
        inuse =  False

    volume_metrics = dict()
    volume_metrics['VolumeID'] = volume_id
    volume_metrics['ReadOps'] = read_ops
    volume_metrics['WriteOps'] = write_ops
    volume_metrics['IdleTime'] =  average_idle_time
    volume_metrics['BurstBalance'] = average_burst_balance

    return volume_metrics

orphaned_volume_metric = Gauge(
            'orphaned_volumes',
            'Number of orphaned volumes',
            ['volume_id', 'ReadOps','WriteOps','IdleTime', 'BurstBalance']
            )    

def detect_orphan_volumes():
    print("\nFinding orphan Volumes...")

    # Retrieve information about all volumes
    response = ec2_client.describe_volumes()
    volumes = response['Volumes']



    for volume in volumes:
        volume_id = volume['VolumeId']
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(days=1)

        volume_metrics = get_volume_metrics(volume_id, start_time, end_time)
        volume_metrics['Attachment-date'] = get_volume_attachment_date(volume_id,ec2_client)

        average_idle_time = volume_metrics['IdleTime']
        average_burst_balance = volume_metrics['BurstBalance']

        # Check if the volume is not attached to any instance
        if 'Attachments' not in volume or len(volume['Attachments']) == 0:
            # Additional checks for orphan volumes
            if volume.get('State', '') != 'in-use':
                orphan_volumes.add(('VOLUME', volume_id, volume_metrics.values()))
                orphaned_volume_metric.labels(volume_id=volume_id,ReadOps=volume_metrics['ReadOps'],WriteOps=volume_metrics['WriteOps'],IdleTime=volume_metrics['IdleTime'], BurstBalance=volume_metrics['BurstBalance']).set(1)


            elif volume.get('State', '') == 'in-use' and not volume.get('Tags'):
                orphan_volumes.add(('VOLUME', volume_id, volume_metrics.values()))
                orphaned_volume_metric.labels(volume_id=volume_id,ReadOps=volume_metrics['ReadOps'],WriteOps=volume_metrics['WriteOps'],IdleTime=volume_metrics['IdleTime'], BurstBalance=volume_metrics['BurstBalance']).set(1)


            elif volume.get('State', '') == 'in-use' and all(tag['Key'] != 'Name' for tag in volume.get('Tags', [])):
                orphan_volumes.add(('VOLUME', volume_id, volume_metrics.values()))
                orphaned_volume_metric.labels(volume_id=volume_id,ReadOps=volume_metrics['ReadOps'],WriteOps=volume_metrics['WriteOps'],IdleTime=volume_metrics['IdleTime'], BurstBalance=volume_metrics['BurstBalance']).set(1)


            elif average_idle_time < threshold1 or average_burst_balance <= threshold2:
                orphan_volumes.add(('VOLUME', volume_id, volume_metrics.values()))
                orphaned_volume_metric.labels(volume_id=volume_id,ReadOps=volume_metrics['ReadOps'],WriteOps=volume_metrics['WriteOps'],IdleTime=volume_metrics['IdleTime'], BurstBalance=volume_metrics['BurstBalance']).set(1)

        
        # Check if the volume is attached to a stopped instance
        for attachment in volume.get('Attachments', []):
            if attachment.get('State', '') == 'stopped':
                orphan_volumes.add(('VOLUME', volume_id, volume_metrics.values()))
                orphaned_volume_metric.labels(volume_id=volume_id,ReadOps=volume_metrics['ReadOps'],WriteOps=volume_metrics['WriteOps'],IdleTime=volume_metrics['IdleTime'], BurstBalance=volume_metrics['BurstBalance']).set(1)


        
    add_to_csv(orphan_volumes)
    return orphan_volumes


    # output_file = 'orphaned_resources.xlsx'
    # writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
    # vol_headers = ['volumeID', 'Attachment-date', 'ReadOps', 'WriteOps', 'IdleTime', 'BurstBalance']
    # vol_metrics_df = pd.DataFrame.from_records(list(volume[2] for volume in orphan_volumes), columns=vol_headers)
    # vol_metrics_df.to_excel(writer, sheet_name='VOLUMES', index=False)


   



