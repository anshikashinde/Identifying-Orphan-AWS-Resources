import boto3
import datetime
import yaml
from add_to_csv import add_to_csv
from prometheus_client import Gauge



# Load the config file
with open('config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)

region_name = config['region_name']
threshold_days = config['threshold_days']
dbconnections_threshold = config['dbconnections_threshold']
readLatency_threshold = config['readLatency_threshold']
writeLatency_threshold = config['writeLatency_threshold']
freeableMem_threshold = config['freeableMem_threshold']
freeStorage_threhsold = config['freeStorage_threhsold']
cpuSurplus_threshold = config['cpuSurplus_threshold']
burstBalance_threshold = config['burstBalance_threshold']
ebsIOBalance_threshold = config['ebsIOBalance_threshold']
ebsByteBalance_threshold = config['ebsByteBalance_threshold']

cloudwatch_client = boto3.client('cloudwatch', region_name = region_name)
end_time = datetime.datetime.utcnow()
start_time = end_time - datetime.timedelta(days=1)

def get_db_metrics(db_instance_identifier):
    # Check the active database connections metric
    response = cloudwatch_client.get_metric_statistics(
    Namespace='AWS/RDS',
    MetricName='DatabaseConnections',
    Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_identifier}],
    StartTime=start_time,
    EndTime=end_time,
    Period=3600,
    Statistics=['Sum']
    )
    db_connections = response['Datapoints'][0]['Sum']
 
    # Check Read Latency
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/RDS',
        MetricName='ReadLatency',
        Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_identifier}],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Average']
    )
    read_latency = response['Datapoints'][0]['Average']

    # Check Write Latency
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/RDS',
        MetricName='WriteLatency',
        Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_identifier}],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Average']
    )
    write_latency = response['Datapoints'][0]['Average']

    # Check Burst Balance
    response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/RDS',
            MetricName='BurstBalance',
            Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_identifier}],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,
            Statistics=['Average']
        )
    burst_balance = response['Datapoints'][0]['Average']

    # Check Freeable Memory
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/RDS',
        MetricName='FreeableMemory',
        Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_identifier}],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Average']
    )
    freeable_mem = response['Datapoints'][0]['Average']

    # Check Free Storage Space
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/RDS',
        MetricName='FreeStorageSpace',
        Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_identifier}],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Average']
    )
    free_storage_space = response['Datapoints'][0]['Average']

    # Check CPU Surplus Credit Balance
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/RDS',
        MetricName='CPUSurplusCreditBalance',
        Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_identifier}],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Average']
    )
    cpu_surplus = response['Datapoints'][0]['Average'] 

    # Check EBS IO Balance
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/RDS',
        MetricName='EBSIOBalance%',
        Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_identifier}],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Average']
    )
    ebs_io_balance = response['Datapoints'][0]['Average']

     # Check EBS Byte Balance
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/RDS',
        MetricName='EBSByteBalance%',
        Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_instance_identifier}],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Average']
    )
    ebs_byte_balance = response['Datapoints'][0]['Average']

    db_metrics = dict()
    db_metrics['db_ID'] = db_instance_identifier
    db_metrics['DBConnections'] = db_connections
    db_metrics['ReadLatency'] = read_latency
    db_metrics['WriteLatency'] = write_latency
    db_metrics['BurstBalance'] = burst_balance
    db_metrics['FreeableMem'] = freeable_mem
    db_metrics['FreeStorageSpace'] = free_storage_space
    db_metrics['cpuSurplus'] = cpu_surplus
    db_metrics['ebsByteBalance'] = ebs_byte_balance
    db_metrics['ebsIOBalance'] = ebs_io_balance


    return db_metrics
orphaned_rds_metric = Gauge(
            'orphaned_db',
            'Orphaned RDS instances',
            ['db_id', 'DBConnections','ReadLatency','WriteLatency', 'BurstBalance','FreeableMem', 'FreeStorageSpace', 'cpuSurplus','ebsByteBalance','ebsIOBalance' ]
            )    
def detect_orphaned_rds_instances():
    print("\nFinding orphaned RDS Instances...")
    orphan_databases = set()
    rds = boto3.client('rds', region_name= region_name)
    
    # Retrieve all RDS instances
    response = rds.describe_db_instances()
    db_instances = response['DBInstances']



    for db_instance in db_instances:
        db_instance_identifier = db_instance['DBInstanceIdentifier']        
        db_metrics = get_db_metrics(db_instance_identifier)

        db_metrics['Status'] = db_instance['DBInstanceStatus']
        db_metrics['ARN'] = db_instance['DBInstanceArn']
        db_createTime = db_instance['InstanceCreateTime']
        db_metrics['Tags'] = tuple(db_instance.get('TagList', []))
        
        threshold_days = 30
        current_time = datetime.datetime.now(db_createTime.tzinfo)
        time_difference = current_time - db_createTime

        required_tags = ['Project', 'Environment']  # Specify your required tags here
        missing_tags = [tag for tag in required_tags if tag not in [t['Key'] for t in db_metrics['Tags']]]

        response = rds.describe_db_instances()
        db_instances = response['DBInstances']

        # Condition 1: Check instance status
        if db_metrics['Status'] not in ['available', 'stopped']:
            orphan_databases.add(('RDS',db_instance_identifier, db_metrics.values()))
            
        # Condition 2: Verify instance identifier
        elif not db_instance_identifier:
            orphan_databases.add(('RDS',db_instance_identifier, db_metrics.values()))
            
        # Condition 3: Ensure valid instance ARN
        elif not db_metrics['ARN']:
            orphan_databases.add(('RDS',db_instance_identifier, db_metrics.values()))    
        
        # Condition 4: Check instance create time
        # Here, we consider instances older than 30 days as potentially orphaned

        elif time_difference.days > threshold_days:
            orphan_databases.add(('RDS',db_instance_identifier, db_metrics.values()))
            orphaned_rds_metric.labels(db_id=db_metrics['db_ID'], DBConnections=db_metrics['DBConnections'], ReadLatency=db_metrics['ReadLatency'], WriteLatency=db_metrics['WriteLatency'], BurstBalance=db_metrics['BurstBalance'], FreeableMem=db_metrics['FreeableMem'], FreeStorageSpace=db_metrics['FreeStorageSpace'], cpuSurplus= db_metrics['cpuSurplus'] , ebsByteBalance=db_metrics['ebsByteBalance'],ebsIOBalance=db_metrics['ebsIOBalance']).set(1)


        # Condition 5: Verify instance tags
        elif missing_tags:
            orphan_databases.add(('RDS', db_instance_identifier, db_metrics.values()))
            orphaned_rds_metric.labels(db_id=db_metrics['db_ID'], DBConnections=db_metrics['DBConnections'], ReadLatency=db_metrics['ReadLatency'], WriteLatency=db_metrics['WriteLatency'], BurstBalance=db_metrics['BurstBalance'], FreeableMem=db_metrics['FreeableMem'], FreeStorageSpace=db_metrics['FreeStorageSpace'], cpuSurplus= db_metrics['cpuSurplus'] , ebsByteBalance=db_metrics['ebsByteBalance'],ebsIOBalance=db_metrics['ebsIOBalance']).set(1)

        elif db_metrics['DBConnections'] == dbconnections_threshold:
            # No active database connections recorded in the last 5 minutes
            orphan_databases.add(('RDS',db_instance_identifier, db_metrics.values()))
            orphaned_rds_metric.labels(db_id=db_metrics['db_ID'], DBConnections=db_metrics['DBConnections'], ReadLatency=db_metrics['ReadLatency'], WriteLatency=db_metrics['WriteLatency'], BurstBalance=db_metrics['BurstBalance'], FreeableMem=db_metrics['FreeableMem'], FreeStorageSpace=db_metrics['FreeStorageSpace'], cpuSurplus= db_metrics['cpuSurplus'] , ebsByteBalance=db_metrics['ebsByteBalance'],ebsIOBalance=db_metrics['ebsIOBalance']).set(1)

        elif db_metrics['ReadLatency'] < readLatency_threshold:
            # Average read latency is high (greater than 10ms) or no data points available
            orphan_databases.add(('RDS',db_instance_identifier, db_metrics.values()))
            orphaned_rds_metric.labels(db_id=db_metrics['db_ID'], DBConnections=db_metrics['DBConnections'], ReadLatency=db_metrics['ReadLatency'], WriteLatency=db_metrics['WriteLatency'], BurstBalance=db_metrics['BurstBalance'], FreeableMem=db_metrics['FreeableMem'], FreeStorageSpace=db_metrics['FreeStorageSpace'], cpuSurplus= db_metrics['cpuSurplus'] , ebsByteBalance=db_metrics['ebsByteBalance'],ebsIOBalance=db_metrics['ebsIOBalance']).set(1)
            
        elif db_metrics['WriteLatency'] < writeLatency_threshold:
            # Average write latency is high (greater than 10ms) or no data points available
            orphan_databases.add(('RDS',db_instance_identifier, db_metrics.values())) 
            orphaned_rds_metric.labels(db_id=db_metrics['db_ID'], DBConnections=db_metrics['DBConnections'], ReadLatency=db_metrics['ReadLatency'], WriteLatency=db_metrics['WriteLatency'], BurstBalance=db_metrics['BurstBalance'], FreeableMem=db_metrics['FreeableMem'], FreeStorageSpace=db_metrics['FreeStorageSpace'], cpuSurplus= db_metrics['cpuSurplus'] , ebsByteBalance=db_metrics['ebsByteBalance'],ebsIOBalance=db_metrics['ebsIOBalance']).set(1)

        elif db_metrics['FreeableMem'] < freeableMem_threshold:
            # Freeable memory is low (less than 1GB) or no data points available
            orphan_databases.add(('RDS',db_instance_identifier, db_metrics.values()))
            orphaned_rds_metric.labels(db_id=db_metrics['db_ID'], DBConnections=db_metrics['DBConnections'], ReadLatency=db_metrics['ReadLatency'], WriteLatency=db_metrics['WriteLatency'], BurstBalance=db_metrics['BurstBalance'], FreeableMem=db_metrics['FreeableMem'], FreeStorageSpace=db_metrics['FreeStorageSpace'], cpuSurplus= db_metrics['cpuSurplus'] , ebsByteBalance=db_metrics['ebsByteBalance'],ebsIOBalance=db_metrics['ebsIOBalance']).set(1)
             
        elif db_metrics['FreeStorageSpace'] < freeStorage_threhsold:
            # Free storage space is low (less than 1GB) or no data points available
            orphan_databases.add(('RDS',db_instance_identifier, db_metrics.values())) 
            orphaned_rds_metric.labels(db_id=db_metrics['db_ID'], DBConnections=db_metrics['DBConnections'], ReadLatency=db_metrics['ReadLatency'], WriteLatency=db_metrics['WriteLatency'], BurstBalance=db_metrics['BurstBalance'], FreeableMem=db_metrics['FreeableMem'], FreeStorageSpace=db_metrics['FreeStorageSpace'], cpuSurplus= db_metrics['cpuSurplus'] , ebsByteBalance=db_metrics['ebsByteBalance'],ebsIOBalance=db_metrics['ebsIOBalance']).set(1)

        elif db_metrics['cpuSurplus'] == cpuSurplus_threshold:
            # CPU surplus credit balance is low (less than 10) or no data points available
            orphan_databases.add(('RDS',db_instance_identifier, db_metrics.values()))
            orphaned_rds_metric.labels(db_id=db_metrics['db_ID'], DBConnections=db_metrics['DBConnections'], ReadLatency=db_metrics['ReadLatency'], WriteLatency=db_metrics['WriteLatency'], BurstBalance=db_metrics['BurstBalance'], FreeableMem=db_metrics['FreeableMem'], FreeStorageSpace=db_metrics['FreeStorageSpace'], cpuSurplus= db_metrics['cpuSurplus'] , ebsByteBalance=db_metrics['ebsByteBalance'],ebsIOBalance=db_metrics['ebsIOBalance']).set(1)
           
        elif db_metrics['BurstBalance'] <= burstBalance_threshold:
            # Burst balance is low (less than 20%) or no data points available
            orphan_databases.add(('RDS',db_instance_identifier, db_metrics.values()))
            orphaned_rds_metric.labels(db_id=db_metrics['db_ID'], DBConnections=db_metrics['DBConnections'], ReadLatency=db_metrics['ReadLatency'], WriteLatency=db_metrics['WriteLatency'], BurstBalance=db_metrics['BurstBalance'], FreeableMem=db_metrics['FreeableMem'], FreeStorageSpace=db_metrics['FreeStorageSpace'], cpuSurplus= db_metrics['cpuSurplus'] , ebsByteBalance=db_metrics['ebsByteBalance'],ebsIOBalance=db_metrics['ebsIOBalance']).set(1)

        elif db_metrics['ebsIOBalance'] <= ebsIOBalance_threshold:
            # EBS IO Balance is high (greater than 50%) or no data points available
            orphan_databases.add(('RDS',db_instance_identifier, db_metrics.values()))
            orphaned_rds_metric.labels(db_id=db_metrics['db_ID'], DBConnections=db_metrics['DBConnections'], ReadLatency=db_metrics['ReadLatency'], WriteLatency=db_metrics['WriteLatency'], BurstBalance=db_metrics['BurstBalance'], FreeableMem=db_metrics['FreeableMem'], FreeStorageSpace=db_metrics['FreeStorageSpace'], cpuSurplus= db_metrics['cpuSurplus'] , ebsByteBalance=db_metrics['ebsByteBalance'],ebsIOBalance=db_metrics['ebsIOBalance']).set(1)
           
        elif db_metrics['ebsByteBalance'] <= ebsByteBalance_threshold:
            # EBS Byte Balance is high (greater than 50%) or no data points available
            orphan_databases.add(('RDS',db_instance_identifier, db_metrics.values()))
            orphaned_rds_metric.labels(db_id=db_metrics['db_ID'], DBConnections=db_metrics['DBConnections'], ReadLatency=db_metrics['ReadLatency'], WriteLatency=db_metrics['WriteLatency'], BurstBalance=db_metrics['BurstBalance'], FreeableMem=db_metrics['FreeableMem'], FreeStorageSpace=db_metrics['FreeStorageSpace'], cpuSurplus= db_metrics['cpuSurplus'] , ebsByteBalance=db_metrics['ebsByteBalance'],ebsIOBalance=db_metrics['ebsIOBalance']).set(1)
            
    add_to_csv(orphan_databases)
    return orphan_databases


