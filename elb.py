import boto3
import datetime
from add_to_csv import add_to_csv
import yaml
from prometheus_client import Gauge


# Load the config file
with open('config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)
    
region_name = config['region_name']
healthyHostCount_threshold = config['healthyHostCount_threshold']
requestCount_threshold = config['requestCount_threshold']

cloudwatch_client = boto3.client('cloudwatch', region_name = region_name)


def get_elb_metrics(lb_arn):
    # Check the HealthyHostCount metric
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/ApplicationELB',
        MetricName='HealthyHostCount',
        Dimensions=[
            {
                'Name': 'LoadBalancer',
                'Value': lb_arn.split('/')[1]
            },
        ],
        StartTime=datetime.datetime.utcnow() - datetime.timedelta(days=1),
        EndTime=datetime.datetime.utcnow(),
        Period=86400, 
        Statistics=['Average']
    )
    if response['Datapoints']:
        elb_health = response['Datapoints'][0]['Average']
    else:
        elb_health = 0

    # Check the RequestCount metric
    response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/ApplicationELB',
        MetricName='RequestCount',
        Dimensions=[
            {
                'Name': 'LoadBalancer',
                'Value': lb_arn.split('/')[1]
            },
        ],
        StartTime=datetime.datetime.utcnow() - datetime.timedelta(days = 1),
        EndTime=datetime.datetime.utcnow(),
        Period=86400,
        Statistics=['Sum']
    )
    if response['Datapoints']:
        elb_request_count = response['Datapoints'][0]['Sum']
    else:
        elb_request_count = 0
    
    elb_metrics = dict()
    elb_metrics['elb_arn'] = lb_arn
    elb_metrics['HealthyHostCount'] = elb_health
    elb_metrics['RequestCount'] = elb_request_count

    return elb_metrics

orphaned_elb_metric = Gauge(
    'orphaned_elb',
    'Number of orphaned elb',
    ['elb_id']
    )
def detect_orphaned_load_balancers():
    print("\nFinding orphaned Load Balancers...")
    elbv2_client = boto3.client('elbv2', region_name = region_name)

    # Retrieve the list of load balancers
    response = elbv2_client.describe_load_balancers()
    load_balancers = response['LoadBalancers']

    orphaned_load_balancers = []
    potentially_orphaned_load_balancers = []

    
    for lb in load_balancers:
        lb_arn = lb['LoadBalancerArn']
        elb_id = lb_arn.split(':')[-1]

        lb_metrics = get_elb_metrics(lb_arn)

        elb_id = lb_arn.split(':')[-1]
        orphaned_elb_metric.labels(elb_id=elb_id).set(lb_metrics['HealthyHostCount'])
        orphaned_elb_metric.labels(elb_id=elb_id).set(lb_metrics['RequestCount'])       
  
        if not lb_metrics['HealthyHostCount'] or lb_metrics['HealthyHostCount'][0]['Average']  == healthyHostCount_threshold:
            # No healthy hosts recorded in the last 5 minutes
            orphaned_load_balancers.append(('ELB', elb_id, lb_metrics.values()))
        else:
            if not lb_metrics['RequestCount'] or lb_metrics['RequestCount'][0]['Sum'] < requestCount_threshold:
                # Request count is less than 500 in the last 5 minutes
                potentially_orphaned_load_balancers.append(('Potential ELB', elb_id, lb_metrics.values()))
                add_to_csv(potentially_orphaned_load_balancers)

    add_to_csv(orphaned_load_balancers)

    return orphaned_load_balancers


