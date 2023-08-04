from prometheus_client import start_http_server
from ec2 import detect_orphan_ec2_instances
from volumes import detect_orphan_volumes
from elb import detect_orphaned_load_balancers
from eip import get_orphaned_eips
from rds import detect_orphaned_rds_instances
import time
import pandas as pd

if __name__ == '__main__':
   
   start_http_server(8090)

   while True:
    orphaned_eips = get_orphaned_eips()
    orphan_instances = set()
    detect_orphan_ec2_instances()
    orphan_volumes = set()
    detect_orphan_volumes()
    orphaned_lbs= detect_orphaned_load_balancers()
    orphan_databases = set()
    detect_orphaned_rds_instances()
    time.sleep(86400)

#Add to excel
    def add_to_excel(): 
       output_file = 'orphaned_resources.xlsx'
       writer= pd.ExcelWriter(output_file, engine='xlsxwriter')

       ec2_headers = ['instanceID', 'DiskReadOps', 'DiskWriteOps', 'CPU_Util', 'DiskReadBytes', 'DiskWriteBytes', 'StatusCheckFailed']
       ec2_metrics_df = pd.DataFrame.from_records(list(instance[2] for instance in orphan_instances), columns=ec2_headers)
       ec2_metrics_df.to_excel(writer, sheet_name='EC2', index=False)

       vol_headers = ['volumeID', 'Attachment-date', 'ReadOps', 'WriteOps', 'IdleTime', 'BurstBalance']
       vol_metrics_df = pd.DataFrame.from_records(list(volume[2] for volume in orphan_volumes), columns=vol_headers)
       vol_metrics_df.to_excel(writer, sheet_name='VOLUMES', index=False)

       rds_headers = ['rdsID', 'Status', 'ARN', 'Tags', 'DBConnections', 'ReadLatency', 'WriteLatency', 'BurstBalance', 'FreeableMem', 'FreeStorageSpace', 'cpuSurplus', 'ebsByteBalance', 'ebsIOBalance']
       rds_metrics_df = pd.DataFrame.from_records(list(db[2] for db in orphan_databases), columns=rds_headers)
       rds_metrics_df.to_excel(writer, sheet_name='RDS', index=False)

       ip_addresses = [(str(ip), 'available') for ip in orphaned_eips]
       eip_metrics_df = pd.DataFrame(ip_addresses, columns=['EIP','Status'])
       eip_metrics_df.to_excel(writer, sheet_name='EIP', index=False)

       elb_headers = ['elbID', 'HealthyHostCount', 'RequestCount']
       elb_metrics_df = pd.DataFrame.from_records(list(elb[2] for elb in orphaned_lbs), columns=elb_headers)
       elb_metrics_df.to_excel(writer, sheet_name='ELB', index=False)

       writer._save()

    add_to_excel()
