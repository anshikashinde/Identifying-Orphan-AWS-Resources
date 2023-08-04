# Summer Internship 2023

## Project Title
Identification & Visualisation of Orphan AWS Resources

## Description
This is a python script to identify orphan EC2, Volumes, RDS, Elastic Load Balancers & EIPs in a AWS region.
It alanyzes the CloudWatch metrics to classify resources as orphan & adds them to a CSV and excel file.

It exposes Prometheus metrics of orphan resources on an endpoint. Grafana will use Prometheus metrics as datasource to create a dashboard displaying the orphan resources along with its corresponding CloudWatch metrics.


## Pre-requisties
1. AWS Programmatic access
2. Docker Desktop

## Local Project Setup

1. Install all the Dependencies, run command - 
```
pip3 install -r requirements.txt
```
2. Setup Grafana and Prometheus using docker -  
```  
docker run -d --name=grafana -p 3000:3000 --link 
prometheus:prometheus grafana/grafana  
```
You can see containers being created on your Docker Desktop. Also make sure that port 3000 is not already in use.  

 3. On docker desktop go to   
prometheus > files > prometheus > prometheus.yml   

    add the below code under scrape_configs with proper indentation:   
 ```
    -job_name: "orphaned_resources"  
       metrics_path: /  
       static_configs:  
          -targets: ["host.docker.internal:8090"]
 ```   

restart the prometheus container upon adding this.

4. Run the python file -  identify_orphaned_resources.py

  For making any changes in variables go to config.yml file



