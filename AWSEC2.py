import boto3
from AWSUtils import getTag,WARNTIME
from datetime import datetime, timedelta, time
from dateutil.tz import tzutc
DEFAULTREGION='eu-west-1'


class AWS_EC2:

    def __init__(self,apikey,apisecret,REGION=DEFAULTREGION):
        self.apikey = apikey
        self.apisecret = apisecret
        self.region = DEFAULTREGION
        self.awsclient = boto3.resource('ec2', aws_access_key_id=self.apikey,aws_secret_access_key=self.apisecret,region_name=REGION)
        self.cloudwatch = boto3.client("cloudwatch", aws_access_key_id=self.apikey,aws_secret_access_key=self.apisecret,region_name=self.region)
        

    def getMetricsCPU(self,instances, start,end):
        result = []
        total=0
        for instance in instances:
            total+=1
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='CPUUtilization',
                Dimensions=[
                    {
                    'Name': 'InstanceId',
                    'Value': instance.id
                    },
                ],
                StartTime=start,
                EndTime=end,
                Period=480,
                Statistics=[
                    'Average',
                ],
                Unit='Percent'
            )

            if len(response['Datapoints'])!=0:
                name = getTag('Name',instance.tags)
                if not name:
                    name = instance.id
                name += ' ( ' + instance.instance_type + ' )'
                result.append([name])
        return (result,total)

    def getEC2NotPoweredOff(self):
        start = datetime.combine((datetime.now() - timedelta(seconds=86400)).date(),time(22,0))
        end   = datetime.combine((datetime.now()).date(),time(4,0))
        (resultNight,totalNight) = self.getMetricsCPU(self.awsclient.instances.all(),start,end)
        start = datetime.combine((datetime.now() - timedelta(seconds=86400)).date(),time(8,0))
        end   = datetime.combine((datetime.now() - timedelta(seconds=86400)).date(),time(20,0))
        (result,total) = self.getMetricsCPU(self.awsclient.instances.all(),start,end)
        return(resultNight,totalNight,result)


    def getEc2Inventory(self):
        inventory = []
        DATE=datetime.now().strftime("%Y-%m-%d")
        for instance in self.awsclient.instances.all():
            if instance.state['Code']==16: # Is started
                tags    = instance.tags
                if tags!= None:
                    name = getTag('Name',tags)
                else:
                    name='UNKNOWN'

                ec2type = instance.instance_type
                os = instance.platform
                if os == None:
                    OS = getTag('OS',tags)
                    if OS!=None and OS.lower().find('red hat')!=-1:
                        os = 'Red Hat Enterprise Linux'
                    else:
                        os = 'Linux/UNIX'
                else:
                    os='Windows'

                application = getTag('Application',tags)
                newapp = getTag('New_service',tags)
                if newapp != None and newapp.upper() in ['YES','1','TRUE']:
                    newapp = True
                else:
                    newapp = False

                inventory.append([True, os, DATE, ec2type, newapp, application, name,False])
        
        return inventory


    ###############################################################################################
    # Get EC2 RI Inventory
    ###############################################################################################
    def getEc2RI(self,WARNTIME=30):
        reserved_instances = {}
        soon_expire_ri = {}
        awsclient = boto3.client('ec2', aws_access_key_id=self.apikey,aws_secret_access_key=self.apisecret,region_name=self.region)

        reservations = awsclient.describe_reserved_instances()
        now = datetime.utcnow().replace(tzinfo=tzutc())
        for ri in reservations['ReservedInstances']:
            if ri['State']=='active':
                key = (ri['ProductDescription'], ri['InstanceType'],ri['AvailabilityZone'] if 'AvailabilityZone' in ri else self.region)
                reserved_instances[key] = reserved_instances.get(key, 0) + ri['InstanceCount']
                expire_time = ri['Start'] + timedelta(seconds=ri['Duration'])
                if (expire_time - now) < timedelta(days=WARNTIME):
                    soon_expire_ri[ri['ReservedInstancesId']] = key + (expire_time,)
        return (reserved_instances,soon_expire_ri)

    ###############################################################################################
    # Check for unattached EBS
    ###############################################################################################
    def getMetrics(self,volume_id,start_date,today):  
        """Get volume idle time on an individual volume over `start_date`
        to today"""
        metrics = self.cloudwatch.get_metric_statistics(
            Namespace='AWS/EBS',
            MetricName='VolumeIdleTime',
            Dimensions=[{'Name': 'VolumeId', 'Value': volume_id}],
            Period=3600,  # every hour
            StartTime=start_date,
            EndTime=today,
            Statistics=['Minimum'],
            Unit='Seconds'
        )
        return metrics['Datapoints']

    def isCandidate(self,volume_id,start_date,today):  
        """Make sure the volume has not been used in the past two weeks"""
        metrics = self.getMetrics(volume_id,start_date,today)
        if len(metrics):
            for metric in metrics:
                if metric['Minimum'] < 299:
                    return False
        return True

    def getAvailableVolumes(self,EBSUNATTACHEDDAYWARN=WARNTIME):
        today = datetime.now() + timedelta(days=1)   
        two_weeks = timedelta(days=EBSUNATTACHEDDAYWARN)  
        start_date = today - two_weeks
        available_volumes = self.awsclient.volumes.filter(
            Filters=[{'Name': 'status', 'Values': ['available']}]
        )
        candidate_volumes = [volume.volume_id for volume in available_volumes if self.isCandidate(volume.volume_id,start_date,today)]
        return candidate_volumes



if __name__ == "__main__":
    print('Must be used with other script')