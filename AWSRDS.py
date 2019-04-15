import boto3
from AWSUtils import getTag,WARNTIME,DEFAULTREGION
from datetime import datetime, timedelta, time
from dateutil.tz import tzutc

class AWS_RDS:
    def __init__(self,apikey,apisecret,REGION=DEFAULTREGION):
        self.apikey = apikey
        self.apisecret = apisecret
        self.region = DEFAULTREGION
        self.awsclient = boto3.client('rds', aws_access_key_id=apikey,aws_secret_access_key=apisecret,region_name=REGION)

    def getRdsInventory(self):
        DATE=datetime.now().strftime("%Y-%m-%d")
        inventory = []
        instances = self.awsclient.describe_db_instances()['DBInstances']
        for instance in instances:
            if instance['DBInstanceStatus'] == 'available':
                multiaz = instance['MultiAZ']
                engine = instance['Engine'].replace('postgres','postgresql')
                arn = instance['DBInstanceArn']
                tags = self.awsclient.list_tags_for_resource(ResourceName=arn)['TagList']

                name = getTag('Name',tags)
                application = getTag('Application',tags)
                newapp = getTag('New_service',tags)
                if newapp != None and newapp.upper() in ['YES','1','TRUE']:
                    newapp = True
                else:
                    newapp = False

                inventory.append([False,engine,DATE,instance['DBInstanceClass'],newapp, application, name,multiaz])
        return inventory

    def getRdsRI(self) :

        soon_expire_ri = {}
        reserved_instances = {}

        reserved_rds_instances = self.awsclient.describe_reserved_db_instances()
        reservations = reserved_rds_instances['ReservedDBInstances']  
        now = datetime.utcnow().replace(tzinfo=tzutc())
        for ri in reservations:
            ri_id = ri['ReservedDBInstanceId']
            ri_type = ri['DBInstanceClass']
            ri_count = ri['DBInstanceCount']
            ri_multiaz = ri['MultiAZ']
            ri_engine = ri['ProductDescription'].replace('(li)','').replace('(byod)','')
            key = (ri_engine,ri_type, ri_multiaz)
            reserved_instances[key] = reserved_instances.get(key, 0) + ri_count
            ri_start_time = ri['StartTime']
            expire_time = ri_start_time +timedelta(seconds=ri['Duration'])
            if (expire_time - now) < timedelta(days=WARNTIME):
                soon_expire_ri[ri_id] = (ri_engine,ri_type, 'eu-west-1', expire_time)
        return (reserved_instances,soon_expire_ri)

if __name__ == "__main__":
    print('Must be used with other script')
