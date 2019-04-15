#!/usr/bin/python
import boto3
import mysql.connector
from pprint import pprint
import datetime
from dateutil.tz import tzutc
from datetime import datetime, timedelta, time
import six
import sys
from AWSEC2 import AWS_EC2
from AWSRDS import AWS_RDS
from AWSUtils import loadConfigFile

INDATABASE=False

REGION='eu-west-1'
WARNTIME = 30

config = loadConfigFile('config')
ACCOUNT = config['ACCOUNT']
INSTANCE_ACCOUNT = config['INSTANCE_ACCOUNT']
RI_ACCOUNT = config['RI_ACCOUNT']
NONPROD_ACCOUNT = config['NONPROD_ACCOUNT']
EBS_ACCOUNT = dict(INSTANCE_ACCOUNT,**NONPROD_ACCOUNT)

MYSQL_HOST = config['DB']['host']
MYSQL_USER = config['DB']['user']
MYSQL_PASS = config['DB']['pass']
MYSQL_DB   = config['DB']['db']

MANDATORY_TAG=['OS','Name']
DATE=datetime.now().strftime("%Y-%m-%d")


###############################################################################################
# AWS Tag manamgement
###############################################################################################
def getTag(name,tags):
    if tags:
        for tag in tags:
            if tag['Key'].lower()==name.lower():
                return tag['Value']
    return None

def checkMandatoryTag(awsclient):
    result = []
    for instance in awsclient.instances.all():
        tags = instance.tags
        if tags==None:
            result.append("Instance %s without tags" % instance.id)
        else:
            for  t in MANDATORY_TAG:
                if getTag(t,tags)==None:
                    result.append([instance.id,t])

    return result


###############################################################################################
# Get ec2 inventory
###############################################################################################

def getEc2Inventory(cursor):
    ec2inventory = {}
    inventory=[]
    for key in INSTANCE_ACCOUNT:
        ec2 = AWS_EC2(key,INSTANCE_ACCOUNT[key])
        inventory += ec2.getEc2Inventory()

    for instance in inventory:
        os = instance[1]
        ec2type = instance[3]
        region='eu-west-1'
        hashkey=(os, ec2type, region)
        ec2inventory[hashkey]= ec2inventory.get(hashkey, 0) + 1
 
    if INDATABASE:
        query='INSERT INTO AWS_EC2_INVENTORY(ec2,type,date,size,newservice,application,name,AZ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
        cursor.executemany(query,inventory)
        conn.commit()

    return(inventory,ec2inventory)

###############################################################################################
# Get EC2 Reserved instances
###############################################################################################    
def getEC2RI():
    for key in RI_ACCOUNT:
        aws = AWS_EC2(key,RI_ACCOUNT[key])
        return aws.getEc2RI()
###############################################################################################
# Get Unattached EBS
###############################################################################################    
def getAvailableVolumes():
    availableVolumes=[]
    for key in EBS_ACCOUNT:
        account = ACCOUNT[key]
        aws=AWS_EC2(key,EBS_ACCOUNT[key])
        volume = aws.getAvailableVolumes()
        for v in volume:
            availableVolumes.append((account,v))

    return availableVolumes


def getEC2NotPoweredOff():
    # Check of ec2 on during night
    total = 0
    resultNight=[]
    resultDay=[]
    for key in NONPROD_ACCOUNT:
        aws = AWS_EC2(key, NONPROD_ACCOUNT[key])
        (res,tot,res2) = aws.getEC2NotPoweredOff()
        resultNight += res
        total += tot
        resultDay +=res2
    return (resultDay,resultNight,total)

###############################################################################################
# Get RDS  Inventory
###############################################################################################    

def getRdsInventory(cursor):
    inventory = []
    running_instances = {}

    for key in INSTANCE_ACCOUNT:
        aws = AWS_RDS(key,INSTANCE_ACCOUNT[key])
        inventory += aws.getRdsInventory()
    
    for rds in inventory:
        engine = rds[1]
        dbinstance = rds[3]
        az = rds[7]
        key = (engine,dbinstance, az)
        running_instances[key] = running_instances.get(key, 0) + 1

    if INDATABASE:
        query='INSERT INTO AWS_EC2_INVENTORY(ec2,type,date,size,newservice,application,name,AZ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
        cursor.executemany(query,inventory)
        conn.commit()

    return running_instances

###############################################################################################
# Get RDS RI Inventory
###############################################################################################    

def getRDSRI():
    for key in RI_ACCOUNT:
        aws = AWS_RDS(key,RI_ACCOUNT[key])
        return aws.getRdsRI()

   
###############################################################################################
# Compare inventories
###############################################################################################    
def compareRI(reserved_instances,inventory):
    diff = dict([(x, reserved_instances[x] - inventory.get(x, 0))
             for x in reserved_instances])

    # Subtract all region-unspecific RIs.
    for reserved_pkey in reserved_instances:
        if reserved_pkey[2] == REGION:
            # Go through all running instances and subtract.
            for running_pkey in inventory:
                if running_pkey[0] == reserved_pkey[0] and running_pkey[1] == reserved_pkey[1]:
                    diff[running_pkey] = diff.get(running_pkey, 0) + inventory[running_pkey]
                    diff[reserved_pkey] -= inventory[running_pkey]

    # For all other running instances, add a negative amount
    for pkey in inventory:
        if pkey not in reserved_instances:
            diff[pkey] = diff.get(pkey, 0) - inventory[pkey]

    unused_ri = {}
    unreserved_instances = {}
    for k, v in six.iteritems(diff):
        if v > 0:
            unused_ri[k] = v
        elif v < 0:
            unreserved_instances[k] = -v

    return (unused_ri,unreserved_instances)

###############################################################################################
# Show Report
###############################################################################################    
def report(conn,type,reserved_instances, unused_ri, unreserved_instances, soon_expire_ri, inventory):
    # Report
    if INDATABASE:
        cursor = conn.cursor()
    else:
        cursor=None
    print("Reserved instances:")
    result=[]
    for k, v in sorted(six.iteritems(reserved_instances), key=lambda x: x[0]):
        print("\t(%s)\t%12s\t%s\t%s" % ((v,) + k))
    (k1,k2,k3) = k
    result.append([type,k1,k2,k3,int(v)])
    if INDATABASE:
        if (type=='rds'):
            query = 'INSERT INTO AWS_RI(ri_type,platform,type,AZ,number,unused) VALUES (%s,%s,%s,%s,%s,False)'
        else:
            query = 'INSERT INTO AWS_RI(ri_type,platform,type,region,number,unused) VALUES (%s,%s,%s,%s,%s,False)'
        cursor.executemany(query,result)
        conn.commit()

    result=[]
    unused=0
    print("Unused reserved instances:")
    for k, v in sorted(six.iteritems(unused_ri), key=lambda x: x[0]):
        print("\t(%s)\t%12s\t%s\t%s" % ((v,) + k))
    (k1,k2,k3) = k
    result.append([type,k1,k2,k3,int(v)])
    unused+=v
    if not unused_ri:
        print("\tNone")
    print("")

    if INDATABASE:
        if (type=='rds'):
            query = 'INSERT INTO AWS_RI(ri_type,platform,type,AZ,number,unused) VALUES (%s,%s,%s,%s,%s,True)'
        else:
            query = 'INSERT INTO AWS_RI(ri_type,platform,type,region,number,unused) VALUES (%s,%s,%s,%s,%s,True)'
        cursor.executemany(query,result)
        conn.commit()

    print("Expiring soon (less than %sd) reserved instances:" % WARNTIME)
    for k, v in sorted(six.iteritems(soon_expire_ri), key=lambda x: x[1][:2]):
        (platform, instance_type, region, expire_date) = v
        expire_date = expire_date.strftime('%Y-%m-%d')
        print("\t%s\t%12s\t%s\t%s\t%s" % (k, platform, instance_type, region, expire_date))

    if not soon_expire_ri:
        print("\tNone")
    print("")

    result=[]
    ondemand=0
    print("On-demand instances, which haven't got a reserved instance:")
    for k, v in sorted(six.iteritems(unreserved_instances), key=lambda x: x[0]):
        print("\t(%s)\t%12s\t%s\t%s" % ((v,) + k))
        ondemand+=v
        (k1,k2,k3) = k
        result.append([type,k1,k2,k3,int(v)])
    if not unreserved_instances:
        print("\tNone")
    print("")

    if INDATABASE:
        if (type=='rds'):
            query = 'INSERT INTO AWS_NO_RI(ri_type,platform,type,AZ,number) VALUES (%s,%s,%s,%s,%s)'
        else:
            query = 'INSERT INTO AWS_NO_RI(ri_type,platform,type,region,number) VALUES (%s,%s,%s,%s,%s)'
        cursor.executemany(query,result)
        conn.commit()

    date=datetime.now()
    print(date)
    if INDATABASE:
        query="INSERT INTO AWS_RI_STAT(ri_type,date,ondemand,ri) VALUES (%s,%s,%s,%s)"
        cursor.execute(query,(type,date,round(100*ondemand/sum(inventory.values())),round(100*unused/sum(reserved_instances.values()))))
        conn.commit()
    print("Running on-demand instances:   %s" % round(100*ondemand/sum(inventory.values())))
    print("Reserved instances:            %s" % round(100*unused/sum(reserved_instances.values())))
    print("")

###############################################################################################
# Main
###############################################################################################
date=datetime.now().strftime("%Y-%m-%d")

try:
    if INDATABASE:
	    conn = mysql.connector.connect(host=MYSQL_HOST,database=MYSQL_DB,user=MYSQL_USER,password=MYSQL_PASS)
	    cursor = conn.cursor()
	    cursor.execute("DELETE FROM AWS_EC2_up",())
	
	    cursor.execute("DELETE FROM AWS_EBS_unused",())
	    cursor.execute("DELETE FROM AWS_NO_RI",())	
	    cursor.execute("DELETE FROM AWS_RI",())
	    cursor.execute('DELETE FROM AWS_RI_STAT WHERE date=%(d)s',{'d':date})
	    cursor.execute('DELETE FROM AWS_EC2_up_stat WHERE date=%(d)s',{'d':date})
	    cursor.execute('DELETE FROM AWS_EC2_INVENTORY WHERE date=%(d)s',{'d':date})
    else:
        conn=None
        cursor=None
except Exception as e:
	print("Error during db deletion")
	print(e)



# Let's play with RDS
print("-------------------------------------------------------------------------------------")
print("-                                        RDS                                        -")
print("-------------------------------------------------------------------------------------")

rdsinventory = {}
reserved_instances = {}
soon_expire_ri = {}

rdsinventory = getRdsInventory(cursor)
(reserved_instances, soon_expire_ri)=getRDSRI()

(unused_ri,unreserved_instances) = compareRI(reserved_instances,rdsinventory)
report(conn,'rds',reserved_instances, unused_ri, unreserved_instances, soon_expire_ri, rdsinventory)


# Let's play with EC2
print("-------------------------------------------------------------------------------------")
print("-                                        EC2                                        -")
print("-------------------------------------------------------------------------------------")
untag=[]

(inventory,ec2inventory)=getEc2Inventory(cursor)
(reserved_instances,soon_expire_ri) = getEC2RI()
(unused_ri,unreserved_instances) = compareRI(reserved_instances,ec2inventory)
report(conn,'ec2',reserved_instances, unused_ri, unreserved_instances, soon_expire_ri,ec2inventory)

availableVolumes = getAvailableVolumes()

print("-------------------------------------------------------------------------------------")
print("-                                EBS Unused since 30 days                            -")
print("-------------------------------------------------------------------------------------")
print("EBS not attached since 30 days")
result=[]
for t in availableVolumes:
    print("\t "+t[0]+' : ' +t[1])
    result.append(t)

if INDATABASE:
    query="INSERT INTO AWS_EBS_unused(env,volume_id) VALUES (%s,%s)"
    cursor.executemany(query,result)
    conn.commit()



# Check of ec2 on during night
(resultDay,resultNight,total)= getEC2NotPoweredOff()
  
print("-------------------------------------------------------------------------------------")
print("-                      EC2 Non Prodnot turned off during night                       -")
print("-------------------------------------------------------------------------------------")
print("EC2 NON Prod Not turned off during night")
print("  -- Percent : %i" % round(100*len(resultNight)/total))
for t in resultNight:
    print("\t "+ t[0])


print("-------------------------------------------------------------------------------------")
print("-                             EC2 not turned off during night                       -")
print("-------------------------------------------------------------------------------------")
print("EC2 NON PROD turned off during day")
print("  -- Percent : %i" % round(100*len(resultDay)/total))
for t in resultDay:
    print("\t "+ t[0])

if INDATABASE:
    query='INSERT INTO AWS_EC2_up(name,day) VALUES (%s,FALSE)'
    cursor.executemany(query,resultNight)
    conn.commit()

    query='INSERT INTO AWS_EC2_up(name,day) VALUES (%s,TRUE)'
    cursor.executemany(query,resultDay)
    conn.commit()

date=datetime.now()
if INDATABASE:
    query='INSERT INTO AWS_EC2_up_stat(date,percent,day) VALUES (%s,%s,FALSE)'
    cursor.execute(query,(date,round(100*len(resultNight)/total)))

    query='INSERT INTO AWS_EC2_up_stat(date,percent,day) VALUES (%s,%s,TRUE)'
    cursor.execute(query,(date,round(100*len(resultDay)/total)))
    conn.commit()

