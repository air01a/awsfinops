import boto3
import json
WARNTIME=30
DEFAULTREGION='eu-west-1'

def getTag(name,tags):
    if tags:
        for tag in tags:
            if tag['Key'].lower()==name.lower():
                return tag['Value']
    return None

def loadConfigFile(name):
    with open(name) as json_file:  
        data = json.load(json_file)
    return data

def saveConfigFile(name,data):
    with open(name, 'w') as outfile:
        json.dump(data, outfile)
