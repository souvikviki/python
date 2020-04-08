from subprocess import call
import re
# import json

def prerequisites():
    call('pip install pyyaml', shell=True)

def test():
    import yaml
    data = ''
    
    with open('config.yaml') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    
    x = {
	"blueprint" : "multinode-hdp",
	"default_password" : "hadoop",
	"host_groups" :[
   {
	"name" : "master_1",
	"hosts" : [
	{
	"fqdn" : "ip-172-31-32-150.ap-south-1.compute.internal"
	}
	]
	},
   {
	"name" : "master_2",
	"hosts" : [
	{
	"fqdn" : "ip-172-31-32-234.ap-south-1.compute.internal"
	}
	]
	},
   {
	"name" : "slave_1",
	"hosts" : [
	{
	"fqdn" : "ip-172-31-41-226.ap-south-1.compute.internal"
	}
	]
	}
	]
    }
    x["host_groups"] = []
    for i in range(0, data['number_of_agents']):
        print(data['ambari_agents'][i]['ambari_agent_' + str(i+1)])
        ambariAgent = 'ambari_agent_' + str(i+1)
        x["host_groups"].append({"name": ambariAgent,"hosts":[{"fqdn":data['ambari_agents'][i]['ambari_agent_'+str(i+1)][0]['fqdn']}]})
    jsonFile = open("hostMapping.json","w+")
    jsonFile.write(str(x))
    
test()