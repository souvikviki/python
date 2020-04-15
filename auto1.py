from subprocess import call
import re

def prerequisites():
    call('pip install pyyaml', shell=True)

def test1():
    import yaml
    data = ''
    
    with open("config.yaml") as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    #print(data)
    x = {
	"blueprint" : data['cluster_name'],
	"default_password" : "hadoop",
	"host_groups" : []
    }

    x["host_groups"] = []
    for i in range(0, data['number_of_agents']):
        ambariAgent = 'ambari_agent_' + str(i+1)
        x["host_groups"].append({"name": ambariAgent,"hosts":[{"fqdn":data['ambari_agent_'+str(i+1)][0]['fqdn']}]})
    jsonFile = open("hostmapping.json","w+")
    jsonFile.write(str(x))

test1()