from subprocess import call
import re
# import json

def prerequisites():
    call('pip install pyyaml', shell=True)

def test():
    import yaml
    data = ''
    # active_namenode_host = ''
    # standby_namenode_host = ''
    # zk_host_1 = ''
    # zk_host_2 = ''
    # zk_host_3 = ''
    # journalnode_host_1 = ''
    # journalnode_host_2 = ''
    # journalnode_host_3 = ''
    # number_of_agents = 0

    # namenode_list = []
    # zookeeper_list = []
    # journalnode_list = []
    
    with open('config.yaml') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    # print("Souvik")
    # print(data['ambari_agents'])
    # host_groups = []
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
    for i in range(0, len(data['ambari_agents'])):
        # print(data['ambari_agents'][i])
        # print('ambari_agent_' + str(i+1))
        # print(data['ambari_agents'][i]['ambari_agent_' + str(i+1)])
        ambariAgent = 'ambari_agent_' + str(i+1)
        # host_groups.append("{"+'name'+":"+'ambari_agent_' +str(i+1)+"'"+"},")
        x["host_groups"].append({"name": ambariAgent,"hosts":[{"fqdn":data['ambari_agents'][i]['ambari_agent_'+str(i+1)][0]['fqdn']}]})
        # for item in range(0, len(data['ambari_agents'][i]['ambari_agent_' + str(i+1)])):
        #     print(data['ambari_agents'][i]['ambari_agent_' + str(i+1)][item])
    # print(host_groups)
    jsonFile = open("hostMapping.json","w+")
    # listToStr = ' '.join(map(str, x)) 
    jsonFile.write(str(x))
    
    # convert into JSON:
    # y = json.dumps(x)
    # the result is a JSON string:
    # print(y)
    # print(x["host_groups"][0]['hosts'][0]['fqdn'])
    # print(x)
    # print(data['ambari_agents'][1]['ambari_agent_2'][0]['fqdn'])   
    # print('ZOOKEEPER_SERVER' in data['ambari_agent_12'][2]['components'])
    # number_of_agents = data['number_of_agents']
    # namenode_directories = data['namenode_directories']
    # datanode_directories = data['datanode_directories']
    # zookeeper_directories = data['zookeeper_directories']
    # journalnode_directories = data['journalnode_directories']
    # cluster_name = data['cluster_name']
    # hdp_version = data['hdp_version']

    # # loop through the agents to find the host fqdns
    # for i in range(1, number_of_agents+1):
    #     agent = 'ambari_agent_' + str(i)
    #     print(data[agent])

    #     # find the active namenode and standby namenode hosts fqdn
    #     components = data[agent][2]['components']
    #     if 'NAMENODE' in components:
    #         namenode_list.append(agent)
    #     if 'ZOOKEEPER_SERVER' in components:
    #         zookeeper_list.append(agent)
    #     if 'JOURNALNODE' in components:
    #         journalnode_list.append(agent)

    

    # print(namenode_list, journalnode_list, zookeeper_list)

    # ### BEGIN - CORE SITE PROPERTIES ###

    # zk_quorum_value = ""
    # for index, zk_host in enumerate(zookeeper_list):
    #     if index == len(zookeeper_list)-1:
    #         hostgroup = "%HOSTGROUP::"+str(zk_host)+"%:2181"
    #         zk_quorum_value+=hostgroup
    #     else:
    #         hostgroup = "%HOSTGROUP::"+str(zk_host)+"%:2181,"
    #         zk_quorum_value+=hostgroup    

    # zk_quorum_property = '"ha.zookeeper.quorum": "' + zk_quorum_value + '"'

    # core_site_properties = '{\n\t"fs.defaultFS": "hdfs://mycluster"'

    # if zk_quorum_value is not '':
    #     additional_property = ",\n\t" + zk_quorum_property
    #     core_site_properties += additional_property
    
    # core_site_properties+="\n}"

    # print(core_site_properties)

    # ### END - CORE SITE PROPERTIES ###
    

    # ### BEGIN - HDFS SITE PROPERTIES ###    
    # dynamicHDFSproperties = []

    # # static properties
    # dynamicHDFSproperties.append('"dfs.client.failover.proxy.provider.mycluster": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"')
    # dynamicHDFSproperties.append('"dfs.ha.automatic-failover.enabled": "true"')
    # dynamicHDFSproperties.append('"dfs.ha.fencing.methods": "shell(/bin/true)"')
    # dynamicHDFSproperties.append('"dfs.nameservices": "mycluster"')
    
    # # dfs.ha.namenodes.mycluster
    # nnValue = ''
    # for i in range(1, len(namenode_list)+1):
    #     if i==len(namenode_list):
    #         nn = 'nn'+str(i)
    #         nnValue+=nn
    #     else:
    #         nn = 'nn'+str(i)+','
    #         nnValue+=nn
    # nnMyClusterProperty = '"dfs.ha.namenodes.mycluster": "'+nnValue+'"'
    # dynamicHDFSproperties.append(nnMyClusterProperty)

    # # everything wrt namenode 1
    # # dfs.namenode.http-address
    # nnValue = '%HOSTGROUP::'+namenode_list[0]+'%:50070'
    # activeNamenodeHTTP = '"dfs.namenode.http-address": "'+nnValue+'"'
    # dynamicHDFSproperties.append(activeNamenodeHTTP)
    # # dfs.namenode.https-address
    # nnValue = '%HOSTGROUP::'+namenode_list[0]+'%:50470'
    # activeNamenodeHTTPS = '"dfs.namenode.https-address": "'+nnValue+'"'
    # dynamicHDFSproperties.append(activeNamenodeHTTPS)

    # # all the nn* properties
    # # http-address
    # for i in range(1, len(namenode_list)+1):
    #     nnValue = '%HOSTGROUP::'+str(namenode_list[i-1])+'%:50070'
    #     httpNN = '"dfs.namenode.http-address.mycluster.nn'+str(i)+'": "'+nnValue+'"'
    #     dynamicHDFSproperties.append(httpNN)
    # # https-address
    # for i in range(1, len(namenode_list)+1):
    #     nnValue = '%HOSTGROUP::'+str(namenode_list[i-1])+'%:50470'
    #     httpNN = '"dfs.namenode.https-address.mycluster.nn'+str(i)+'": "'+nnValue+'"'
    #     dynamicHDFSproperties.append(httpNN)   
    # # rpc-address
    # for i in range(1, len(namenode_list)+1):
    #     nnValue = '%HOSTGROUP::'+str(namenode_list[i-1])+'%:8020'
    #     httpNN = '"dfs.namenode.rpc-address.mycluster.nn'+str(i)+'": "'+nnValue+'"'
    #     dynamicHDFSproperties.append(httpNN)

    # # datanode data, namenode name, jounalnode directories, zookeeper directories
    # namenode_dir_list = ""
    # datanode_dir_list = ""
    # journalnode_dir_list = ""
    # zookeeper_dir_list = ""
    # for index, directory in enumerate(namenode_directories):
    #     if index == len(namenode_directories)-1:
    #         namenode_dir_list+=str(directory)
    #     else:
    #         directory= str(directory) + ","
    #         namenode_dir_list+=directory    
    # for index, directory in enumerate(datanode_directories):
    #     if index == len(datanode_directories)-1:
    #         datanode_dir_list+=str(directory)
    #     else:
    #         directory= str(directory) + ","
    #         datanode_dir_list+=directory
    # for index, journalnode in enumerate(journalnode_directories):
    #     if index == len(journalnode_directories)-1:
    #         journalnode_dir_list+=str(journalnode)
    #     else:
    #         journalnode= str(journalnode) + ","
    #         journalnode_dir_list+=journalnode    
    # for index, zookeeperNode in enumerate(zookeeper_directories):
    #     if index == len(zookeeper_directories)-1:
    #         zookeeper_dir_list+=str(zookeeperNode)
    #     else:
    #         zookeeperNode= str(zookeeperNode) + ","
    #         zookeeper_dir_list+=zookeeperNode  
    # # namenode_directories
    # namenodeDirProperty = '"dfs.namenode.name.dir": "'+namenode_dir_list+'"'
    # dynamicHDFSproperties.append(namenodeDirProperty)
    # # namenode_directories
    # datanodeDirProperty = '"dfs.datanode.data.dir": "'+datanode_dir_list+'"'
    # dynamicHDFSproperties.append(datanodeDirProperty)
    # # journalnode_directories
    # journalnodeDirProperty = '"dfs.journalnode.edits.dir": "'+journalnode_dir_list+'"'
    # dynamicHDFSproperties.append(journalnodeDirProperty)

    # # journalnode hostgroup property
    # if len(journalnode_list) > 0:
    #     # journal nodes exist and must be handled
    #     sharedEditsValue = "qjournal://"
    #     for agent in journalnode_list:
    #         if agent == journalnode_list[-1]:
    #             nnValue = "%HOSTGROUP::"+agent+"%:8485/mycluster"
    #             sharedEditsValue += nnValue
    #         else:
    #             nnValue = "%HOSTGROUP::"+agent+"%:8485;"
    #             sharedEditsValue += nnValue
    # sharedEditsProperty = '"dfs.namenode.shared.edits.dir": "'+sharedEditsValue+'"'
    # dynamicHDFSproperties.append(sharedEditsProperty)

    # # finalizing the properties
    # hdfs_site_properties = "{"
    # for index, property in enumerate(dynamicHDFSproperties):
    #     if index == len(dynamicHDFSproperties)-1:
    #         nnValue = '\n\t'+property
    #         hdfs_site_properties += nnValue
    #     else:
    #         nnValue = '\n\t'+property+','
    #         hdfs_site_properties += nnValue
    # hdfs_site_properties+="\n}"
    # print(hdfs_site_properties)
    
    ### END - HDFS SITE PROPERTIES ###

    ### BEGIN - ZOOKEEPER PROPERTIES ###
    # dataDir property
    # dynamicZookeeperProperties = []

    # dataDirProperty = '"dataDir": "'+zookeeper_dir_list+'"'
    # print(dataDirProperty)
    # dynamicZookeeperProperties.append(dataDirProperty)
    # zookeeper_properties = "{"
    # for index, property in enumerate(dynamicZookeeperProperties):
    #     if index == len(dynamicZookeeperProperties)-1:
    #         nnValue = '\n\t'+property
    #         zookeeper_properties += nnValue
    #     else:
    #         nnValue = '\n\t'+property+','
    #         zookeeper_properties += nnValue
    # zookeeper_properties+="\n}"

    # print(zookeeper_properties)

    ### END - ZOOKEEPER PROPERTIES ###    

    # replacing the placeholders with properties
    # cluster_config_file = "./cluster_configuration.json"
    # f = open(cluster_config_file, "r+")
    # f_content = f.read()
    # f_content = re.sub(r'"<core_site_properties>"', core_site_properties, f_content)
    # f_content = re.sub(r'"<hdfs_site_properties>"', hdfs_site_properties, f_content)
    # f_content = re.sub(r'"<zookeeper_properties>"', zookeeper_properties, f_content)
    # f.seek(0)
    # f.truncate()
    # f.write(f_content)
    # f.close()

    # namenode_dir_list = ""
    # datanode_dir_list = ""
    # for index, directory in enumerate(namenode_directories):
    #     if index == len(namenode_directories)-1:
    #         namenode_dir_list+=str(directory)
    #     else:
    #         directory= str(directory) + ","
    #         namenode_dir_list+=directory    
    # for index, directory in enumerate(datanode_directories):
    #     if index == len(datanode_directories)-1:
    #         datanode_dir_list+=str(directory)
    #     else:
    #         directory= str(directory) + ","
    #         datanode_dir_list+=directory 
    # cluster_config_file = "./cluster_configuration.json"
    # f = open(cluster_config_file, "r+")
    # f_content = f.read()
    # if zk_host_1 is not '':

    #     f_content = re.sub()
    # f_content = re.sub(r'<namenode_directories>', namenode_dir_list, f_content)
    # f_content = re.sub(r'<datanode_directories>', datanode_dir_list, f_content)
    # f_content = re.sub(r'<zookeeper_data_dir>', zookeeper_data_dir, f_content)
    # f_content = re.sub(r'<journal_node_data_dir>', journal_node_data_dir, f_content)
    # f_content = re.sub(r'<cluster_name>', cluster_name, f_content)
    # f_content = re.sub(r'<stack_version>', str(hdp_version), f_content)
    # f_content = re.sub(r'<zk_host_1>', zk_host_1, f_content)
    # f_content = re.sub(r'<zk_host_2>', zk_host_2, f_content)
    # f_content = re.sub(r'<zk_host_3>', zk_host_3, f_content)
    # f_content = re.sub(r'<active_namenode>', active_namenode_host, f_content)
    # f_content = re.sub(r'<standby_namenode>', standby_namenode_host, f_content)
    # f_content = re.sub(r'<journalnode_1>', journalnode_host_1, f_content)
    # f_content = re.sub(r'<journalnode_2>', journalnode_host_2, f_content)
    # f_content = re.sub(r'<journalnode_3>', journalnode_host_3, f_content)
    # f.seek(0)
    # f.truncate()
    # f.write(f_content)
    # f.close()
        
test()
