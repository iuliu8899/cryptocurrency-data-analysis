#!/usr/bin/env python
# coding: utf-8

# # Cryptocurrency
# #### Group Member: Weiqiu Liu, Ming Fang 

# In[1]:


import pandas


# ## read files

# In[2]:


inputs = pandas.read_csv('inputs.csv', header=None)
outputs = pandas.read_csv('outputs.csv', header=None)
transactions = pandas.read_csv('transactions.csv', header=None)
inputs.columns = ["id", "tx_id", "sig_id", "output_id"]
outputs.columns = ["id", "tx_id", "pk_id", "value"]
transactions.columns = ['id', "block_id"]


# ## 1 Basic statistic
# ### 1.1 Transactions

# In[3]:


def answer_1_1():
    print ("------------------- 1.1 -------------------")
    print ("How many transactions were there in total?")
    print (len(transactions['id'].unique()))
    print ("Of these, how many transactions had one input and two outputs?")
    count_outputs = outputs['tx_id'].value_counts()
    count_inputs = inputs['tx_id'].value_counts()
    count_outputs_1 = count_outputs[count_outputs == 1]
    count_outputs_2 = count_outputs[count_outputs == 2]
    count_inputs_1 = count_inputs[count_inputs == 1]
    print (len(count_outputs_2[count_outputs_2.index.isin(count_inputs_1.index)]))
    print ("How many transactions had one input and one output?")
    print (len(count_outputs_1[count_outputs_1.index.isin(count_inputs_1.index)]))


# In[4]:


answer_1_1()


# ### 1.2 UTXOs

# In[5]:


def answer_1_2():
    print ("------------------- 1.2 -------------------")
    temp_outputs = outputs[['id','pk_id','value']]
    temp_inputs = inputs['output_id']
    UTXOs = temp_outputs[~temp_outputs['id'].isin(temp_inputs)].set_index('id')
    print ("How many UTXOs exist, as of the last block of the dataset?")
    print (len(UTXOs))
    print ("Which UTXO has the highest associated value?")
    print ("output id:", UTXOs.value.idxmax())
    print ("value:", UTXOs.value.max())
    return UTXOs


# In[6]:


UTXOs = answer_1_2()


# ### 1.3 Public keys

# In[7]:


def answer_1_3():
    print ("------------------- 1.3 -------------------")
    print ("How many distinct public keys were used across all blocks in the dataset?")
    #pk_ids = pandas.concat([outputs['pk_id'], tags['pk_id']], axis=0, ignore_index=True).unique()
    print (len(outputs['pk_id'].unique()))
    print ("Which public key received the highest number of bitcoins, and how many bitcoins has it received?")
    temp = outputs
    temp = temp.groupby('pk_id')['value'].sum()
    print("pk_id:", temp.idxmax(), "value:", temp.max())
    print ("Which public key acted as an output the most number of times, and how many times did it act as output?")
    count_pk = outputs['pk_id'].value_counts()
    print ("pk_id:", count_pk.idxmax(), "value:", count_pk.max())


# In[8]:


answer_1_3()
# outputs.loc[outputs['pk_id']==148105]


# ### 1.4 Invalid transactions

# In[9]:


def answer_1_4():
    print ("------------------- 1.4 -------------------")
    ## double spend
    temp_spend = inputs
    temp_spend = temp_spend.loc[(temp_spend['output_id'].duplicated()) & (temp_spend['output_id'] != -1)]
    i = 0
    for _, row in temp_spend.iterrows():
        i += 1
        print (str(i)+". double spend:")
        print ("id:", row.id, "tx_id:", row.tx_id, "sig_id:", row.sig_id, "output_id:", row.output_id)
        print ("spent in:")
        s = inputs.loc[(inputs['output_id'] == row.output_id) & (inputs['id'] != row.id)]
        #print ("id:", s.id, "tx_id:", s.tx_id, "sig_id:", s.sig_id, "output_id:", s.output_id)
        row = s.reset_index().loc[0]
        print ("id:", row.id, "tx_id:", row.tx_id, "sig_id:", row.sig_id, "output_id:", row.output_id)
        print ("")
        
    ## ouput > input
    temp_input = inputs
    temp_output = outputs
    money_in = pandas.merge(temp_input[['tx_id','output_id']],temp_output[['id','value']],left_on='output_id',right_on='id')[['tx_id','value']]
    money_in = money_in.groupby('tx_id')['value'].sum()
    dict_money_in = {'tx_id':money_in.index,'money_in':money_in.values}
    money_in = pandas.DataFrame(dict_money_in)
    money_out = temp_output.groupby('tx_id')['value'].sum()
    dict_money_out = {'tx_id':money_out.index,'money_out':money_out.values}
    money_out = pandas.DataFrame(dict_money_out)
    transactions = pandas.merge(money_in[['tx_id','money_in']], money_out[['tx_id','money_out']], on='tx_id')
    invalid_tx = transactions.loc[(transactions['money_in']<transactions['money_out'])]
    for _, row in invalid_tx.iterrows():
        i += 1
        print (str(i)+". output > input:")
        print ("tx_id:", row.tx_id, "money_in:", row.money_in, "money_out:", row.money_out)
        print ("money_in:")
        j = 0
        for _, row1 in inputs.loc[inputs['tx_id']==row.tx_id].iterrows():
            for _, row2 in outputs.loc[outputs['id']==row1.output_id].iterrows():
                j += 1
                print (str(j)+") input_id:", row1.tx_id, "UTXO_id:", row1.output_id, "owner_id:", row1.sig_id, "value:",row2.value)
        print ("money_out:")
        j = 0
        for _, row1 in outputs.loc[outputs['tx_id']==row.tx_id].iterrows():
            j += 1
            print (str(j)+") output_id:", row1.id, "payee_id:", row1.pk_id, "value:", row1.value)


# In[10]:


answer_1_4()


# ## 2 Clustering
# ### generate cluster dataframe

# In[11]:


def generate_clusters():
    max_cluster_id = 0
    # init a new data frame to store clusters, columns = ['pk_id','cluster_id']
    clusters = pandas.DataFrame()
    # set pk_id columns to all unique sig_id in inputs
    clusters['pk_id'] = inputs['sig_id'].unique()
    # set index to pk_id
    clusters = clusters.set_index('pk_id')
    # init all cluster_id as 0
    clusters['cluster_id'] = 0

    # recursion funtion to find all related publick keys with input pk_id, store and return them in pk_id_list
    def get_all_related_pk_id(pk_id,pk_id_list):
        # get all transactions containing the input pk_id
        tx_id_array = inputs.loc[inputs['sig_id']==pk_id,'tx_id'].unique()
        # traverse transactions
        for tx_id_related in tx_id_array:
            # get all public keys in this transaction
            # the public key is related to the input pk_id, thus they should be clustered in same group
            pk_id_array = inputs.loc[inputs['tx_id']==tx_id_related,'sig_id'].unique()
            # traverse public keys
            for pk_id_related in pk_id_array:
                # if related public key has not yet been in the related public key list,
                # add it in and find public keys related to this one recursively
                if pk_id_related not in pk_id_list:
                    pk_id_list += [pk_id_related]
                    if pk_id_related == pk_id:
                        continue
                    pk_id_list = get_all_related_pk_id(pk_id_related,pk_id_list)
        return pk_id_list

    # main loop to call get_all_related_pk_id() function
    for pk_id in clusters.index:
        # if current pk_id has been assigned a cluster_id, it means it has already went through the recursion so no need to do it again
        if clusters.loc[pk_id].cluster_id != 0:
            continue
        # init a blank list
        pk_id_list = []
        # call function
        pk_id_list = get_all_related_pk_id(pk_id,pk_id_list)
        # increment current max_cluster_id and assign it to new clusters
        max_cluster_id += 1
        clusters.loc[pk_id_list] = max_cluster_id
    return clusters


# In[12]:


clusters = generate_clusters()
clusters.to_csv("clusters.csv")


# ### 2.1 Specific cluster

# In[13]:


def answer_2_1():
    print ("------------------- 2.1 -------------------")
    print ("How big is the cluster that contains public key (pk_id) 41442?")
    cluster_id_41442 = clusters.loc[41442].cluster_id
    pk_id_in_cluster = clusters.loc[clusters['cluster_id']==cluster_id_41442].index
    print (clusters['cluster_id'].value_counts()[cluster_id_41442])
    print ("Identify the cluster according to its keys with the lowest and highest numeric values.")
    print ("lowest pk_id:", pk_id_in_cluster.min())
    print ("highest pk_id:", pk_id_in_cluster.max())


# In[14]:


answer_2_1()


# ### 2.2 Biggest cluster

# In[15]:


def answer_2_2():
    print ("------------------- 2.2 -------------------")
    print ("Which cluster has the largest number of keys, and how many keys does it contain?")
    temp = clusters
    cluster_id, number_of_keys = temp['cluster_id'].value_counts().idxmax(), temp['cluster_id'].value_counts().max()
    print ("number of keys:",number_of_keys)
    print ("Identify it according to its keys with the lowest and highest numeric values.")
    print ("lowest pk_id:",temp.loc[temp['cluster_id']==cluster_id].index.min())
    print ("highest pk_id:",temp.loc[temp['cluster_id']==cluster_id].index.max())


# In[16]:


answer_2_2()


# ### 2.3 Richest cluster

# In[17]:


def answer_2_3():
    print ("------------------- 2.3 -------------------")
    print ("As of the last block in the dataset, which cluster controls the most unspent bitcoins, and how many bitcoins does it control? Again, identify it according to its keys with the lowest and highest numeric values.")
    pk_id_grouped_UTXOs_dict = UTXOs.groupby('pk_id')['value'].sum()
    pk_id_grouped_UTXOs = pandas.DataFrame({'pk_id':pk_id_grouped_UTXOs_dict.index,'value':pk_id_grouped_UTXOs_dict.values}).set_index('pk_id')
    clusters_UTXOs = pandas.merge(clusters,pk_id_grouped_UTXOs,left_index=True,right_index=True,how='inner')
    clusters_UTXOs = clusters_UTXOs.groupby('cluster_id')['value'].sum()
    print ("highest unspent bitcoin:",clusters_UTXOs.max())
    cluster_id = clusters_UTXOs.idxmax()
    print ("lowest pk_id:",clusters.loc[clusters['cluster_id']==cluster_id].index.min())
    print ("highest pk_id:",clusters.loc[clusters['cluster_id']==cluster_id].index.max())
    print ("Which transaction is responsible for sending the largest number of bitcoins to this entity (i.e., to one or more of the keys in this cluster)?")
    cluster_with_highest_UTXO = clusters.loc[clusters['cluster_id']==cluster_id].index
    output_to_this_cluster = outputs.loc[outputs['pk_id'].isin(cluster_with_highest_UTXO)]
    tx = output_to_this_cluster.groupby('tx_id').value.sum()
    print ("tx_id:",tx.idxmax(),"value",tx.max())


# In[18]:


answer_2_3()


# ### 2.4 Heuristics

# In[26]:


# function used to find false positive
def find_false_positive(temp_inputs):
    max_cluster_id = 0
    temp_clusters = pandas.DataFrame()
    temp_clusters['pk_id'] = temp_inputs['sig_id'].unique()
    temp_clusters = temp_clusters.set_index('pk_id')
    temp_clusters['cluster_id'] = 0

    def get_all_related_pk_id(pk_id,pk_id_list):
        tx_id_array = temp_inputs.loc[temp_inputs['sig_id']==pk_id,'tx_id'].unique()
        if temp_clusters.loc[pk_id].cluster_id != 0:
            print(123456)
        for tx_id_related in tx_id_array:
            pk_id_array = temp_inputs.loc[temp_inputs['tx_id']==tx_id_related,'sig_id'].unique()
            for pk_id_related in pk_id_array:
                if pk_id_related not in pk_id_list:
                    pk_id_list += [pk_id_related]
                    if pk_id_related == pk_id:
                        continue
                    pk_id_list = get_all_related_pk_id(pk_id_related,pk_id_list)
        return pk_id_list

    for pk_id in temp_clusters.index:
        if temp_clusters.loc[pk_id].cluster_id != 0:
            continue
        pk_id_list = []
        pk_id_list = get_all_related_pk_id(pk_id,pk_id_list)
        max_cluster_id += 1
        temp_clusters.loc[pk_id_list] = max_cluster_id
    return temp_clusters

# function used to find false negative
def find_false_negative():
    output_pk_id_array = outputs['pk_id'].unique()
    for pk_id in output_pk_id_array:
        if len(outputs.loc[outputs['pk_id']==pk_id]) == 1 and len(inputs.loc[inputs['sig_id']==pk_id]) == 1:
            tid1 = (outputs.loc[outputs['pk_id']==pk_id])['tx_id'].unique()[0]
            cid = clusters.loc[inputs.loc[inputs['tx_id']==tid1,'sig_id'].unique()[0]].cluster_id
            tid2 = inputs.loc[inputs['sig_id']==pk_id,'tx_id'].unique()[0]
            if clusters.loc[pk_id].cluster_id != cid and cid != 1:
                if len(outputs.loc[outputs['tx_id']==tid1,'pk_id']) == 2 and len(outputs.loc[outputs['tx_id']==tid2,'pk_id']) == 2 and len(inputs.loc[inputs['tx_id']==tid2,'sig_id']) == 1 and len(inputs.loc[inputs['tx_id']==tid1,'sig_id']) > 100:
                    print (clusters.loc[pk_id].cluster_id,cid,pk_id,tid1,tid2,len(inputs.loc[inputs['tx_id']==tid1,'sig_id']))

def answer_2_4():
    print ("------------------- 2.4 -------------------")
    print ("Identify at least one potential source of false positives (keys that are clustered together but are not actually owned by the same entity) and one source of false negatives (keys that were not clustered together but are owned by the same entity).")
    # false positive (only list one result found by find_false_positive())
    target_cid = 30455
    target_tid = 119993
    pk_id_array = clusters.loc[clusters['cluster_id']==target_cid].index
    related_inputs = inputs.loc[inputs['sig_id'].isin(pk_id_array)]
    tx_removed_target_tid = related_inputs.loc[related_inputs['tx_id']!=target_tid]
    cluster_after_remove = find_false_positive(tx_removed_target_tid)
    print ("\nFalse Positive:")
    print ("After remove transaction with tx_id 119993, original cluster with highest pk_id:",clusters.loc[clusters['cluster_id']==target_cid].index.max(),"and lowest pk_id:",clusters.loc[clusters['cluster_id']==target_cid].index.min(),"(total number of keys: "+str(len(clusters.loc[clusters['cluster_id']==target_cid].index))+") would be clustered as two large group and within that number of keys would be",cluster_after_remove['cluster_id'].value_counts().values,".")
    print ("i.e. transaction 119993 was trying to merge two large cluster, and this 'merging' transaction only happened once.")
    print ("The input entries in transaction 119993:")
    print (related_inputs.loc[related_inputs['tx_id']==target_tid].set_index('id'))
    # false negative (only list one result found by find_false_negative())
    print ("\nFalse Negative:")
    target_cid1 = 870
    target_cid2 = 89
    target_pkid = 28601
    target_tid1 = 28702
    target_tid2 = 52070
    print ("In transaction "+str(target_tid1)+", there are 168 inputs and 2 outputs. It seems like one entity collects his owned bitcoins together to pay for one recipient and one collection (change) address that he controled.")
    print ("The change address (pk_id: "+str(target_pkid)+") only happened twice: one in the output of transaction "+str(target_tid1)+" for collection (change), another one for spending as a input in transaction "+str(target_tid2)+".")
    print ("The change address should be clustered same as inputs since they are all controled by same entity. But in multi-input clustering, this public key would be clustered as a distinct group (cluster_id: "+str(target_cid1)+") from the large entity (cluster_id: "+str(target_cid2)+") because it only happened one time as the input alone.")
    print ("(cluster_id can be seen in the generated clusters.csv file)")
    print ("\nWhat strategy could you use to make your clustering heuristic more accurate?")
    print ("1. Take extra care about some transactions happened less time but \"trying\" to merge two large clusters. (e.g. false positive above)")
    print ("2. Take consideration about the output of one transaction, the change address in output should be clustered to the same group with the input. (e.g. false negative above)")


# In[27]:


answer_2_4()


# ## 3 Tagging and tracking
# ### associate both individual keys and larger clusters with real entities

# In[21]:


def get_tags():
    tags = pandas.read_csv('tags.csv')
    pk_id_grouped_UTXOs_dict = UTXOs.groupby('pk_id')['value'].sum()
    pk_id_grouped_UTXOs = pandas.DataFrame({'pk_id':pk_id_grouped_UTXOs_dict.index,'value':pk_id_grouped_UTXOs_dict.values}).set_index('pk_id')
    clusters_UTXOs = pandas.merge(clusters,pk_id_grouped_UTXOs,left_index=True,right_index=True,how='inner')
    clusters_UTXOs = clusters_UTXOs.groupby('cluster_id')['value'].sum()
    clusters_UTXOs = pandas.DataFrame({'cluster_id':clusters_UTXOs.index,'UTXO_value':clusters_UTXOs.values})
    tags = pandas.merge(tags,clusters,right_index = True,left_on='pk_id',how='inner')
    tags = pandas.merge(tags,clusters_UTXOs,on='cluster_id',how='left')
    tags = tags.fillna(0)
    return tags
# columns = [type, name, pk_id, cluster_id, UTXO_value]
tags = get_tags()


# ### 3.1 Richest service

# In[22]:


def answer_3_1():
    print ("------------------- 3.1 -------------------")
    print ("Which tagged entity controls the most unspent bitcoins, and how many bitcoins does it control? Be careful to consider entities that may control multiple tagged clusters.")
    temp_tags = tags.groupby('name')['UTXO_value'].sum()
    print (temp_tags.idxmax(),int(temp_tags.max()))


# In[23]:


answer_3_1()


# ### 3.2 Interactions

# In[34]:


def answer_3_2():
    print ("------------------- 3.2 -------------------")
    print ("How many transactions sent bitcoins directly from a (fictional) exchange to a (fictional) dark market?")
    pk_id_exchange = clusters.loc[clusters['cluster_id'].isin(tags.loc[tags['type']=='Exchange'].cluster_id)].index
    pk_id_darkmarket = clusters.loc[clusters['cluster_id'].isin(tags.loc[tags['type']=='DarkMarket'].cluster_id)].index
    tx_from_exchange = inputs.loc[inputs['sig_id'].isin(pk_id_exchange)].tx_id.unique()
    tx_from_exchange_to_dark_market = outputs.loc[(outputs['pk_id'].isin(pk_id_darkmarket)) & (outputs['tx_id'].isin(tx_from_exchange))]
    print (len(tx_from_exchange_to_dark_market.tx_id.unique()))
    print ("How many bitcoins in total were sent across these transactions?")
    money_in = pandas.merge(inputs.loc[inputs['tx_id'].isin(tx_from_exchange_to_dark_market.tx_id.unique())][['tx_id','output_id']],outputs[['id','value']],left_on='output_id',right_on='id')[['tx_id','value']]
    print ("input total:",money_in['value'].sum(),"bitcoins")
    print ("sent to darkmarket:",tx_from_exchange_to_dark_market.value.sum(),"bitcoins")


# In[35]:


answer_3_2()


# ### 3.3 Tracking techniques

# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




