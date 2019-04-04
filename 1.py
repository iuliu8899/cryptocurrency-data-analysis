#!/usr/bin/env python
# coding: utf-8

# # Cryptocurrency

# In[1]:


import pandas


# ## read files

# In[2]:


inputs = pandas.read_csv('inputs.csv', header=None)
outputs = pandas.read_csv('outputs.csv', header=None)
tags = pandas.read_csv('tags.csv')
transactions = pandas.read_csv('transactions.csv', header=None)

inputs.columns = ["id", "tx_id", "sig_id", "output_id"]
outputs.columns = ["id", "tx_id", "pk_id", "value"]
tags.columns = ["type", "service", "pk_id"]
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
    temp_outputs = outputs[['id','value']]
    temp_inputs = inputs['output_id']
    UTXO = temp_outputs[~temp_outputs['id'].isin(temp_inputs)].set_index('id')
    print ("How many UTXOs exist, as of the last block of the dataset?")
    print (len(UTXO))
    print ("Which UTXO has the highest associated value?")
    print ("output id:", UTXO.idxmax().value)
    print ("value:", UTXO.max().value)


# In[6]:


answer_1_2()


# ### 1.3 Public keys

# In[7]:


def answer_1_3():
    print ("------------------- 1.3 -------------------")
    print ("How many distinct public keys were used across all blocks in the dataset?")
    #pk_ids = pandas.concat([outputs['pk_id'], tags['pk_id']], axis=0, ignore_index=True).unique()
    print (len(outputs['pk_id'].unique())) #174702

    print ("Which public key received the highest number of bitcoins, and how many bitcoins has it received?")
    temp = outputs
    temp = temp.groupby('pk_id')['value'].sum()
    print("pk:", temp.idxmax(), "value:", temp.max())
    
    print ("Which public key acted as an output the most number of times, and how many times did it act as output?")
    count_pk = outputs['pk_id'].value_counts()
    print ("pk:", count_pk.idxmax(), "value:", count_pk.max())


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
# ### 2.1 Specific cluster

# In[ ]:




