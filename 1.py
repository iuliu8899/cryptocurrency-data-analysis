#!/usr/bin/env python
# coding: utf-8

# # Cryptocurrency

# In[129]:


import pandas


# ## read files

# In[131]:


inputs = pandas.read_csv('inputs.csv', header=None)
outputs = pandas.read_csv('outputs.csv', header=None)
tags = pandas.read_csv('tags.csv')
transactions = pandas.read_csv('transactions.csv', header=None)

inputs.columns = ["id", "tx_id", "sig_id", "output_id"]
outputs.columns = ["id", "tx_id", "pk_id", "value"]
tags.columns = ["type", "service", "pk_id"]
transactions.columns = ['id', "block_id"]
inputs.set_index('id')
outputs.set_index('id')
tags.set_index('pk_id')
transactions.set_index('id');


# ## 1
# ### 1.1

# In[12]:


def answer_1_1():
    print ("------------------- 1.1 -------------------")
    print ("How many transactions were there in total?")
    print (len(transactions['id'].unique()))

    print ("Of these, how many transactions had one input and two outputs?")
    i = 0 # 1 input 2 output
    j = 0 # 1 input 1 output
    count_outputs = outputs['tx_id'].value_counts()
    count_inputs = inputs['tx_id'].value_counts()
    #print (count_inputs)
    for tx_id in count_outputs.index:
        if count_inputs[tx_id] == 1:
            n = count_outputs[tx_id]
            if n == 2:
                i += 1
            elif n == 1:
                j += 1
    print (i)

    print ("How many transactions had one input and one output?")
    print (j)


# In[13]:


answer_1_1()


# ### 1.2

# In[ ]:





# ### 1.3

# In[127]:


def answer_1_3():
    print ("------------------- 1.3 -------------------")
    print ("How many distinct public keys were used across all blocks in the dataset?")
    #pk_ids = pandas.concat([outputs['pk_id'], tags['pk_id']], axis=0, ignore_index=True).unique()
    print (len(outputs['pk_id'].unique())) #174702

    print ("Which public key received the highest number of bitcoins, and how many bitcoins has it received?")
    temp = outputs
    temp = temp.groupby('pk_id')['value'].sum()
    print(temp.idxmax(), temp.max())
    
    print ("Which public key acted as an output the most number of times, and how many times did it act as output?")
    count_pk = outputs['pk_id'].value_counts()
    print (count_pk.idxmax(), count_pk.max())


# In[128]:


answer_1_3()


# In[92]:


# outputs.loc[outputs['pk_id']==148105]


# ### 1.4

# In[ ]:





# In[ ]:




