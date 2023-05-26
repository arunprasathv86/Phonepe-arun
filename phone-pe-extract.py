import os
import json
import pandas as pd

path="/home/arunv/pulse/data/aggregated/transaction/country/india/state/"
user_state_list=os.listdir(path)
#print(user_state_list)
clm = {'State':[],'Year':[],'Quarter':[],'Transaction_type':[],'Transaction_count':[],
       'Transaction_amount':[]}
for i in user_state_list:
    p_i=path+i+"/"
    Agg_yr=os.listdir(p_i)
    #print(Agg_yr)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        #print(j,Agg_yr_list)
        for k in Agg_yr_list:
            p_k=p_j+k
            #print(k,p_k)
            Data=open(p_k,'r')
            D=json.load(Data)
            try:
                for z in D['data']['transactionData']:
                    Name=z['name']
                    count = z['paymentInstruments'][0]['count']
                    amount = z['paymentInstruments'][0]['amount']
                    clm['Transactction_type'].append(Name)
                    clm['Transactction_count'].append(count)
                    clm['Transactction_amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))
            except:
                pass

Agg_Trans=pd.DataFrame(clm)
#print(Agg_Trans)
