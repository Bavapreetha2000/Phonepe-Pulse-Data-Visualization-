import requests
import pygit
import streamlit as st
import pandas as pd
import os
import json
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine

#cloning
response = requests.get('https://api.github.com/repos/PhonePe/pulse')
repo = response.json()
clone_url = repo['clone_url']
repo_name = "pulse"
#st.write(os.getcwd())
clone_dir= os.path.join(os.getcwd(), repo_name)
subprocess.run(["git", "clone", clone_url, clone_dir], check=True)

#table creation
path_1="C:/Phonepe/pulse-master/data/aggregated/transaction/country/india/state/"
aggregate_transaction_state_list = os.listdir(path_1)
aggregate_transaction = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in aggregate_transaction_state_list:
    p_i = path_1 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            A = json.load(Data)
            
            for l in A['data']['transactionData']:
                Name = l['name']
                count = l['paymentInstruments'][0]['count']
                amount = l['paymentInstruments'][0]['amount']
                aggregate_transaction['State'].append(i)
                aggregate_transaction['Year'].append(j)
                aggregate_transaction['Quarter'].append(int(k.strip('.json')))
                aggregate_transaction['Transaction_type'].append(Name)
                aggregate_transaction['Transaction_count'].append(count)
                aggregate_transaction['Transaction_amount'].append(amount)

df_aggregated_transaction = pd.DataFrame(aggregate_transaction)


path_2 = "C:/Phonepe/pulse-master/data/aggregated/user/country/india/state/"
aggregate_user_state_list = os.listdir(path_2)
aggregate_user = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [], 'User_Percentage': []}

for i in aggregate_user_state_list:
    p_i = path_2 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            B = json.load(Data)
            
            try:
                for l in B["data"]["usersByDevice"]:
                    brand_name = l["brand"]
                    count_ = l["count"]
                    ALL_percentage = l["percentage"]
                    aggregate_user["State"].append(i)
                    aggregate_user["Year"].append(j)
                    aggregate_user["Quarter"].append(int(k.strip('.json')))
                    aggregate_user["Brands"].append(brand_name)
                    aggregate_user["User_Count"].append(count_)
                    aggregate_user["User_Percentage"].append(ALL_percentage*100)
            except:
                pass

df_aggregated_user = pd.DataFrame(aggregate_user)

         

path_3 = "C:/Phonepe/pulse-master/data/map/transaction/hover/country/india/state/"
map_transaction_ststae_list = os.listdir(path_3)

map_transaction = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Transaction_Count': [], 'Transaction_Amount': []}

for i in map_transaction_ststae_list:
    p_i = path_3 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            C = json.load(Data)
            
            for l in C["data"]["hoverDataList"]:
                District = l["name"]
                count = l["metric"][0]["count"]
                amount = l["metric"][0]["amount"]
                map_transaction['State'].append(i)
                map_transaction['Year'].append(j)
                map_transaction['Quarter'].append(int(k.strip('.json')))
                map_transaction["District"].append(District)
                map_transaction["Transaction_Count"].append(count)
                map_transaction["Transaction_Amount"].append(amount)
                
df_map_transaction = pd.DataFrame(map_transaction)



path_4 = "C:/Phonepe/pulse-master/data/map/user/hover/country/india/state/"
map_user_state_list = os.listdir(path_4)

map_user = {"State": [], "Year": [], "Quarter": [], "District": [], "Registered_User": []}

for i in map_user_state_list:
    p_i = path_4 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            D = json.load(Data)

            for l in D["data"]["hoverData"].items():
                district = l[0]
                registereduser = l[1]["registeredUsers"]
                map_user['State'].append(i)
                map_user['Year'].append(j)
                map_user['Quarter'].append(int(k.strip('.json')))
                map_user["District"].append(district)
                map_user["Registered_User"].append(registereduser)
                
df_map_user = pd.DataFrame(map_user)



path_5 = "C:/Phonepe/pulse-master/data/top/transaction/country/india/state/"
top_transaction_state_list = os.listdir(path_5)

top_transaction = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in top_transaction_state_list:
    p_i = path_5 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            E = json.load(Data)
            
            for l in E['data']['pincodes']:
                Name = l['entityName']
                count = l['metric']['count']
                amount = l['metric']['amount']
                top_transaction['State'].append(i)
                top_transaction['Year'].append(j)
                top_transaction['Quarter'].append(int(k.strip('.json')))
                top_transaction['District_Pincode'].append(Name)
                top_transaction['Transaction_count'].append(count)
                top_transaction['Transaction_amount'].append(amount)

df_top_transaction = pd.DataFrame(top_transaction)



path_6 = "C:/Phonepe/pulse-master/data/top/user/country/india/state/"
top_user_state_list = os.listdir(path_6)

top_user = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Registered_User': []}

for i in top_user_state_list:
    p_i = path_6 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            F = json.load(Data)
            
            for l in F['data']['pincodes']:
                Name = l['name']
                registeredUser = l['registeredUsers']
                top_user['State'].append(i)
                top_user['Year'].append(j)
                top_user['Quarter'].append(int(k.strip('.json')))
                top_user['District_Pincode'].append(Name)
                top_user['Registered_User'].append(registeredUser)
                
df_top_user = pd.DataFrame(top_user)         
 
#st.write(os.getcwd())        
         
mydb = mysql.connector.connect(
  host = "localhost",
  user = "root",
  password = "BavaPreetha",
  auth_plugin = "mysql_native_password"
)
#st.write(os.getcwd())
mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_pulse")
#st.write(os.getcwd())
mycursor.close()
mydb.close()

# Connect to db
engine = create_engine('mysql+mysqlconnector://root:BavaPreetha@localhost/phonepe_pulse', echo=False)

df_aggregated_transaction.to_sql('aggregated_transaction', engine, if_exists = 'replace', index=False,   
                                 dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                       'Year': sqlalchemy.types.Integer, 
                                       'Quater': sqlalchemy.types.Integer, 
                                       'Transaction_type': sqlalchemy.types.VARCHAR(length=50), 
                                       'Transaction_count': sqlalchemy.types.Integer,
                                       'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

df_aggregated_user.to_sql('aggregated_user', engine, if_exists = 'replace', index=False,
                          dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                 'Year': sqlalchemy.types.Integer, 
                                 'Quater': sqlalchemy.types.Integer,
                                 'Brands': sqlalchemy.types.VARCHAR(length=50), 
                                 'User_Count': sqlalchemy.types.Integer, 
                                 'User_Percentage': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
                 
df_map_transaction.to_sql('map_transaction', engine, if_exists = 'replace', index=False,
                          dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                 'Year': sqlalchemy.types.Integer, 
                                 'Quater': sqlalchemy.types.Integer, 
                                 'District': sqlalchemy.types.VARCHAR(length=50), 
                                 'Transaction_Count': sqlalchemy.types.Integer, 
                                 'Transaction_Amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

df_map_user.to_sql('map_user', engine, if_exists = 'replace', index=False,
                   dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                          'Year': sqlalchemy.types.Integer, 
                          'Quater': sqlalchemy.types.Integer, 
                          'District': sqlalchemy.types.VARCHAR(length=50), 
                          'Registered_User': sqlalchemy.types.Integer, })
              
df_top_transaction.to_sql('top_transaction', engine, if_exists = 'replace', index=False,
                         dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                'Year': sqlalchemy.types.Integer, 
                                'Quater': sqlalchemy.types.Integer,   
                                'District_Pincode': sqlalchemy.types.Integer,
                                'Transaction_count': sqlalchemy.types.Integer, 
                                'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

df_top_user.to_sql('top_user', engine, if_exists = 'replace', index=False,
                   dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                          'Year': sqlalchemy.types.Integer, 
                          'Quater': sqlalchemy.types.Integer,                           
                          'District_Pincode': sqlalchemy.types.Integer, 
                          'Registered_User': sqlalchemy.types.Integer,})


         