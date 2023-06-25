import requests
import subprocess
import os
import json
import pandas as pd
import numpy as np
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import streamlit as st

conn = mysql.connector.connect(host='localhost', user='root', password='BavaPreetha', db='phonepe_pulse')
cursor = conn.cursor()

st.header(':violet[Phonepe Data ]')

total_data = st.radio('**Select your option**',('All India', 'State wise','Top Ten list'),horizontal=True)

if total_data == 'All India':
  tab1, tab2 = st.tabs(['Transaction','User'])
  with tab1:
        col1, col2, col3 = st.columns(3)
        with col1:
            year_data = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022'),key='year_data')
        with col2:
            quarter_data = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='quarter_data')
        with col3:
            transaction_type = st.selectbox('**Select Transaction type**', ('Recharge & bill payments','Peer-to-peer payments',
            'Merchant payments','Financial Services','Others'),key='transaction_type')
            
        cursor.execute(f"SELECT State, Transaction_amount FROM aggregated_transaction WHERE Year = '{year_data}' AND Quarter = '{quarter_data}' AND Transaction_type = '{transaction_type}';")
        in_transaction_1 = cursor.fetchall()
        df_transaction_1 = pd.DataFrame(np.array(in_transaction_1), columns=['State', 'Transaction_amount'])
        st.write(f"Transaction Amount Statewise for '{year_data}' in '{quarter_data}' quarter for '{transaction_type}' ");
        st.bar_chart(df_transaction_1, x='State', y='Transaction_amount');
        
        cursor.execute(f"SELECT State, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE Year = '{year_data}' AND Quarter = '{quarter_data}' AND Transaction_type = '{transaction_type}';")
        in_transaction_2 = cursor.fetchall()
        df_transaction_2 = pd.DataFrame(np.array(in_transaction_2), columns=['State','Transaction_count','Transaction_amount'])
        st.write(f"Transaction Count Statewise for '{year_data}' in '{quarter_data}' quarter for '{transaction_type}' ");
        st.bar_chart(df_transaction_2, x='State', y='Transaction_count');
        
        cursor.execute(f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE Year = '{year_data}' AND Quarter = '{quarter_data}' AND Transaction_type = '{transaction_type}';")
        in_transaction_3 = cursor.fetchall()
        df_transaction_3 = pd.DataFrame(np.array(in_transaction_3), columns=['Total','Average'])
        st.write(f"Overall Sum and Average Transaction amount for '{year_data}' in '{quarter_data}' quarter for '{transaction_type}' ");
        st.write(df_transaction_3['Total'], df_transaction_3['Average']); 
        
        cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE Year = '{year_data}' AND Quarter = '{quarter_data}' AND Transaction_type = '{transaction_type}';")
        in_transaction_4 = cursor.fetchall()
        df_transaction_4 = pd.DataFrame(np.array(in_transaction_4), columns=['Total','Average'])
        st.write(f"Overall Sum and Average Transaction count for '{year_data}' in '{quarter_data}' quarter for '{transaction_type}' ");
        st.write(df_transaction_4['Total'], df_transaction_4['Average']);
  
  with tab2:
        
        col1, col2 = st.columns(2)
        with col1:
            user_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022'),key='user_year')
        with col2:
            user_quarter = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='user_quarter')
        

        cursor.execute(f"SELECT State, SUM(User_Count) FROM aggregated_user WHERE Year = '{user_year}' AND Quarter = '{user_quarter}' GROUP BY State;")
        in_transaction_5 = cursor.fetchall()
        df_transaction_5 = pd.DataFrame(np.array(in_transaction_5), columns=['State', 'User Count'])
        st.write(f"Transaction User Count Statewise for '{user_year}' in '{user_quarter}' ");
        st.bar_chart(df_transaction_5, x='State', y='User Count');

        cursor.execute(f"SELECT SUM(User_Count), AVG(User_Count) FROM aggregated_user WHERE Year = '{user_year}' AND Quarter = '{user_quarter}';")
        in_transaction_6 = cursor.fetchall()
        df_transaction_6 = pd.DataFrame(np.array(in_transaction_6), columns=['Total','Average'])
        st.write(f"Overall Sum and Average user count for '{year_data}' in '{quarter_data}' quarter for '{transaction_type}' ");
        st.write(df_transaction_6['Total'], df_transaction_6['Average']);
    


elif total_data =='State wise':
  st.header(':red[Phonepe Data ]')
  tab3, tab4 = st.tabs(['Transaction','User'])
  with tab3:

        col1, col2,col3 = st.columns(3)
        with col1:
            state_select = st.selectbox('**Select State**',('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh','assam', 'bihar', 
            'chandigarh', 'chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 
            'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh','maharashtra', 'manipur', 
            'meghalaya', 'mizoram', 'nagaland','odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana', 
            'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'),key='state_select')
        with col2:
            state_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022'),key='state_year')
        with col3:
            state_quarter = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='state_quarter')
            
        cursor.execute(f"SELECT Transaction_type, Transaction_amount FROM aggregated_transaction WHERE State = '{state_select}' AND Year = '{state_year}' AND Quarter = '{state_quarter}';")
        in_transaction_7 = cursor.fetchall()
        df_transaction_7 = pd.DataFrame(np.array(in_transaction_7), columns=['Transaction_type', 'Transaction_amount'])
        st.write(f"Transaction User Count Statewise for '{state_select}' in '{state_year}' quarter '{state_quarter}' ");
        st.bar_chart(df_transaction_7, x='Transaction_type', y='Transaction_amount');
      
        cursor.execute(f"SELECT Transaction_type, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE State = '{state_select}' AND Year = '{state_year}' AND Quarter = '{state_quarter}';")
        in_transaction_8 = cursor.fetchall()
        df_transaction_8 = pd.DataFrame(np.array(in_transaction_8), columns=['Transaction_type','Transaction_count','Transaction_amount'])
        dst.write(f"Transaction Count transaction type wise '{state_select}' in '{state_year}' quarter '{state_quarter}' ");
        st.bar_chart(df_transaction_8, x='Transaction_type', y='Transaction_count');
        st.write(f"Transaction Amount transaction type wise '{state_select}' in '{state_year}' quarter '{state_quarter}' ");
        st.bar_chart(df_transaction_8, x='Transaction_type', y='Transaction_amount');
          
        cursor.execute(f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE State = '{state_select}' AND Year = '{state_year}' AND Quarter = '{state_quarter}';")
        in_transaction_9 = cursor.fetchall()
        df_transaction_9 = pd.DataFrame(np.array(in_transaction_9), columns=['Total','Average'])
        st.write(df_transaction_9)
        
        cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE State = '{state_select}' AND Year ='{state_year}' AND Quarter = '{state_quarter}';")
        in_transaction_10 = cursor.fetchall()
        df_transaction_10 = pd.DataFrame(np.array(in_transaction_10), columns=['Total','Average'])
        st.write(df_transaction_10)
   
  with tab4:
        col5, col6 = st.columns(2)
        with col5:
            state_user_select = st.selectbox('**Select State**',('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh','assam', 'bihar', 
            'chandigarh', 'chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 
            'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh','maharashtra', 'manipur', 
            'meghalaya', 'mizoram', 'nagaland','odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana', 
            'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'),key='state_user_select')
        with col6:
            state_user_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022'),key='state_user_year')
         
        cursor.execute(f"SELECT Quarter, SUM(User_Count) FROM aggregated_user WHERE State = '{state_user_select}' AND Year = '{state_user_year}' GROUP BY Quarter;")
        in_transaction_11 = cursor.fetchall()
        df_transaction_11 = pd.DataFrame(np.array(in_transaction_11), columns=['Quarter', 'User Count'])
        st.write(df_transaction_11)
           
        cursor.execute(f"SELECT SUM(User_Count), AVG(User_Count) FROM aggregated_user WHERE State = '{state_user_select}' AND Year = '{state_user_year}';")
        in_transaction_12 = cursor.fetchall()
        df_transaction_12 = pd.DataFrame(np.array(in_transaction_12), columns=['Total','Average'])
        st.write(df_transaction_12)
        
        df_transaction_11['Quarter'] = df_transaction_11['Quarter'].astype(int)
        df_transaction_11['User Count'] = df_transaction_11['User Count'].astype(int)
        df_transaction_11_fig = px.bar(df_transaction_11 , x = 'Quarter', y ='User Count', color ='User Count', color_continuous_scale = 'thermal', title = 'User Analysis Chart', height = 500,)
        df_transaction_11_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
        st.plotly_chart(df_transaction_11_fig,use_container_width=True)

        st.header(':violet[Total calculation]')

        col3, col4 = st.columns(2)
        with col3:
            st.subheader('User Analysis')
            st.dataframe(df_transaction_11)
        with col4:
            st.subheader('User Count')
            st.dataframe(df_transaction_12)

       
        
  
  

  

        
       
    
  
    
else:
  tab5, tab6 = st.tabs(['Transaction','User'])
  with tab5:
        top_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022'),key='top_year')
        
        cursor.execute(f"SELECT State, SUM(Transaction_amount) As Transaction_amount FROM top_transaction WHERE Year = '{top_year}' GROUP BY State ORDER BY Transaction_amount DESC LIMIT 10;")
        in_transaction_13 = cursor.fetchall()
        df_transaction_13 = pd.DataFrame(np.array(in_transaction_13), columns=['State', 'Top Transaction amount'])
        st.write(df_transaction_13)
        
		cursor.execute(f"SELECT State, SUM(Transaction_amount) as Transaction_amount, SUM(Transaction_count) as Transaction_count FROM top_transaction WHERE Year = '{top_year}' GROUP BY State ORDER BY Transaction_amount DESC LIMIT 10;")
        in_transaction_14 = cursor.fetchall()
        df_transaction_14 = pd.DataFrame(np.array(in_transaction_14), columns=['State', 'Top Transaction amount','Total Transaction count'])
        st.write(df_transaction_14)
        
        
  with tab6:
        top_user_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022'),key='top_user_year') 
        
        cursor.execute(f"SELECT State, SUM(Registered_User) AS Top_user FROM top_user WHERE Year='{top_user_year}' GROUP BY State ORDER BY Top_user DESC LIMIT 10;")
        in_transaction_15 = cursor.fetchall()
        df_transaction_15 = pd.DataFrame(np.array(in_transaction_15), columns=['State', 'Total User count'])
        st.write(df_transaction_15)



   