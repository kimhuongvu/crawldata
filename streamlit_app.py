import streamlit as st
import sys
import pandas as pd
import random
sys.path.insert(1, "/home/bis2023/.local/bin")
from streamlit_option_menu import option_menu
# Import the required modules
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from time import sleep
from stqdm import stqdm
from crawl_data import start_driver, close_driver, get_product_info_from_page


# Initialize the hostname, username, password, and port variables
hostname = ["172.17.0.2"]
user_name = "cassandra"
pass_word = "cassandra"
portname = 9042

# Create a Cluster object with the provided hostname and port
auth_provider = PlainTextAuthProvider(username=user_name, password=pass_word)
cluster = Cluster(hostname,port=portname,auth_provider=auth_provider)
sessiondb = cluster.connect('crawldata')

result_set =sessiondb.execute(
    """
    select * from dim_products 
"""
)
# Convert to df
data = []
for row in result_set:
    data.append(row._asdict())

# Convert the list of dictionaries into a DataFrame
df = pd.DataFrame(data)
df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d')
st.balloons()

data_link = '/home/bis2023/Documents/tiki_products.csv'

with st.sidebar:
  selected = option_menu(
    menu_title = "Crawl Data",
    options = ["Monitoring","By Link"],
    icons = ["grid-3x2-gap-fill","cloud-upload","list-task"],
    menu_icon = "cast",
    default_index = 0,
  )


def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')


if selected == "Monitoring":
    st.title(f"You Have selected {selected}")
    
    # overview
    df_monitor = df[df['name'].notna()]
    total_product = df_monitor.shape[0]

    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric('Total products', f'{total_product}')

    with col2:
        highest_price = int(df_monitor['price'].max())
        st.metric('Highest price', f'{highest_price}')
    
    with col3:
        highest_sold = int(df_monitor['sold'].max())
        st.metric('Highest sold', f'{highest_sold}')

    # markdown
    st.markdown("""---""")
    
    # line chart
    prod_by_days = df_monitor.groupby(['created_at'])['name'].count().reset_index()
    st.bar_chart(
        prod_by_days,
        x='created_at',
        y='name',
        color="#66ccff"
    )
    
    # bar chart
    bar_chart_data = df_monitor.groupby(['category'])['name'].count().reset_index()
    st.bar_chart(
        bar_chart_data,
        x='category',
        y='name',
        color="#99ff66")
    
    top_cate_sold = df_monitor.groupby('category')['sold'].sum().reset_index().sort_values(by='sold', ascending=False)
    st.bar_chart(
        top_cate_sold,
        x='category',
        y='sold',
        color="#66ccff")
      
    # markdown
    st.markdown("""---""")
    
    # download file
    csv = convert_df(df_monitor)
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name='crawl_data_cassandra_group.csv',
        mime='text/csv',
    )
    
    # table 
    st.table(df_monitor.head(5))
    

if selected == "By Link":
    st.title(f"You Have selected {selected}")
    # Create a text input 
    user_input = st.text_input('Enter a link you want to crawl:')
    print(user_input) 
    if len(user_input) >= 30:
        for _ in stqdm(range(50)):
            sleep(0.5)
            start_driver(force_restart=True)
            sleep(random.randint(5,10))
            page_result= get_product_info_from_page(user_input)
            result_df = pd.DataFrame(page_result)
            st.table(result_df)
            st.download_button(
                label="Download data as CSV",
                data=result_df,
                file_name='crawl_data_by_link_cassandra_group.csv',
                mime='text/csv',
            )

