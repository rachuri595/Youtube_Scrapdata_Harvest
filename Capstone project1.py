    #Importing necessary libraries

import googleapiclient.discovery
import pandas as pd
import mysql.connector as sql
import streamlit as st
from streamlit_option_menu import option_menu
import time
import pymongo
from pymongo import MongoClient
import matplotlib.pyplot as plt
from datetime import datetime

api_key='AIzaSyBB8U8DNGQPc5QpBjOwAPoEPUmGFyRqspA'
youtube = googleapiclient.discovery.build('youtube', "v3", developerKey=api_key)

# Connecting to MongoDB Compass

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["youtube_scrapdata"]

# Connceting to a SQL server and creaating new database

my_db = sql.connect( 
             host="127.0.0.1",
             user="root",
             database = "youtube_scrapharvest",
             port = 3306,
             password="334298",
              ) 
      
mycursor = my_db.cursor() 



#########################################################################################                    
# Creating Channel details Function
##########################################################################################

def channel_details(channel_id):
  ch_id = []
  request = youtube.channels().list(
          part="snippet,contentDetails,statistics",
          id=channel_id
      )
  response = request.execute()

  for i in range(len(response['items'])):
    z = dict(title = response['items'][i]['snippet']['title'],
             channel_id = response['items'][i]['id'],
             des = response['items'][i]['snippet']['description'],
             joined = response['items'][i]['snippet']['publishedAt'],
             thumbnails = response['items'][i]['snippet']['thumbnails']['medium']['url'],
             sub_count = response['items'][i]['statistics']['subscriberCount'],
             vid_count = response['items'][i]['statistics']['videoCount'],
             views = response['items'][i]['statistics']['viewCount'])
    ch_id.append(z)
  return ch_id


##########################################################################################
# Retrieving all video id details Function
###########################################################################################

def uploads_id(channel_id):
    request = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    )
    response = request.execute()
    uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    return uploads_playlist_id


def playlist_items(playlist_id):
  page_num = None
  col_video_id =[]

  while True:

    request = youtube.playlistItems().list(
      part="contentDetails",
      maxResults=50,
      pageToken = page_num,
      playlistId=playlist_id
    )

    response = request.execute()

    if 'items' in response:
      y = response['items']
      for i in y:
        video_id = i['contentDetails']['videoId']
        col_video_id.append(video_id)
    else:
      break

    page_num = response.get("nextPageToken")

    if not page_num:
        break

  return col_video_id


def retrieve_all_video_id(channel_id):
  upload_playlist_id = uploads_id(channel_id)
  all_video_id = playlist_items(upload_playlist_id)
  return (all_video_id)



##########################################################################################
# Creating video_details Function
###########################################################################################

def video_details(video_id):
    vid_stat = []  # Initialize vid_stat as an empty list
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
    response = request.execute()

    for i in response['items']:
        z= dict(
            channel_id = i['snippet']['channelId'],
            channel_name = i['snippet']['channelTitle'] ,
            video_id=i['id'],
            video_name=i['snippet']['title'],
            video_description=i['snippet']['description'],
            published_date=i['snippet']['publishedAt'],
            view_count=i['statistics'].get('viewCount'),
            comment_count=i['statistics'].get('commentCount'),
            duration=i['contentDetails']['duration'],
            thumbnail=i['snippet']['thumbnails']['default']['url'],
            caption_status=i['contentDetails']['caption'],
            favorite_count=i['statistics'].get('favoriteCount'),
            like_count = i['statistics'].get('likeCount')
        )

        vid_stat.append(z)

    return vid_stat


##########################################################################################
# Defining Comment section function
###########################################################################################

def comment_section(comment_video_id):

  data=[]

  try:
    page_num = None

    while True:

      request = youtube.commentThreads().list(
      part="snippet,replies",
      pageToken = page_num,
      maxResults=50,
      videoId=comment_video_id
      )
      response = request.execute()

      for i in response.get('items',[]):
        y= dict(channel_id = i['snippet']['channelId'],
              comment_id= i['id'],
              comment_video_id = i['snippet']['videoId'],
              comment_text = i['snippet']['topLevelComment']['snippet']['textOriginal'],
              comment_author = i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
              comment_published_date = i['snippet']['topLevelComment']['snippet']['publishedAt'])
        data.append(y)

      page_num = response.get("nextPageToken")

      if not page_num:
        break
  except:
    pass

  return data

##########################################################################################
# Collective video_details and comment_section details by channel_id
###########################################################################################

def video_details_by_channel(channel_id):
    video_ids = retrieve_all_video_id(channel_id)
    
    all_video_details = []
    
    for video_id in video_ids:
        video_info = video_details(video_id)
        all_video_details.extend(video_info)
    
    return all_video_details

def comment_section_by_channel(channel_id):
    video_ids = retrieve_all_video_id(channel_id)
   
    all_comments = []
    
    for video_id in video_ids:  
        video_comments = comment_section(video_id)
        all_comments.extend(video_comments) 
    
    return all_comments


##########################################################################################
# Streamlit Markdowns
###########################################################################################

st.title('**YouTube Scrap Data Harvesting and Warehousing using SQL, MongoDB and Streamlit**', anchor = False)

selected = option_menu(
    menu_title = None,
    options=["Home", "Retrieval & Transformation","Insights"],
    icons=["house-fill","database-fill","bar-chart-fill" ],
    default_index = 0,
    menu_icon="cast",
    orientation="horizontal",
    key="navigation_menu",
    styles={
            "font_color": "#DC143C",   
            "border": "2px solid #DC143C", 
            "padding": "10px 25px"   
        }

)
##########################################################################################
  # HOME Page 
###########################################################################################
if selected == "Home":
  col1, col2 = st.columns(2)

  with col1:
   st.header(":red[Abstract] 	:bookmark_tabs:", anchor = False)
   st.divider()
   with st.container(height=400):
     st.markdown("#### Retrieving the Youtube channels data from youtube api via Google Console, creating and transforming the data into a SQL database as a Data Warehouse, also storing the data in mongodb and querying the data to display in Streamlit.")

  with col2:
   st.header(":red[Tools] :wrench: ", anchor = False)
   st.divider()
   with st.container(height=400):
     st.markdown("- #### Streamlit \n - #### Python\n - #### MongoDB\n -  #### MySQL Workbench\n - #### Google Search Console API")

##########################################################################################
  # Youtube Data Retrieval Page
###########################################################################################

if selected == "Retrieval & Transformation":
  tab1,tab2 = st.tabs(["📂:red[DATA EXTRACTION & MONGODB CONVERSION]","🗒️:green[TRANSFORMATION TO MYSQL]"])

  with tab1:
    st.header(":red[DATA EXTRACTION & MONGODB CONVERSION]", anchor = False)
    st.divider()
    channel_id = st.text_input("**Enter your Youtube ID here:**")  #"UCqW8jxh4tH1Z1sWPbkGWL4g" #Akshat Srivatsava

    if channel_id and st.button("Extract Data"):
      details = channel_details(channel_id)
      st.table(details)  
      st.success("Channel Uploaded succesfully")
      st.divider() 

    if channel_id and st.button("Convert Data to MongoDB document"):
      with st.status("Uploading data...",expanded=True) :
        st.write("Searching Database...")
        time.sleep(2)


        ch_details = channel_details(channel_id)
        vid_details = video_details_by_channel(channel_id)
        comment_details = comment_section_by_channel(channel_id)


        st.write("Opening Database")
        time.sleep(1)
              
        st.write("Creating Database...")
        time.sleep(1)

        collection1 = db["Channel_details"]
        collection1.insert_one({"channel_information":ch_details, "video_information": vid_details,"comment_information":comment_details})
            
            
        st.success("Upload to MongoDB document successful !!")
        


##########################################################################################
# SQL Conversion tab
###########################################################################################

  with tab2:
    st.header(":red[Select a channel to begin Transformation in MYSQL SERVER]", anchor = False)
    st.divider()

# Channel_names drop down option
    def channel_names():   
      ch_names = []
      for i in db["Channel_details"].find({}, {"_id": 0, "channel_information.title": 1}):
        channel_info = i['channel_information'][0]  
        ch_names.append(channel_info['title'])
      return ch_names



    ch_names = channel_names()  
    option = st.selectbox("Choose a channel", options=ch_names)
    st.write("You selected:", option)

# Convert Publised_at column values to Datetime format
    def convert_to_datetime(date_str):
      try:
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
      except ValueError:
        return None
      
 # Convert Duration column values to minutes format   
        
    def convert_minutes(duration_str):
        
      duration_str = duration_str[2:] 
      hours = 0 
      minutes = 0
      seconds = 0
      if 'H' in duration_str:
        hours_str = duration_str.split('H')[0]
        hours = int(hours_str)
        duration_str = duration_str.replace(hours_str + 'H', '')   
      if 'M' in duration_str:
        minutes_str = duration_str.split('M')[0]
        minutes = int(minutes_str)
        duration_str = duration_str.replace(minutes_str + 'M', '')
          
      if 'S' in duration_str:
        seconds_str = duration_str.split('S')[0]
        seconds = int(seconds_str)

      total_minutes = (hours*60) + minutes + (seconds / 60 ) 
      return total_minutes

          

    def convert_channel_to_sql():
       
      table1 = '''CREATE TABLE IF NOT EXISTS channels (Channel_name VARCHAR(255),
                                                           Channel_Id VARCHAR(255) PRIMARY KEY,
                                                           Description TEXT,
                                                           Joined DATETIME,
                                                           Thumbnail TEXT,
                                                           Subscribers INT,
                                                           Total_videos INT,
                                                           Total_views INT )'''
      mycursor.execute(table1)
      my_db.commit()

      collection1 = db["Channel_details"]
      data = collection1.find_one({"channel_information.channel_id": channel_id})

      df_channel = pd.DataFrame(data["channel_information"],index=[0])
        
      # Inserting values in to sql tables
        
      insert_channel = '''INSERT INTO channels ( Channel_name,
                                            Channel_Id ,
                                            Description, 
                                            Joined ,
                                            Thumbnail ,
                                            Subscribers ,
                                            Total_videos ,
                                            Total_views )
                                        values (%s,%s,%s,%s,%s,%s,%s,%s)'''
        
      for index, row in df_channel.iterrows():
        row["joined"] = convert_to_datetime(row["joined"])
        mycursor.execute(insert_channel, row.values.tolist())
        
      my_db.commit()


    def convert_video_to_sql():
    # Creating Videos Table
      table2 = '''CREATE TABLE IF NOT EXISTS Videos (Channel_Id VARCHAR(255),
                                                     Channel_Name VARCHAR(255),  
                                                     Video_id VARCHAR(255)  PRIMARY KEY,
                                                     Video_name VARCHAR(255), 
                                                     Description TEXT,
                                                     Published_Date DATETIME,
                                                     Views INT,
                                                     No_of_comments INT,
                                                     Length VARCHAR(25),
                                                     Thumbnail TEXT,
                                                     Caption TEXT,
                                                     Favourites INT,
                                                     Likes INT )'''
      mycursor.execute(table2)
      my_db.commit()


      collection1 = db["Channel_details"] 
      data = collection1.find_one({"channel_information.channel_id": channel_id})
        
      df_video = pd.DataFrame(data["video_information"])

      insert_video =  '''INSERT INTO Videos (
                                              Channel_Id ,
                                              Channel_name ,
                                              Video_id ,
                                              Video_name , 
                                              Description,
                                              Published_Date,
                                              Views,
                                              No_of_comments,
                                              Length,
                                              Thumbnail,
                                              Caption,
                                              Favourites,
                                              Likes )
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
      for index, row in df_video.iterrows():
        row['published_date'] = datetime.strptime(row['published_date'], '%Y-%m-%dT%H:%M:%SZ')
        row['duration'] = convert_minutes(row['duration'])
        # Convert the row values into a tuple to maintain the order of elements
        values = tuple(row.values)
        mycursor.execute(insert_video, values)
          
      my_db.commit()
      

    def convert_comment_to_sql():

      table3 = '''CREATE TABLE IF NOT EXISTS Comments (
                                    Channel_Id VARCHAR(255),
                                    Comment_id VARCHAR(255),
                                    Video_id VARCHAR(255),
                                    Comment TEXT,
                                    Comment_author VARCHAR(255),
                                    Comment_Date DATETIME,
                                    PRIMARY KEY (Comment_id))'''
      mycursor.execute(table3)
      my_db.commit()
        
      collection1 = db["Channel_details"] 
      data = collection1.find_one({"channel_information.channel_id": channel_id})
    
      df_comment =pd.DataFrame(data["comment_information"])
        
      insert_comment = '''INSERT INTO Comments (
                  Channel_Id, 
                  Comment_id ,
                  Video_id ,
                  Comment ,
                  Comment_author,
                  Comment_Date 
                  )
                  VALUES (%s,%s,%s,%s,%s,%s)'''
        
      for index, row in df_comment.iterrows():
        row['comment_published_date'] = datetime.strptime(row['comment_published_date'], '%Y-%m-%dT%H:%M:%SZ')
        values = tuple(row.values)
        mycursor.execute(insert_comment, values)
      my_db.commit()

    if st.button("Upload"):
      try:
        convert_channel_to_sql()
        convert_video_to_sql()
        convert_comment_to_sql()
        st.success("Channel details uploaded successfully to Mysql Server database.....")
      except:
        st.error("Channel ID already uploaded into Mysql server!!!") 
    
      


##########################################################################################
# Insights Page
###########################################################################################


if selected == "Insights":
  st.markdown("# ")
  st.markdown(":red[Select one of the options to have insights on Youtube Data]")
  all_questions = st.selectbox("Please select the Questions",
                           ('1.What are the names of all the videos and their corresponding channels?',
                           '2.Which channels have the most number of videos, and how many videos do they have?',
                           '3.What are the top 10 most viewed videos and their respective channels?',
                           '4.How many comments were made on each video, and what are their corresponding video names?',
                           '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
                           '6.Which channel has most number of Subscribers?',
                           '7.What is the total number of views for each channel, and what are their corresponding channel names?',
                           '8.What are the names of all the channels that have published videos in the year 2023?',
                           '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                           '10.Which videos have the highest number of comments, and what are their corresponding channel names?',)) 
  

  if all_questions == '1.What are the names of all the videos and their corresponding channels?':
    mycursor.execute('''Select channel_id, video_name
                      from videos 
                      order by channel_id''')
    df = pd.DataFrame(mycursor.fetchall(),columns = mycursor.column_names)
    st.write(df)


  elif all_questions == "2.Which channels have the most number of videos, and how many videos do they have?":
     mycursor.execute('''Select channel_name, count(video_id) as No_of_videos
                     from videos 
                     group by channel_name''')
     df = pd.DataFrame(mycursor.fetchall(),columns = mycursor.column_names)
     st.write(df)
     plt.figure(figsize=(10,8))
     x = plt.bar(df['channel_name'], df['No_of_videos'], color='green')
     plt.title("Total Videos per channel")
     plt.tight_layout()
     plt.xticks(rotation=90, ha='right') 
     plt.show()
     st.pyplot(plt.gcf(), use_container_width=True)


  elif all_questions == "3.What are the top 10 most viewed videos and their respective channels?":
     mycursor.execute('''select channel_name, views 
                 from videos
                 order by views desc
                 limit 10''')
     df = pd.DataFrame(mycursor.fetchall(),columns = mycursor.column_names)
     st.write(df)
  

  elif all_questions == "4.How many comments were made on each video, and what are their corresponding video names?":
    mycursor.execute('''select  Video_name , count(comment) as Total_comments from videos v
                 join comments c on c.video_id = v.video_id
                 group by video_name 
                 order by video_name desc
                 ''')
    df = pd.DataFrame(mycursor.fetchall(),columns = mycursor.column_names)
    st.write(df)


  elif all_questions == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    mycursor.execute('''select channel_name,video_name, likes 
                    from videos
                 ''')
    df = pd.DataFrame(mycursor.fetchall(),columns = mycursor.column_names)
    st.write(df)
    plt.figure(figsize=(10,8))
    x= plt.bar(df['channel_name'], df['likes'], color='skyblue')
    plt.title("Total number of Likes per channel")
    plt.tight_layout()
    plt.xticks(rotation=90, ha='right') 
    plt.show()
    st.pyplot(plt.gcf(), use_container_width=True)



  elif all_questions == "6.Which channel has most number of Subscribers?":
    mycursor.execute('''select channel_name,subscribers, Total_videos
                 from channels
                 order by subscribers desc
                 ''')
    df = pd.DataFrame(mycursor.fetchall(),columns = mycursor.column_names)
    st.write(df)

    plt.figure(figsize=(10,8))
    x = plt.bar(df['channel_name'], df['subscribers'], color='red')
    plt.title("Total number of Subscribers per Channel ")
    plt.tight_layout()
    plt.xticks(rotation=90, ha='right') 
    plt.show()
    st.pyplot(plt.gcf(), use_container_width=True)


  elif all_questions == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
    mycursor.execute('''select channel_name,total_views
                 from channels
                 ''')
    df = pd.DataFrame(mycursor.fetchall(),columns = mycursor.column_names)
    st.write(df)

    plt.figure(figsize=(10,8))
    x = plt.bar(df['channel_name'], df['total_views'], color='green')
    plt.title("Total views per Channel")
    plt.xticks(rotation=90, ha='right') 
    plt.tight_layout()
    st.pyplot(plt.gcf(), use_container_width=True)


  elif all_questions == "8.What are the names of all the channels that have published videos in the year 2023?":
    mycursor.execute('''select distinct channel_name
                    from videos
                    where YEAR(published_date) = 2023
                 ''')
    df = pd.DataFrame(mycursor.fetchall(),columns = mycursor.column_names)
    st.write(df)


  elif all_questions == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    mycursor.execute('''select channel_name, avg(length) as Average_duration
                    from videos
                    group by channel_name
                    order by avg(length) desc
                 ''')
    df = pd.DataFrame(mycursor.fetchall(),columns = mycursor.column_names)
    df
    plt.figure(figsize=(10,8))
    x = plt.bar(df['channel_name'], df['Average_duration'], color='red')
    plt.title("Average Duration of all Videos per Channel")
    plt.tight_layout()
    plt.xticks(rotation=90, ha='right') 
    plt.show()
    st.pyplot(plt.gcf(), use_container_width=True)



  elif all_questions == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    mycursor.execute('''select channel_name ,video_id ,no_of_comments 
                    from videos
                    order by no_of_comments desc
                 ''')
    df = pd.DataFrame(mycursor.fetchall(),columns = mycursor.column_names)
    st.write(df)

    plt.figure(figsize=(10,8))
    x = plt.bar(df['channel_name'], df['no_of_comments'], color='skyblue')
    plt.title("Highest Comments per Channel")
    plt.tight_layout()
    plt.xticks(rotation=90, ha='right') 
    plt.show()
    st.pyplot(plt.gcf(), use_container_width=True)
#  plt.gcf()is used to ensure to get current figure to run in streamlit app in the browser.
