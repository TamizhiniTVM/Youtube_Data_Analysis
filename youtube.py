from googleapiclient.discovery import build
from pprint import pprint
import pandas as pd
from pymongo import MongoClient
import pymysql
import mysql.connector
import streamlit as st
import sqlalchemy
from sqlalchemy import create_engine
import plotly.express as px

api_key = "AIzaSyCvLTuHUjzvIqgOy4Z7QyQRplxqOGlyw_0"
api_service_name = "youtube"
api_version = "v3"

youtube = build(api_service_name, api_version, developerKey=api_key)

def channel_info(channel_id):
  request = youtube.channels().list(
      part="snippet,contentDetails,statistics",
      id=channel_id)
  response = request.execute()
  
  channel_data = dict(channel_name = response['items'][0]['snippet']['title'],
                    channel_id = response['items'][0]['id'],
                    description = response['items'][0]['snippet']['description'],
                    joined = response['items'][0]['snippet']['publishedAt'],
                    sub_count = response['items'][0]['statistics']['subscriberCount'],
                    video_count = response['items'][0]['statistics']['videoCount'],
                    view_count = response['items'][0]['statistics']['viewCount'],
                    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])

  return channel_data

def videoid_info(channel_id):
  video_ids=[]
  request = youtube.channels().list(
      part="contentDetails",
      id=channel_id)
  response = request.execute()

  playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  next_page_token=None

  while True:
    request = youtube.playlistItems().list(
              part="contentDetails",
              maxResults = 50,
              playlistId = playlist_id,
              pageToken=next_page_token
          )
    response1 = request.execute()

    for i in range(len(response1['items'])):
      video_ids.append(response1['items'][i]['contentDetails']['videoId'])

    next_page_token=response1.get('nextPageToken')
    if next_page_token==None:
      break

  return video_ids

def video_info(Video_Ids):
  videos_data=[]

  for video_id in Video_Ids:
    next_page_token=None

    while True:
      request = youtube.videos().list(
              part="snippet,contentDetails,statistics,topicDetails",
              maxResults=50,
              id=video_id,
              pageToken=next_page_token
          )
      response = request.execute()

      for item in response['items']:
        video_data=  dict(channel_id = item['snippet']['channelId'],
                        videoid = item['id'],
                        video_name = item['snippet']['title'],
                        description = item['snippet']['description'],
                        publised = item['snippet']['publishedAt'],
                        tags = item['snippet'].get('tags'),
                        thumbnails = item['snippet']['thumbnails']['default']['url'],
                        likes = item['statistics'].get('likeCount'),
                        views = item['statistics'].get('viewCount'),
                        comments = item['statistics'].get('commentCount'),
                        favorite = item['statistics'].get('favoriteCount'),
                        duration = item['contentDetails']['duration'],
                        caption = item['contentDetails']['caption'])

        videos_data.append(video_data)
      next_page_token=response.get('nextPageToken')
      if next_page_token==None:
        break

  return videos_data

def comment_info(video_ids):
  comment_ids=[]

  try:
    for video_id in video_ids:
      request = youtube.commentThreads().list(
            part="snippet,replies",
            maxResults=50,
            videoId=video_id
        )
      response = request.execute()

      for item in response['items']:
        comment_data = dict(channel_id = item['snippet']['channelId'],
                            comment_id = item['snippet']['topLevelComment']['id'],
                            comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_published = item['snippet']['topLevelComment']['snippet']['publishedAt'])

        comment_ids.append(comment_data)

  except:
    pass

  return comment_ids

cloud_client= MongoClient('mongodb+srv://tamizhinitvmm:tamizhinitvm@cluster0.tlkctoe.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db=cloud_client['Youtube_Data']
mycoll=db['channel_details']

def channel_information(channel_id):
  channel_Details=channel_info(channel_id)
  videoid_Details=videoid_info(channel_id)
  video_Details=video_info(videoid_Details)
  comment_Details=comment_info(videoid_Details)

  mycoll=db['channel_details']
  existing_channel=mycoll.find_one({'channel_info.channel_id': channel_id})

  if existing_channel:
    mycoll.update_one({'channel_info.channel_id': channel_id},{'$set':{
                'channel_info': channel_Details,
                'video_info': video_Details,
                'comment_info': comment_Details}})
    
  else:
    mycoll.insert_one({'channel_info':channel_Details,
                        'video_info':video_Details,
                        'comment_info':comment_Details})

def channels_table(ch_name_s):
    connection = pymysql.connect(
    host = 'localhost',
    user = 'root',
    password = 'Tamizhini@04',
    port = 3306,
    autocommit = True
    )
    mycursor = connection.cursor()

    #mycursor.execute('create database if not exists Youtube_Data')
    mycursor.execute('use Youtube_Data')

    query = '''CREATE TABLE IF NOT EXISTS Channels (
                channel_name VARCHAR(100),
                channel_id VARCHAR(80) PRIMARY KEY,
                description TEXT,
                joined VARCHAR(50),
                sub_count BIGINT,
                video_count INT,
                view_count BIGINT,
                playlist_id VARCHAR(80))'''

    mycursor.execute(query)

    db=cloud_client['Youtube_Data']
    global mycoll
    mycoll=db['channel_details']

    ch_detail=[]
    for ch_data in mycoll.find({'channel_info.channel_name': ch_name_s},{'_id':0}):
        ch_detail.append(ch_data['channel_info'])
    df1=pd.DataFrame(ch_detail)

    for index,row in df1.iterrows():
        query2='''INSERT INTO channels(channel_name,
                                        channel_id,
                                        description,
                                        joined,
                                        sub_count,
                                        video_count,
                                        view_count,
                                        playlist_id)

                                        values(%s,%s,%s,%s,%s,%s,%s,%s)
                                        
                  ON DUPLICATE KEY UPDATE
                                        channel_name=values(channel_name),
                                        description=values(description),
                                        joined=values(joined),
                                        sub_count=values(sub_count),
                                        video_count=values(video_count),
                                        view_count=values(view_count)'''
        
        values = (row['channel_name'],
                row['channel_id'],
                row['description'],
                row['joined'],
                row['sub_count'],
                row['video_count'],
                row['view_count'],
                row['playlist_id'])
        
        mycursor.execute(query2,values)
        
def videos_table(ch_name_s):
    connection = pymysql.connect(
    host = 'localhost',
    user = 'root',
    password = 'Tamizhini@04',
    port = 3306,
    autocommit = True
    )
    mycursor = connection.cursor()

    mycursor.execute('create database if not exists Youtube_Data')
    mycursor.execute('use Youtube_Data')

    query = '''CREATE TABLE IF NOT EXISTS Videos (
            channel_id VARCHAR(100),
            videoid VARCHAR(100) PRIMARY KEY,
            video_name TEXT ,
            description TEXT,
            publised VARCHAR(30),
            tags TEXT,
            thumbnails VARCHAR(200),
            views BIGINT,
            likes BIGINT,
            comments BIGINT,
            favorite VARCHAR(80),
            duration VARCHAR(50),
            caption VARCHAR(30))'''
    

    mycursor.execute(query)

    db=cloud_client['Youtube_Data']

    vi_details=[]
    for ch_data in mycoll.find({'channel_info.channel_name':ch_name_s},{'_id':0}):
        vi_details.append(ch_data['video_info'])

    df2 = pd.DataFrame(vi_details[0])

    try:
        for index,row in df2.iterrows():
                tags_str = row['tags'][0] if row['tags'] else ''

                query3= '''INSERT INTO Videos (
                                        channel_id,
                                        videoid,
                                        video_name,
                                        description,
                                        publised,
                                        tags,
                                        thumbnails,
                                        views,
                                        likes,
                                        comments,
                                        favorite,
                                        duration,
                                        caption)
                                        
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                
                           ON DUPLICATE KEY UPDATE
                                        video_name = VALUES(video_name),
                                        description = VALUES(description),
                                        publised = VALUES(publised),
                                        tags = VALUES(tags),
                                        thumbnails = VALUES(thumbnails),
                                        views = VALUES(views),
                                        likes = VALUES(likes),
                                        comments = VALUES(comments),
                                        favorite = VALUES(favorite),
                                        duration = VALUES(duration),
                                        caption = VALUES(caption)'''

                values1 = (row['channel_id'],
                            row['videoid'],
                            row['video_name'],
                            row['description'],
                            row['publised'],
                            tags_str,
                            row['thumbnails'],
                            row['views'],
                            row['likes'],
                            row['comments'],
                            row['favorite'],
                            row['duration'],
                            row['caption'])
                        
                mycursor.execute(query3,values1)

    except Exception as e:
            print(e)
            raise Exception(e)

def comments_table(ch_name_s):
    connection = pymysql.connect(
    host = 'localhost',
    user = 'root',
    password = 'Tamizhini@04',
    port = 3306,
    autocommit = True
    )
    mycursor = connection.cursor()

    mycursor.execute('CREATE DATABASE IF NOT EXISTS Youtube_Data')
    mycursor.execute('USE Youtube_Data')

    query = '''CREATE TABLE IF NOT EXISTS Comments(
                                             channel_id VARCHAR(100),
                                             comment_id VARCHAR(100) PRIMARY KEY,
                                             comment_text TEXT,
                                             comment_author VARCHAR(50),
                                             comment_published VARCHAR(50)
                                             )'''

    mycursor.execute(query)

    db=cloud_client['Youtube_Data']

    cm_details=[]
    for ch_data in mycoll.find({'channel_info.channel_name':ch_name_s},{'_id':0}):
        cm_details.append(ch_data['comment_info'])

    df3=pd.DataFrame(cm_details[0])

    try:
        for index, row in df3.iterrows():
            query4 = '''INSERT INTO Comments(
                                        channel_id,
                                        comment_id,
                                        comment_text,
                                        comment_author,
                                        comment_published)
                                        
                                        values(%s,%s,%s,%s,%s)
                                        
                        ON DUPLICATE KEY UPDATE
                                        comment_text = VALUES(comment_text),
                                        comment_author = VALUES(comment_author),
                                        comment_published = VALUES(comment_published)'''

            values = (
                    row['channel_id'],
                    row['comment_id'],
                    row['comment_text'],
                    row['comment_author'],
                    row['comment_published'])
               
            mycursor.execute(query4,values)
          

    except Exception as e:
          print(e)
          raise Exception(e)

def tables(each_channel):
    channel_tab=channels_table(each_channel)
    videos_tab= videos_table(each_channel)
    comments_tab= comments_table(each_channel)

    return 'Tables created successfully'

def show_channel_table():
    ch_list=[]
    for ch_data in mycoll.find({},{'_id':0,'channel_info':1}):
        ch_list.append(ch_data['channel_info'])
        
    df1=st.dataframe(ch_list)    
    
    return df1

def show_video_table():
    video_list=[]
    for vi_data in mycoll.find({},{'video_info':1}):
        for i in vi_data['video_info']:
                video_list.append(i)

    df2=st.dataframe(video_list)

    return df2

def show_comment_table():
     comments_list=[]
     for cm_data in mycoll.find({},{'comment_info':1}):
          for i in cm_data['comment_info']:
               comments_list.append(i)

     df3=st.dataframe(comments_list)

     return df3

with st.sidebar:
    st.title(':blue[Youtube Data Harvesting]')
    st.header(':violet[Skills Gained]')
    st.write('Python scripting')
    st.write('API Integration')
    st.write('Data collection')
    st.write('Streamlit')
    st.write('Data Management using SQL')
    st.write('Visualisation')

Channel_id = st.text_input(':green[Enter your channel ID here]')
if st.button(':red[Collect and Store Data]'):
    ch_ids=[]
    db=cloud_client['Youtube_Data']
    mycoll=db['channel_details']
    for ch_data in mycoll.find({},{'_id':0,'channel_info':1}):
        ch_ids.append(ch_data['channel_info']['channel_id'])

    if Channel_id in ch_ids:
        channel_Details=channel_info(Channel_id)
        videoid_Details=videoid_info(Channel_id)
        video_Details=video_info(videoid_Details)
        comment_Details=comment_info(videoid_Details)

        mycoll=db['channel_details']

        mycoll.update_one({'channel_info.channel_id': Channel_id},{'$set':{
                'channel_info': channel_Details,
                'video_info': video_Details,
                'comment_info': comment_Details}})
        st.success('Channel details of this Channel id is updated successfully')

    else:
        insert=channel_information(Channel_id)
        st.success('Channel details of this Channel id is inserted successfully')

channel_list=[]
for ch_data in mycoll.find({},{'_id':0,'channel_info':1}):
    channel_list.append(ch_data['channel_info']['channel_name'])

unique_values = st.selectbox('Select the channel',channel_list)

if st.button(':blue[Migrate to SQL]'):
    tab=tables(unique_values)
    st.success(tab)

show_table=st.radio(':green[SELECT THE TABLE FOR VIEW]',['CHANNELS','VIDEOS','COMMENTS'])

if show_table=='CHANNELS':
    show_channel_table()

elif show_table=='VIDEOS':
    show_video_table()

elif show_table=='COMMENTS':
    show_comment_table()

sql_qns=['1.What are the names of all the videos and their corresponding channels?',
        '2.Which channels have the most number of videos, and how many videos do they have?',
        '3.What are the top 10 most viewed videos and their respective channels?',
        '4.How many comments were made on each video, and what are their corresponding video names?',
        '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7.What is the total number of views for each channel, and what are their corresponding channel names?',
        '8.What are the names of all the channels that have published videos in the year 2022?',
        '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10.Which videos have the highest number of comments, and what are their corresponding channel names?']

option=st.selectbox(':green[SELECT YOUR QUESTION]',options=sql_qns,index=None,placeholder='Choose an option')

connection = pymysql.connect(
    host = 'localhost',
    user = 'root',
    password = 'Tamizhini@04',
    port = 3306,
    autocommit = True
    )
mycursor = connection.cursor()

if option==sql_qns[0]:
    query= '''SELECT videos.video_name,channels.channel_name
        FROM Youtube_Data.channels
        JOIN Youtube_Data.videos on channels.channel_id = videos.channel_id'''

    mycursor.execute(query)

    table1=mycursor.fetchall()
    df=st.write(pd.DataFrame(table1,columns=['Video Name','Channel Name']))

elif option==sql_qns[1]:
    query='''SELECT video_count,channel_name
            FROM Youtube_data.channels 
            ORDER BY video_count DESC'''

    mycursor.execute(query)

    t2=mycursor.fetchall()
    df1=pd.DataFrame(t2,columns=['Highest Video Count','Channel Name'])
    fig= px.bar(df1, x='Channel Name', y='Highest Video Count',
                title='Channels with highest video count')
    st.plotly_chart(fig)

elif option==sql_qns[2]:
    query='''SELECT channels.channel_name, videos.video_name, videos.views
            FROM youtube_data.videos
            JOIN youtube_data.channels ON channels.channel_id = videos.channel_id 
            WHERE views IS NOT NULL
            ORDER BY views DESC LIMIT 10'''

    mycursor.execute(query)

    t3=mycursor.fetchall()
    df2=pd.DataFrame(t3,columns=['Channel Name', 'Video Name', 'Highest View Count'])
    fig=px.treemap(df2, path=['Channel Name','Video Name'],values='Highest View Count',
               color='Video Name',
               title='Top 10 most viewed videos and their respective Channels')
    fig.update_traces(textfont=dict(color="black",size=15))
    fig.update_layout(uniformtext=dict(minsize=12, mode='show'))
    st.plotly_chart(fig)

elif option==sql_qns[3]:
    query='''SELECT video_name, comments
            FROM youtube_data.videos
            WHERE comments IS NOT NULL'''

    mycursor.execute(query)

    t3=mycursor.fetchall()
    df3=st.write(pd.DataFrame(t3,columns=['Video Name','No.of.Comments']))

elif option==sql_qns[4]:
    query='''SELECT videos.video_name, videos.likes, channels.channel_name
            FROM Youtube_data.videos
            JOIN Youtube_data.channels ON channels.channel_id = videos.channel_id
            WHERE videos.likes IS NOT NULL
            ORDER BY videos.likes DESC LIMIT 15'''

    mycursor.execute(query)

    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=['Video Name','Likes Count','Channel Name'])

    fig=px.sunburst(df4, path=['Channel Name','Video Name'], values='Likes Count',
                    color='Video Name',
                    title='Videos with highest number of likes')
    fig.update_traces(textinfo="value",
                      hovertemplate='<b>%{label}</b><br>Views: %{value}<extra></extra>',
                      textfont=dict(color="black", size=18))
    st.plotly_chart(fig)

elif option==sql_qns[5]:
    query='''SELECT video_name, likes FROM youtube_data.videos'''

    mycursor.execute(query)

    t5=mycursor.fetchall()
    df5=st.write(pd.DataFrame(t5,columns=['Video Name','Like Count']))

elif option==sql_qns[6]:
    query='''SELECT channel_name,view_count FROM youtube_data.channels'''

    mycursor.execute(query)

    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=['Channel Name','View Count'])
    fig=px.bar(df6, x='Channel Name', y='View Count',
               title='Number of views for each channel')
    fig.update_traces(text=df6['View Count'],
                      textposition='outside',
                      texttemplate='%{text:.2s}')
    fig.update_layout(height=600)
    st.plotly_chart(fig)

elif option==sql_qns[7]:
    query='''SELECT videos.video_name, channels.channel_name
            FROM youtube_data.videos
            JOIN youtube_data.channels ON channels.channel_id=videos.channel_id
            WHERE SUBSTRING(publised, 1, 4) = '2022' '''

    mycursor.execute(query)

    t7=mycursor.fetchall()
    df7=st.write(pd.DataFrame(t7,columns=['Videos Published on 2022','Channel Name']))


elif option==sql_qns[8]:
    query='''SELECT channel_name,
            SEC_TO_TIME(AVG(duration_seconds))
            FROM (SELECT channels.channel_name,
            COALESCE(TIME_TO_SEC(SUBSTRING_INDEX(SUBSTRING_INDEX(videos.duration, 'T', -1), 'M', 1)) * 60 + 
            TIME_TO_SEC(SUBSTRING_INDEX(SUBSTRING_INDEX(videos.duration, 'M', -1), 'S', 1)),0) AS duration_seconds
            FROM youtube_data.channels
            JOIN youtube_data.videos ON channels.channel_id = videos.channel_id) AS derived_table
            GROUP BY channels.channel_name'''

    mycursor.execute(query)

    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=['Channel Name','Average Duration'])
    df8['Average Duration'].replace({'minute':'','minutes':'','a':1},inplace=True)
    df8['Average Duration'] = pd.to_timedelta(df8['Average Duration'])
    df8['Average Duration']=df8['Average Duration'].dt.total_seconds() / 60
    fig=px.bar(df8, x='Channel Name', y='Average Duration',
               title='Average Duration of Videos from each Channels')
    fig.update_traces(text=df8['Average Duration'],
                      textposition='outside')
    fig.update_layout(height=600,
                      xaxis_title="Channel Name",
                     yaxis_title="Average Duration (in minutes)",
                     title_x=0.3)
    st.plotly_chart(fig)

elif option==sql_qns[9]:
    query='''SELECT videos.comments,videos.video_name, channels.channel_name
            FROM youtube_data.videos
            JOIN youtube_data.channels ON channels.channel_id=videos.channel_id
            ORDER BY videos.comments DESC LIMIT 20'''

    mycursor.execute(query)

    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=['Comments','Video Name','Channel Name'])
    fig=px.treemap(df9, path=['Channel Name','Video Name'], values='Comments',
                   color='Video Name',
                   title='Videos with highest number of Commments')
    fig.update_traces(textfont=dict(color="black",size=15))
    fig.update_layout(uniformtext=dict(minsize=9, mode='show'))
    st.plotly_chart(fig)

st.snow()
st.balloons()