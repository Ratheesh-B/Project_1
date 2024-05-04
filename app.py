import streamlit as st
import streamlit_option_menu
import os
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import mysql.connector as mc
import pandas as pd
import re

api_service_name = 'youtube'
api_version = 'v3'
dev_key='AIzaSyAe9YHQ01aCDstjH5sYOpK9Qe_40P4e8PI'
#channel_id='UCVhopEghrYuEBPlsqC4k-Rg'
youtube = googleapiclient.discovery.build(api_service_name, api_version,developerKey=dev_key)

mydb= mc.connect(host='localhost' , user='root' , password='1234' , database = 'sample4')

mycursor=mydb.cursor()

mycursor.execute('CREATE TABLE IF NOT EXISTS Channel(channel_id VARCHAR(255) PRIMARY KEY,playlist_id VARCHAR(255),channel_name VARCHAR(255),channel_type VARCHAR(255),total_videos INT,channel_views INT,channel_description VARCHAR(255),channel_status VARCHAR(255))')

mycursor.execute('CREATE TABLE IF NOT EXISTS Video(video_id VARCHAR(255) PRIMARY KEY,channel_id VARCHAR(255),video_name VARCHAR(255),video_description TEXT,Published_date VARCHAR(255),view_count INT,like_count INT, favorite_count INT, comment_count INT , duration INT  , thumbnail VARCHAR(255) , caption_status VARCHAR(255),FOREIGN KEY(channel_id) REFERENCES Channel(channel_id) ON DELETE SET NULL)')

mycursor.execute('CREATE TABLE IF NOT EXISTS Comments(comment_id VARCHAR(255) PRIMARY KEY,video_id VARCHAR(255),comment_text TEXT , comment_author VARCHAR(255),FOREIGN KEY(video_id) REFERENCES Video(video_id) ON DELETE SET NULL )')

#Function to convert alpha numeric duration to Integer(to sec)
def str_to_int(str):
    numbers = re.findall(r'[0-9]+', str)
    num = numbers[::-1]
    second=1
    total_sec=0
    for x in num:
        total_sec+=second*(int)(x)
        second*=60
    return total_sec

#Function to get channel details using channel id
def getChannelDetails(channel_id):
   request = youtube.channels().list(
        part="contentDetails,contentOwnerDetails,id,snippet,statistics,status",id=channel_id)
   response=request.execute()
   user_id = response['items'][0]['snippet']['customUrl']
   channel_name = response['items'][0]['snippet']['title']
   channel_type = response['items'][0]['kind']
   total_videos = response['items'][0]['statistics']['videoCount']
   channel_views = response['items'][0]['statistics']['viewCount']
   channel_description = response['items'][0]['snippet']['description']
   channel_status = response['items'][0]['status']['privacyStatus']
   Playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
   subscribers_count=response['items'][0]['statistics']['subscriberCount']
   data={'channel_id':channel_id,
        'channel_name':channel_name,
        'user_id':user_id,
        'channel_type':channel_type,
        'channel_views':channel_views,
        'channel_description':channel_description,
        'channel_status':channel_status,
        'Playlist_id':Playlist_id ,
         'subscribers_count':subscribers_count}
   sql = 'INSERT INTO Channel(channel_id,playlist_id,channel_name,channel_type,total_videos,channel_views,channel_description,channel_status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
   val = (channel_id,Playlist_id,channel_name,channel_type,total_videos,channel_views,channel_description,channel_status)
   mycursor.execute(sql,val)
   mydb.commit()
   return data

#Function to get channel videos using playlist id taken from getChannelDetails() function.
def getChannelVideos(playlist_id,videos):
    next_page = None

    while True:
        response = youtube.playlistItems().list(playlistId=playlist_id,part='snippet',
                                           pageToken=next_page).execute()

        for i in range(len(response['items'])):
            videos.append(response['items'][i]['snippet']['resourceId']['videoId'])
        next_page = response.get('nextPageToken')

        if next_page is None:
            break
    return videos

#Function to get video details using video Ids taken from getChannelVideos() function
def getVideoDetails(v_ids,Playlistid):
    video_stats = []

    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            duration = str_to_int(video['contentDetails']['duration'])
            sql = 'INSERT INTO Video(video_id,channel_id ,video_name ,video_description ,Published_date ,view_count ,like_count , favorite_count , comment_count  , duration, thumbnail  , caption_status ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            val = (video['id'],video['snippet']['channelId'],video['snippet']['title'] ,video['snippet']['description'],video['snippet']['publishedAt'],video['statistics']['viewCount'] , video['statistics'].get('likeCount'),video['statistics']['favoriteCount'],video['statistics'].get('commentCount'),duration,video['snippet']['thumbnails']['default']['url'],video['contentDetails']['caption'])
            mycursor.execute(sql,val)
            mydb.commit()
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
    return video_stats

#Function to get comment details using video Ids taken from getChannelVideos() function
def getCommentsDetails(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:

                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
                sql = 'INSERT INTO Comments(comment_id ,video_id , comment_text , comment_author ) VALUES (%s,%s , %s ,%s )'
                val = (data['Comment_id'],data['Video_id'] ,data['Comment_text'],data['Comment_author'])
                mycursor.execute(sql,val)
                mydb.commit()
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data


def option1():
   mycursor.execute("select c.channel_name ,v.video_name  from Channel c, Video v where c.channel_id = v.channel_id order by c.channel_name;")
   df1 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
   st.write(df1)

def option2():
   mycursor.execute("select channel_name,total_videos from Channel order by total_videos desc limit 1;")
   df2 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
   st.write(df2)

def option3():
   mycursor.execute("select c.channel_name ,v.video_name, v.view_count  from Channel c, Video v where c.channel_id = v.channel_id  order by view_count desc limit 10;")
   df3 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
   st.write(df3)

def option4():
   mycursor.execute("select v.video_name,count(c.comment_id) as comment_count from Comments c, Video v where c.video_id=v.video_id group by v.video_name;")
   df4 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
   st.write(df4)

def option5():
   mycursor.execute("select c.channel_name , v.video_name , v.like_count from Video v, Channel c where c.channel_id = v.channel_id order by like_count desc;")
   df5 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
   st.write(df5)

def option6():
   mycursor.execute("select video_name,like_count from Video;")
   df6 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
   st.write(df6)

def option7():
   mycursor.execute("select c.channel_name , SUM(v.view_count) from Video v , Channel c where c.channel_id = v.channel_id   group by c.channel_name;")
   df7 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
   st.write(df7)

def option8():
   mycursor.execute("select c.channel_name , count(v.video_id) AS Number_of_videos from Video v , Channel c where c.channel_id = v.channel_id and Published_date like '2022%' group by c.channel_name;")
   df8 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
   st.write(df8)

def option9():
   mycursor.execute("select c.channel_name , avg(v.duration) AS Average_duration  from Video v , Channel c where c.channel_id = v.channel_id   group by c.channel_name;")
   df9 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
   st.write(df9)

def option10():
   mycursor.execute("select  c.channel_name , v.comment_count from Video v, Channel c where c.channel_id = v.channel_id order by v.comment_count desc;")
   df10 = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
   st.write(df10)

def getDetails(channel_name):
   sql = "select channel_id, channel_name, total_videos , channel_views , channel_description  from Channel where channel_name = %s;"
   val = (channel_name,)
   mycursor.execute(sql,val)
   channel = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
   st.write(channel)

st.set_page_config(page_title= 'Youtube Data Harvesting and Warehousing',
                   layout= 'wide',initial_sidebar_state= 'expanded')

with st.sidebar:
    opt_selected = streamlit_option_menu.option_menu('', ['Home','SQL','Previously Searched'],
                           icons=['house','database','card-text'])


if opt_selected=='Home':
   channel_id = st.text_input('YouTube Data Harvesting and Warehousing using SQL and Streamlit',placeholder='search channel')
   on_click=st.button('search')
   vid=[]
   if on_click==True:
      data=getChannelDetails(channel_id)
      playlist_id= data['Playlist_id']
      st.write('Channel Name :',data['channel_name'])
      st.write('Subscribers : ',data['subscribers_count'])
      st.write('Views :',data['channel_views'])
      st.write('description :',data['channel_description'])
      st.write('Channel ID :',data['channel_id'] )
      videos=getChannelVideos(data['Playlist_id'],vid)
      video_details = getVideoDetails(videos,playlist_id)
      com_d = []
      for video in videos:
         com_d = getCommentsDetails(video)

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

if opt_selected=='SQL':

  option = st.selectbox(
     'Select one option',
     ('','What are the names of all the videos and their corresponding channels?',
         'Which channels have the most number of videos, and how many videos do they have?',
         'What are the top 10 most viewed videos and their respective channels?',
         'How many comments were made on each video, and what are their corresponding video names?',
         'Which videos have the highest number of likes, and what are their corresponding channel names?',
         'What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
         'What is the total number of views for each channel, and what are their corresponding channel names?',
         'What are the names of all the channels that have published videos in the year 2022?',
         'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
         'Which videos have the highest number of comments, and what are their corresponding channel names?'))

  if option=='What are the names of all the videos and their corresponding channels?':
     option1()

  elif option=='Which channels have the most number of videos, and how many videos do they have?':
     option2()

  elif option=='What are the top 10 most viewed videos and their respective channels?':
     option3()

  elif option=='How many comments were made on each video, and what are their corresponding video names?':
     option4()

  elif option=='Which videos have the highest number of likes, and what are their corresponding channel names?':
     option5()

  elif option=='What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
     option6()

  elif option=='What is the total number of views for each channel, and what are their corresponding channel names?':
     option7()

  elif option=='What are the names of all the channels that have published videos in the year 2022?':
     option8()

  elif option=='What is the average duration of all videos in each channel, and what are their corresponding channel names?':
     option9()

  elif option=='Which videos have the highest number of comments, and what are their corresponding channel names?':
     option10()

#------------------------------------------------------------------------------------------------------------------------------------------------------------------#

if opt_selected == 'Previously Searched':
   mycursor.execute('select distinct channel_name from Channel')
   result = mycursor.fetchall()
   l=[]
   l.append('')
   for res in range(0,len(result)):
      l.append(result[res][0])
   channel_name = st.selectbox('Select one option',l)
   if(channel_name !=''):
       getDetails(channel_name)
