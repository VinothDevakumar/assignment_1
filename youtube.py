from googleapiclient.discovery import build
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
from streamlit_js_eval import streamlit_js_eval

def Api_connect():
    Api_id="AIzaSyBJUZVbB5QxUv2qm3JXFbmoXy5LFp6fqUQ"
    api_service_name="youtube"
    api_version="v3"
    
    youtube=build(api_service_name,api_version,developerKey=Api_id)
    return youtube

#get channel details

def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

#get video ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data

#get_playlist_details

def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data


#get comment information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data

#Database part
def db_connect():
    import psycopg2
    mydb= psycopg2.connect(
    host="localhost",
    port="5432",
    database="youtube_data",
    user="postgres",
    password="tr00perS")
    return mydb

def create_channels_table():
    conn=db_connect()
    cursor=conn.cursor()
    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                                    Channel_Id varchar(80) primary key,
                                                                    Subscribers bigint,
                                                                    Views bigint,
                                                                    Total_Videos int,
                                                                    Channel_Description text,
                                                                    Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        conn.commit()
    except:
        print("Error:channels table already exists")
    
def create_playlist_table():
    conn=db_connect()
    cursor=conn.cursor()

    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int
                                                        )'''

    cursor.execute(create_query)
    conn.commit()

    def create_video_table():
        conn=db_connect()
        cursor=conn.cursor()

        create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(30) primary key,
                                                        Title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(10),
                                                        Caption_Status varchar(50)
                                                            )'''

        cursor.execute(create_query)
        conn.commit()

def create_comment_table():
    conn=db_connect()
    cursor=conn.cursor()
    
    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                        )'''

    cursor.execute(create_query)
    conn.commit()

def insert_channels_table(channel_info):
    df=pd.DataFrame(channel_info, index=[0])
    conn=db_connect()
    cursor=conn.cursor()
    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscribers,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)

                                                    values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                        row['Channel_Id'],
                        row['Subscribers'],
                        row['Views'],
                        row['Total_Videos'],
                        row['Channel_Description'],
                        row['Playlist_Id'])
        try:
            cursor.execute(insert_query,values)
            conn.commit()
        except:
            print("Channel value already printed")

def insert_playlist_details(play_list):
        conn=db_connect()
        cursor=conn.cursor()
        pd_playlist=pd.DataFrame(play_list)
        for index,row in pd_playlist.iterrows():
                insert_query='''insert into playlists(Playlist_Id,
                                                        Title,
                                                        Channel_Id,
                                                        Channel_Name,
                                                        PublishedAt,
                                                        Video_Count
                                                        )
                                                        
                                                        values(%s,%s,%s,%s,%s,%s)'''
                        
                values=(row['Playlist_Id'],
                        row['Title'],
                        row['Channel_Id'],
                        row['Channel_Name'],
                        row['PublishedAt'],
                        row['Video_Count']
                )
                cursor.execute(insert_query,values)
                conn.commit()           

def insert_comment_details(comm_details):
        conn=db_connect()
        cursor=conn.cursor()
        pd_comm_details=pd.DataFrame(comm_details)
        for index,row in pd_comm_details.iterrows():
                insert_query='''insert into comments(Comment_Id,
                                                        Video_Id,
                                                        Comment_Text,
                                                        Comment_Author,
                                                        Comment_Published
                                                )
                                                
                                                values(%s,%s,%s,%s,%s)'''
                
                
                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published']
                        )
                
                cursor.execute(insert_query,values)
                conn.commit()

def insert_video_details(videodetails):
        pd_video_detail=pd.DataFrame(videodetails)
        conn=db_connect()
        cursor=conn.cursor()

        for index,row in pd_video_detail.iterrows():
                insert_query='''insert into videos(Channel_Name,
                                                        Channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Tags,
                                                        Thumbnail,
                                                        Description,
                                                        Published_Date,
                                                        Duration,
                                                        Views,
                                                        Likes,
                                                        Comments,
                                                        Favorite_Count,
                                                        Definition,
                                                        Caption_Status
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    
                values=(row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status']
                        )         
                cursor.execute(insert_query,values)
                conn.commit()
        
def check_table(table):
    #table = "channels"
    conn=db_connect()
    cursor=conn.cursor()
    cursor.execute(f"SELECT EXISTS(SELECT relname FROM pg_class WHERE relname = \'{table}\');")

    if cursor.fetchone()[0]:
        return True
    else:
        return False

def select_engine():
    from sqlalchemy import create_engine
    engine = create_engine('postgresql+psycopg2://postgres:tr00perS@localhost/youtube_data')
    return engine

youtube=Api_connect()

def show_channel(channel_id):
    sqlal_engine=select_engine()
    query = f'''SELECT * FROM channels where channel_id=\'{channel_id}\';'''
    pd_data=pd.read_sql_query(query,sqlal_engine)
    st_data=st.dataframe(pd_data)
    return st_data

def show_playlist(channel_id):
    sqlal_engine=select_engine()
    query = f'''select * from playlists where channel_id=\'{channel_id}\';'''
    pd_data=pd.read_sql_query(query,sqlal_engine)
    st_playlist=st.dataframe(pd_data)
    return st_playlist

def show_video(channel_id):
    sqlal_engine=select_engine()
    query = f'''select * from videos where channel_id=\'{channel_id}\';'''
    pd_data=pd.read_sql_query(query,sqlal_engine)
    st_videolist=st.dataframe(pd_data)
    return st_videolist

def show_comment(channel_id):
    sqlal_engine=select_engine()
    query = f'''select * from comments where video_id in (select video_id from videos where channel_id=\'{channel_id}\');'''
    pd_data=pd.read_sql_query(query,sqlal_engine)
    st_comment_details=st.dataframe(pd_data)
    return st_comment_details

def channel_dropdown():
    data=[]
    conn=db_connect()
    cursor=conn.cursor()
    query = '''SELECT channel_id FROM channels'''
    cursor.execute(query)
    fetch=cursor.fetchall()
    for i in fetch:
        data.append(i[0])
    return data

def delete_channel_info(channel_id):
    conn=db_connect()
    #channel_id="UCJQJAI7IjbLcpsjWdSzYz0Q"
    cursor=conn.cursor()
    del_query4=f'''delete from channels where channel_id=\'{channel_id}\';'''
    del_query3=f'''delete from playlists where channel_id=\'{channel_id}\';'''
    del_query2=f'''delete from videos where channel_id=\'{channel_id}\';'''
    del_query1=f'''delete from comments where video_id in(select video_id from videos where channel_id=\'{channel_id}\');'''
    cursor.execute(del_query1)
    print("executing1")
    cursor.execute(del_query2)
    print("executing3")
    cursor.execute(del_query3)
    cursor.execute(del_query4)
    conn.commit()


with st.sidebar:
    selected=option_menu(
         menu_title="Menu",
         options = ["Home","About","Contact"],
         icons=["house-heart-fill","calendar2-heart-fill","envelope-heart-fill"],
         menu_icon="hear-eyes-fill",
         default_index=0
    )
    if selected == "Home":
         st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    if selected == "About":
        st.header("Skill Take Away")
        st.caption("Python Scripting")
        st.caption("Data Collection")
        st.caption("API Integration")
        st.caption("Data Management using Postgres Database")
    if selected == "Contact":
        st.title("Please connect @vinoth.deva")
    

show_option=st.radio("SELECT Channel VIEW",("NEW CHANNEL","EXISTING CHANNELS"))
if show_option=="NEW CHANNEL":
    channel_id=st.text_input("Enter New channel ID")
    if st.button("collect and store data"):
        conn=db_connect()
        cursor=conn.cursor()
        cursor.execute(f"SELECT channel_id FROM channels where channel_id='{channel_id}';")
        res=cursor.fetchall()
        if len(res) > 0:
            st.warning('Channel already present kindly check in existing channel drop box', icon="⚠️")
        else:
            channelinfo=get_channel_info(channel_id)
            insert_channels_table(channelinfo)
            playlistinfo=get_playlist_details(channel_id)
            insert_playlist_details(playlistinfo)
            videolist=get_videos_ids(channel_id)
            videoinfo=get_video_info(videolist)
            insert_video_details(videoinfo)
            commentsinfo=get_comment_info(videolist)
            insert_comment_details(commentsinfo)
else:
    result=[]
    result=channel_dropdown()
    selected_channel = st.selectbox("Existing Channel analysis?",options=result)
    st.write("you like",selected_channel,"channel")
    channel_id=selected_channel
    if "button1" not in st.session_state:
        st.session_state["button1"] = False
    if "button2" not in st.session_state:
        st.session_state["button2"] = False
    if st.button("Delete"):
        st.session_state["button1"] = not st.session_state["button1"]
    if st.session_state["button1"]:
        if st.button("Do you really wanna delete"):
            st.session_state["button2"] = not st.session_state["button2"]
            st.write("deleting",selected_channel,"channel")
            delete_channel_info(selected_channel)
            streamlit_js_eval(js_expressions="parent.window.location.reload()")
        elif st.button("NO"):
            streamlit_js_eval(js_expressions="parent.window.location.reload()")

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))
if not channel_id:
     st.warning('Please enter the channel id first', icon="⚠️")
else:
    if show_table=="CHANNELS":
        show_channel(channel_id)
    elif show_table=="PLAYLISTS":
        show_playlist(channel_id)
    elif show_table=="VIDEOS":
        show_video(channel_id)
    elif show_table=="COMMENTS":
        show_comment(channel_id)

question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. Videos with higest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))
sqlal_engine=select_engine()
if question=="1. All the videos and the channel name":
    query1=f'''select title as videos,channel_name as channelname from videos;'''
    pd_data=pd.read_sql_query(query1,sqlal_engine)
    st.dataframe(pd_data)
    #st.write(st_query1)
elif question=="2. channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels order by total_videos desc'''
    pd_data=pd.read_sql_query(query2,sqlal_engine)
    st.dataframe(pd_data)
elif question=="3. 10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    pd_data=pd.read_sql_query(query3,sqlal_engine)
    st.dataframe(pd_data)
elif question=="4. comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    pd_data=pd.read_sql_query(query4,sqlal_engine)
    st.dataframe(pd_data)
elif question=="5. Videos with higest likes":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    pd_data=pd.read_sql_query(query5,sqlal_engine)
    st.dataframe(pd_data)
elif question=="6. likes of all videos":
    query6='''select likes as likecount,title as videotitle from videos'''
    pd_data=pd.read_sql_query(query6,sqlal_engine)
    st.dataframe(pd_data)
elif question=="7. views of each channel":
    query7='''select channel_name as channelname ,views as totalviews from channels'''
    pd_data=pd.read_sql_query(query7,sqlal_engine)
    st.dataframe(pd_data)
elif question=="8. videos published in the year of 2022":
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    pd_data=pd.read_sql_query(query8,sqlal_engine)
    st.dataframe(pd_data)
elif question=="9. average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''   
    pd_data=pd.read_sql_query(query9,sqlal_engine)
    st.dataframe(pd_data)
elif question=="10. videos with highest number of comments":
    query10='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    pd_data=pd.read_sql_query(query10,sqlal_engine)
    st.dataframe(pd_data)    