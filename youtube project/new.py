from source import *

channel_id = st.text_input("Channel Id", "")

cnx = mysql.connector.connect(host="localhost", user="root", password="12345", database="youtube")
cursor = cnx.cursor()

if 'key' not in st.session_state:
    st.session_state.key = 0

if st.session_state.key == 0:
    fetchButton = st.button('fetch data')


    if fetchButton:
        data = {}
        video_details = []
        channel_details =channel(channel_id)
        
        details = channel_details[0]
        playlist_id = details['playlist_id']
        video_ids = get_video_ids(youtube, playlist_id)

        for video_id in video_ids:
            video_detail = get_video_details(video_id)
            comments = get_video_comments(youtube,video_id)
            video_detail['comments'] = comments
            video_details.append(video_detail)
    
        data['videos'] = video_details
        details['channel_id'] = channel_id
        data['channel_details'] = channel_details[0]
        data['id'] = channel_id
        connection_string = "mongodb://127.0.0.1:27017"
        client = MongoClient(connection_string)
        db = client['youtube']
        collection = db['channel_details']
        collection.insert_one(data)
        st.session_state.key = 1
        st.write(data)
if st.session_state.key == 1:
    migrateButton = st.button('Migrate to mysql')
    if migrateButton:
        connection_string = "mongodb://127.0.0.1:27017"
        client = MongoClient(connection_string)
        db = client['youtube']
        collection = db['channel_details']
        result = collection.find_one({'id': channel_id})
        client.close()
        ch_details = result['channel_details']  
        
    
        date = parser.parse(ch_details['published_date']  )
        query = "INSERT INTO channel_details (channel_id, channel_name, playlist_id, subscription,totel_videos,views,published_date ) VALUES (%s, %s, %s,%s,%s,%s,%s)"
        values = (ch_details['channel_id'],ch_details['channel_name'],ch_details['playlist_id'],ch_details['subscription'],ch_details['totel_videos'],ch_details['views'],date)
        cursor.execute(query,values)                                                                                                    

        for video in result['videos']:

            videoQuery = "INSERT INTO video_details (video_id,channel_id, video_name,view_count,like_count,comment_count,duration) VALUES (%s, %s, %s,%s,%s,%s,%s)"
            duration = isodate.parse_duration(video['duration'])
            milliseconds = duration.total_seconds() * 1000

            videoValues = (video['video_id'],ch_details['channel_id'],video['title'],video['view_count'],video['like_count'],video['comment_count'],milliseconds)
            cursor.execute(videoQuery,videoValues)
            for comment in video['comments']:
                commentQuery = "INSERT INTO comments (video_id,text,author,likes,date) VALUES (%s, %s,%s,%s,%s)"
                dateTime = datetime.strptime(comment['date'], "%Y-%m-%dT%H:%M:%SZ")
                commentValues = (video['video_id'],comment['text'],comment['author'],comment['likes'],dateTime)
                cursor.execute(commentQuery,commentValues)

        cnx.commit()
        st.success('Migrated')
        st.session_state.key = 2
if st.session_state.key == 2:   
    tab1, tab2, tab3,tab4,tab5,tab6,tab7,tab8,tab9,tab10 = st.tabs(["Get most liked video", "Get channel_name and video_name", "Get channel_name and video_name and view_count","Get channel_name and totel videos","Get video_name and like_count","Get video_name and comment_count","Get channel_name and views","Get channel_name and avg(duration)","Get channel_name and published year 2022","Get sum(like_count) video_details"])
    with tab1:
            query ="select video_name, like_count, channel_name from video_details join channel_details on video_details.channel_id=channel_details.channel_id order by like_count desc"

            results = execute_query(query)
            for row in results:
                st.write(row)

    with tab2:
            query = "SELECT video_name, channel_name FROM video_details JOIN channel_details ON video_details.channel_id = channel_details.channel_id"
            results = execute_query(query)
            for row in results:
                st.write(row)


    with tab3:
            query = "Select video_name,channel_name,view_count from video_details join channel_details on video_details.channel_id=channel_details.channel_id order by view_count desc limit 10"

            results = execute_query(query)
            for row in results:
                st.write(row)
    with tab4:
            query = 'select channel_name,totel_videos from channel_details order by totel_videos desc'
            results = execute_query(query)
            for row in results:
                st.write(row)
    with tab5:
            query = "select video_name, like_count, channel_name from video_details join channel_details on video_details.channel_id=channel_details.channel_id order by like_count desc"
            results = execute_query(query)
            for row in results:
                st.write(row)
    with tab6:
            query ="select video_name, comment_count,channel_name from video_details  join channel_details on video_details.channel_id = channel_details.channel_id order by comment_count desc" 
            results = execute_query(query)
            for row in results:
                st.write(row)

    with tab7:
            query ="select views,channel_name from channel_details order by views"
            results = execute_query(query)
            for row in results:
                st.write(row)
        
        
    with tab8:
            query = "select  channel_name,avg(duration) from video_details join channel_details  on video_details.channel_id=channel_details.channel_id  group by video_id"

            results = execute_query(query)
            for row in results:
                st.write(row)

    with tab9:
            query ="select * from channel_details where year (published_date) = 2022"
            results = execute_query(query)
            for row in results:
                st.write(row)

    with tab10:
            query ="select sum( like_count ),video_name from video_details group by video_id"

            results = execute_query(query)
            for row in results:
                st.write(row)
    

    

        
        



