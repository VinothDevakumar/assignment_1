# assignment_1
YouTube Data Harvesting and Warehousing using SQL and Streamlit:


Pre-requested:
  1) Python Packages:
    a) Pandas
    b) Streamlit
    c) sqlalchemy
    d) psycopg2
    f) googleapiclient
 3) Postgres Database
 4) Youtube API Developer Key

Once all Packages and Postgres Database are installed.
Step 1: Generate YoutubeAPI Key from Google Cloud project:
 1) You need a Google Account to access the Google API Console, request an API key, and register your application.
    Create a project in the Google Developers Console and obtain authorization credentials so your application can submit        API   requests.
  2) After creating your project, make sure the YouTube Data API is one of the services that your application is registered       to use:
      Go to the API Console and select the project that you just registered.
      Visit the Enabled APIs page. In the list of APIs, make sure the status is ON for the YouTube Data API v3.
  3) Once API Key is generated, copy the key to function APi_connect() on file youtube.py.
Step 2: Create DB as youtube_data and modify the user name and password under function db_connct() on file youtube.py.
Step 3: Once the above step is done run the code using the below command.
        streamlit run youtube.py


