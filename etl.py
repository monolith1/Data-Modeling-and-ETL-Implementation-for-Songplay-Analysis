import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *


def process_song_file(cur, filepath):

    """
    Description: This function is responsible for extracting data from the song
    file, transforming it to the correct format, and loading it to both the
    song table and artist table in sparkifydb.

    Arguments:
        cur: the cursor object
        filepath: the path to the song file .json files

    Returns: None
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):

    """
    Description: This function is responsible for extracting data from the log
    file, filtering it by the 'NextSong' action, transforming it to the correct
    format (including converting timestamp to datetime), and loading it to the
    time, users, and songplays tables in sparkifydb.

    Arguments:
        cur: the cursor object
        filepath: the path to the log file .json files

    Returns: None
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [t.dt.values.tolist(), t.dt.hour.values.tolist(), t.dt.weekofyear.values.tolist(), t.dt.weekofyear.values.tolist(), t.dt.month.values.tolist(), t.dt.year.values.tolist(), t.dt.weekday.values.tolist()]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_data_corrected = np.array(time_data).T.tolist()
    time_df = pd.DataFrame(data=time_data_corrected, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    # drop duplicate entries
    user_df = user_df.drop_duplicates(subset=['userId'])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (t[index], row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):

    """
    Description: Gets all files from the provided directory with a matching extension,
    prints the number of files found, and iterates through the files while applying
    the ETL functions that are defined above.

    Arguments:
        cur: the cursor object
        conn: the database connection object
        filepath: the path to the .json files to be processed
        func: ETL user defined function to be applied to the .json files

    Returns: None
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():

    """
    Description: This function is responsible for connecting to sparkifydb and executing
    the ETL functions defined above for both log files and song files.

    Arguments: None

    Returns: None
    """

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()