import albums
import boto3
import os

albums = albums.Albums()
albums.df.to_csv('csvs/album_frame.csv')
albums.genres_by_years().to_csv('csvs/genre.csv')


s3 = boto3.client('s3')
files = ['csvs/album_frame.csv', 'csvs/genre.csv']
bucket = os.environ['BUCKET']

for f in files:
    s3.upload_file(f, bucket, f)
