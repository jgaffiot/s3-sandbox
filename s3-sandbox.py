#!/usr/bin/env python3

"""A script to test AWS S3."""

import os
import sys
import csv

import boto3
from pendulum import Date, today
from peewee import SqliteDatabase, Model, DateField, TextField, IPField, FixedCharField

AWS_ID = os.environ["AWS_ID"]
AWS_PASS = os.environ["AWS_PASS"]
BUCKET = "work-sample-mk"
SAMPLE = "2021/04/events.csv"
ID_SIZE = 36
START_DATE = Date(2021, 1, 1)

DB = SqliteDatabase('s3-sandbox.db')


# id,timestamp,email,country,ip,uri,action,tags
# d24173d9-b062-49a7-a856-74c89dcab4a4,
#    2021-03-31,debbiegarcia@miller.info,Palau,192.66.235.59,index,visit,
#    "['Mr', 'eat', 'PM']"
class User(Model):
    """A user as defined in the sample data."""

    id = FixedCharField(primary_key=True, null=False, max_length=ID_SIZE)
    timestamp = DateField()
    email = TextField()
    country = TextField()
    ip = IPField()
    uri = TextField()
    action = TextField()
    # tags
    tag1 = TextField()
    tag2 = TextField()
    tag3 = TextField()

    class Meta:
        database = DB  # This model uses the "people.db" database.


def main() -> int:
    """The main function of the script."""
    s3 = boto3.client("s3", aws_access_key_id=AWS_ID, aws_secret_access_key=AWS_PASS)

    date = START_DATE
    while date <= today().date():
        treat_data_at(s3, date)
        date = date.add(months=+1)

    return 0


def treat_data_at(s3, date: Date) -> None:
    """Treat the CSV data in the directory corresponding to the given date."""
    # s3.download_file(BUCKET, SAMPLE, "sample.csv")

    for obj in s3.list_objects(Bucket=BUCKET, Prefix=f"{date.year}/{date.month}"):
        for line in csv.reader(obj['Contents']['Key']):
            User(*line).save()


if __name__ == '__main__':
    sys.exit(main())
