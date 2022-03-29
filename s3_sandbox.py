#!/usr/bin/env python3

"""A script to test AWS S3."""

import os
import sys
import csv

import boto3
from pendulum import Date  # , today
from peewee import (
    SqliteDatabase,
    Model,
    DateField,
    TextField,
    IPField,
    FixedCharField,
    IntegrityError,
)

AWS_ID = os.environ["AWS_ID"]
AWS_PASS = os.environ["AWS_PASS"]
# BUCKET = "work-sample-mk"
# SAMPLE = "2021/04/events.csv"
BUCKET = "jgaffiot-test"
SAMPLE = "sample.csv"
ID_SIZE = 36
START_DATE = Date(2021, 1, 1)

db = SqliteDatabase('s3-sandbox.db')


# id,timestamp,email,country,ip,uri,action,tags
# d24173d9-b062-49a7-a856-74c89dcab4a4,
#    2021-03-31,debbiegarcia@miller.info,Palau,192.66.235.59,index,visit,
#    "['Mr', 'eat', 'PM']"
class User(Model):
    """A user as defined in the sample data."""

    @classmethod
    def from_csv_line(
        cls, id, timestamp, email, country, ip, uri, action, tag_string
    ) -> "User":
        """Create a user from a  self"""
        tags = [t.strip("'") for t in tag_string[1:-1].split(', ')]
        return User(
            id=id,
            timestamp=timestamp,
            email=email,
            country=country,
            ip=ip,
            uri=uri,
            action=action,
            tag1=tags[0],
            tag2=tags[1],
            tag3=tags[2],
        )

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
        database = db


def main() -> int:
    """The main function of the script."""
    db.connect()
    if not db.table_exists("user"):
        db.create_tables([User])
    s3 = boto3.client("s3", aws_access_key_id=AWS_ID, aws_secret_access_key=AWS_PASS)

    # date = START_DATE
    # while date <= today().date():
    # treat_data_at(s3, date)
    # date = date.add(months=+1)
    # s3.download_file(BUCKET, SAMPLE, "sample.csv")
    for obj in s3.list_objects_v2(
        Bucket=BUCKET,
        # Prefix=f"{date.year}/{date.month}"
    )['Contents']:
        body = s3.get_object(Bucket=BUCKET, Key=obj['Key'])['Body']
        body.readline()  # discard the title line

        for line in body.iter_lines():
            parsed_line = next(csv.reader([line.decode()]))
            try:
                User.from_csv_line(*parsed_line).save(force_insert=True)
            except IntegrityError:
                pass

    return 0


if __name__ == '__main__':
    sys.exit(main())
