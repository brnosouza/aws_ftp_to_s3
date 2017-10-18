import io
import json
from ftplib import FTP
import boto3

SESSION = 'ecs'

# region S3 info
BUCKET = "abb-integracao"
PATH_BUCKET = 'QA/'
# endregion

# region FTP Info
HOST = "200.189.167.44"
USER = "transfer"
PASSWD = "TR123456@!"
PATH_FTP = 'QA/'
# endregion

# Hold the name of files to upload
files_keys = {}


# region Bucket Functions
def get_s3(session=None):
    if session is not None:
        session = boto3.Session(profile_name=session)
        return session.resource('s3')
    else:
        return boto3.resource('s3')


def get_lambda(session=None):
    if session is not None:
        session = boto3.Session(profile_name=session)
        return session.client('lambda')
    else:
        return boto3.client('lambda')


def upload(fname, file):
    """
    Upload file to S3 if has args
    :param file: string => File
    :param fname: string => Filename
    """
    try:
        if len(fname) > 0:
            print("Publishing %s" % (PATH_BUCKET + fname + '/' + fname + '.csv'))

            get_s3(SESSION).Object(BUCKET, PATH_BUCKET + fname + '/' + fname + '.csv').put(Body=file)
    except Exception as err:
        # Print error and raise it to stop loop for upload
        print(err)
        raise Exception(err)


# endregion


def lambda_handler(event, context):
    """
    Get files from FTP and upload to S3
    """
    try:
        event = json.dumps(event)

        # Download binary data and send do S3
        print("Downloading %s..." % event.filename)

        bytes_io = io.BytesIO()

        with FTP(host=HOST, user=USER, passwd=PASSWD) as ftp:
            # with file as file:
            ftp.retrbinary("RETR " + PATH_FTP + event.download, bytes_io.write)

            bytes_io.seek(0)

            # assume bytes_io is a `BytesIO` object
            byte_str = bytes_io.read()

            # Convert to a "unicode" object
            text_obj = byte_str.decode('UTF-8')

            upload(event.fname, text_obj)

    except Exception as err:
        print(err)
