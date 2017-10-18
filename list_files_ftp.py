import json
import re
from ftplib import FTP
import boto3

SESSION = None

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
def call_lambda(fname, file_to_download):
    return get_lambda(SESSION).invoke(
        FunctionName='ftp_to_s3',
        InvocationType='Event',
        Payload=json.dumps({
            "fname": fname,
            "download": file_to_download
        })
    )


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


def empty_bucket():
    """
    Empty selected Bucket
    """

    try:
        get_s3(SESSION).Bucket(BUCKET).objects.all().delete()

    except Exception as err:
        # Print error and raise it to stop process
        print(err)
        raise Exception(err)


# endregion


def get_prefix(filename):
    """
    Get the prefix from the given filename
    :param filename: string
    :return: match group
    """
    return re.search(r'(?P<prefix>[^0-9]+)_[0-9]+\.[^0-9]+', filename)


def lambda_handler(event, context):
    """
    Get files from FTP and upload to S3
    """
    try:
        # Empty target Bucket
        empty_bucket()

        # Open connection to FTP and close it when is done
        with FTP(host=HOST, user=USER, passwd=PASSWD) as ftp:
            # Change directory inside FTP
            ftp.cwd(PATH_FTP)

            # Get files who has prefix
            for filename in ftp.nlst():
                prefix = get_prefix(filename)

                if prefix is not None:
                    prefix = prefix.group('prefix')
                    files_keys[prefix] = filename

        response = []
        for file in files_keys:
            response.append(call_lambda(file, files_keys[file]))

        return response
    except Exception as err:
        print(err)
