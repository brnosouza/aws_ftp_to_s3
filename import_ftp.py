import re
from ftplib import FTP
import boto3

PATH_FTP = 'files'

BUCKET = "teste-ecs"

# <editor-fold desc="FTP Data">
HOST = "200.189.167.44"
USER = "transfer"
PASSWD = "TR123456@!"
# </editor-fold>

files_keys = {}


def upload(*args):
    """
    Upload file to S3 if has args
    :param args: (byte<gtring>, string)
    """
    try:
        if len(args) > 0:
            session = boto3.Session(profile_name='ecs')
            s3 = session.resource('s3')
            s3.Object(BUCKET, args[1]).put(Body=args[0])
    except Exception as err:
        # Print error and raise it to stop loop for upload
        print(err)
        raise Exception(err)


def get_prefix(filename):
    """
    Get the prefix from the given filename
    :param filename: string
    :return: match group
    """
    return re.search(r'(?P<prefix>[^0-9]+)_[0-9]+\.txt', filename)


def def_handler():
    """
    Get files from FTP and upload to S3
    """
    try:
        # Open connection to FTP and close it when is done
        with FTP(host=HOST, user=USER, passwd=PASSWD) as ftp:
            # Print 'welcome message'
            print(ftp.getwelcome())

            # Change directory inside FTP
            ftp.cwd(PATH_FTP)

            # Get files who has prefix
            for filename in ftp.nlst():
                prefix = get_prefix(filename)

                if prefix is not None:
                    prefix = prefix.group('prefix')
                    files_keys[prefix] = filename

            # Download binary data and send do S3
            for filename in files_keys:
                ftp.retrbinary("RETR " + files_keys[filename],
                               lambda event, args=files_keys[filename]: upload(event, args))

    except Exception as err:
        print(err)


if __name__ == "__main__":
    def_handler()
