import io
import re
import multiprocessing
import time
from ftplib import FTP

import boto3

# region S3 info
BUCKET = "abb-integracao"
PATH_BUCKET = 'QA2/'
# endregion

# region FTP Info
HOST = "200.189.167.44"
USER = "transfer"
PASSWD = "TR123456@!"
PATH_FTP = 'QA/'
# endregion

# Hold the name of files to upload
files_keys = {}


# region Decorator measure timing
def timeit(method):
    def timed(*args, **kwargs):
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()

        print('%r took %2.2f sec' % (method.__name__, te - ts))
        return result

    return timed


# endregion

def upload(fname, file):
    """
    Upload file to S3 if has args
    :param file: string => File
    :param fname: string => Filename
    """
    try:
        if len(fname) > 0:
            print("Publishing %s" % (PATH_BUCKET + fname + '.csv'))

            session = boto3.Session(profile_name='ecs')
            s3 = session.resource('s3')
            s3.Object(BUCKET, PATH_BUCKET + fname + '.csv').put(Body=file)
    except Exception as err:
        # Print error and raise it to stop loop for upload
        print(err)
        raise Exception(err)


def empty_bucket():
    """
    Empty selected Bucket
    """
    try:
        session = boto3.Session(profile_name='ecs')
        s3 = session.resource('s3')
        s3.Bucket(BUCKET).objects.all().delete()
    except Exception as err:
        # Print error and raise it to stop process
        print(err)
        raise Exception(err)


def get_prefix(filename):
    """
    Get the prefix from the given filename
    :param filename: string
    :return: match group
    """
    return re.search(r'(?P<prefix>[^0-9]+)_[0-9]+\.[^0-9]+', filename)


def prepare_file(filename, file_download):
    # Download binary data and send do S3
    print("Downloading %s..." % filename)

    bytes_io = io.BytesIO()

    with FTP(host=HOST, user=USER, passwd=PASSWD) as ftp:
        # with file as file:
        ftp.retrbinary("RETR " + PATH_FTP + file_download, bytes_io.write)

        bytes_io.seek(0)

        # assume bytes_io is a `BytesIO` object
        byte_str = bytes_io.read()

        # Convert to a "unicode" object
        text_obj = byte_str.decode('UTF-8')

        upload(filename, text_obj)


@timeit
def def_handler():
    # print("Starting: %s" % multiprocessing.current_process().name)

    """
    Get files from FTP and upload to S3
    """
    try:
        # Empty target Bucket
        empty_bucket()

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

        for i in files_keys:
            process = multiprocessing.Process(target=prepare_file, args=(i, files_keys[i]))
            # process.daemon = True
            process.start()
            # process.join()

    except Exception as err:
        print(err)


if __name__ == "__main__":
    def_handler()
