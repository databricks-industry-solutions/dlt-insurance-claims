# Load the required libraries
import os
import json
import random
import logging

from pymongo import MongoClient
from boto3.session import Session

# Load the required methods from custom modules
from modules.aws import get_secret

# Get a handle on the system logger
LOGGER = logging.getLogger()
# Set the threshold for logging to the desired level
LOGGER.setLevel(logging.INFO)

# Store the default configuration for all AWS service client sessions
SESSION = Session()
# Create a reference to the low-level S3 service client
S3 = SESSION.client("s3")
# Create a reference to the low-level EventBridge service client
EVENTS = SESSION.client("events")

# Get the name of the S3 bucket where the temporary claims records are stored
BUCKET_NAME = os.environ.get("BUCKET_NAME")
# Get the key to the target S3 object
KEY = os.environ.get("KEY")

# Get the names of the MongoDB Atlas database and collection
DATABASE_NAME   = os.environ.get("DATABASE_NAME")
COLLECTION_NAME = os.environ.get("COLLECTION_NAME")

# Get the name of the EventBridge rule triggering the function
RULE_NAME = os.environ.get("RULE_NAME")

# Get the name of the Secret resource containing the MongoDB Atlas account credentials
SECRET_NAME = os.environ.get("SECRET_NAME")


def lambda_handler(event: dict, context: object) -> None:
    """
    Process requests submitted to the Lambda function.

    Parameters
    ----------
    event: dict
        Input object received from the invoking source.

    context: object
        Runtime information, including methods and properties providing additional information about
        the invocation, function, and execution environment.

    Returns
    -------
    None
    """

    # Establish a connection with the target object
    obj = S3.get_object(Bucket = BUCKET_NAME, Key = KEY)
    # Read the contents of the target object into a memory
    records = json.loads(obj.get("Body").read().decode("utf-8"))

    # Check the total number of records in the source dataset
    if len(records) < 1:
        # Disable to EventBridge event rule triggering the function
        EVENTS.disable_rule(Name = RULE_NAME, EventBusName = "default")
        # Terminate the execution process
        return None

    # Get a random set of record indexes for sampling
    indexes = random.sample(range(0, len(records)), random.randint(100, 250))
    # Select a sample of records
    samples = [records[i] for i in indexes]

    # Log the size of the sample record set
    LOGGER.info(f"Processing sample of claims: {len(samples):,} records")

    # Get the credentials for accessing the MongoDB database
    credentials = get_secret(session = SESSION, secret_name = SECRET_NAME)
    # Get the database hostname
    host = credentials.get("HOST")
    # Unpack the authentication credentials
    username = credentials.get("USERNAME")
    password = credentials.get("PASSWORD")

    # Construct the complete MongoDB URI connection string
    uri = f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
    # Establish a connection with the MongoDB database
    client = MongoClient(uri, tls = True, tlsAllowInvalidCertificates = True)

    try:
        # Attempt to validate the database connection
        client.server_info()

    except Exception as e:
        # The database connection and/or authentication failed
        return None

    # Get (or create) a reference to the target database
    db = client[DATABASE_NAME]
    # Get (or create) a reference to the target collection
    collection = db[COLLECTION_NAME]

    # Iterate over the set of identified documents
    for sample in samples:
        # Insert the current document if it does not exist
        collection.update_one(filter = sample, update = {"$set": sample}, upsert = True)

    # Iterate over the set of sampled record indexes
    for i in sorted(indexes, reverse = True):
        # Delete the current record from the original set
        del records[i]

    # Overwrite the source dataset with the updated record set
    S3.put_object(Bucket = BUCKET_NAME, Key = KEY, Body = json.dumps(records).encode("utf-8"))

    # Terminate the execution process
    return None
