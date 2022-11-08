# Load the required libraries
import os
import logging
import requests

from boto3.session import Session
from requests.auth import HTTPBasicAuth

# Load the required methods from custom modules
from modules.aws import get_secret

# Get a handle on the system logger
LOGGER = logging.getLogger()
# Set the threshold for logging to the desired level
LOGGER.setLevel(logging.INFO)

# Store the default configuration for all AWS service client sessions
SESSION = Session()
# Create a reference to the low-level EventBridge service client
EVENTS = SESSION.client("events")

# Get the name of the Secret resource containing the Fivetran API access credentials
SECRET_NAME = os.environ.get("SECRET_NAME")
# Get the name of the EventBridge rule triggering the function
RULE_NAME = os.environ.get("RULE_NAME")


def connector_exists(auth: HTTPBasicAuth, connector_id: str) -> bool:
    """
    Check if a specific Fivetran connector exists.

    Parameters
    ----------
    auth: HTTPBasicAuth
        Instantiated authentication object.
    connector_id: str
        Unique identifier of the Fivetran connector.

    Returns
    -------
    exists: bool
        Indicates whether the Fivetran connector exists or not.
    """

    # Construct a URL for the target API request
    url = f"https://api.fivetran.com/v1/connectors/{connector_id}"

    try:
        # Attempt to get the connector details
        response = requests.get(url = url, auth = auth, verify = False)

    except Exception as e:
        # Log the caught exception error information
        LOGGER.error(f"Caught unhandled exception for HTTP request (error: {e}): {url}")
        # Return a negative response
        return False

    # Validate the request response and return the appropriate value
    return False if response.status_code != 200 else True


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

    # Extract the set of specified Fivetran connector identifiers
    connector_ids = event.get("connector_ids", [])

    # Validate the set of Fivetran connector identifiers
    if not connector_ids:
        # Print an error message to the system logs
        LOGGER.error("Invalid or missing identifiers for Fivetran connectors")
        # Terminate the execution process
        return None

    # Get the credentials for accessing the MongoDB database
    credentials = get_secret(session = SESSION, secret_name = SECRET_NAME)
    # Extract the API key and secret values
    api_key    = credentials.get("API_KEY")
    api_secret = credentials.get("API_SECRET")
    # Instantiate an authentication object
    auth = HTTPBasicAuth(username = api_key, password = api_secret)

    # Prepare a payload to submit with the API request
    data = {
        "trust_certificates": True,
        "trust_fingerprints": True
    }

    # Initialise a list indicating the successful setup state for each of the specified connectors
    setup_states = [False] * len(connector_ids)

    # Iterate over the set of specified connector identifiers
    for i, connector_id in enumerate(connector_ids):
        # Log the current connector identifier
        LOGGER.info(f"Checking connector: {connector_id}")

        # Check if the current connector exists
        if not connector_exists(auth = auth, connector_id = connector_id):
            # Log the connector details
            LOGGER.warning(f"Connector does not exist: {connector_id}")
            # Remove the current element from the provided list of connectors
            setup_states.pop(i)
            # Skip to the next loop iteration
            continue

        # Construct a URL for the target API request
        url = f"https://api.fivetran.com/v1/connectors/{connector_id}/test"

        try:
            # Attempt to execute the API request
            response = requests.post(url = url, auth = auth, json = data, verify = False)

        except Exception as e:
            # Log the caught exception error information
            LOGGER.error(f"Caught unhandled exception for HTTP request (error: {e}): {url}")
            # Skip to the next loop iteration
            continue

        # Validate the response status code
        if response.status_code != 200:
            # Log the failed request information
            LOGGER.error(f"Failed to execute HTTP request (error: {response.content}): {url}")
            # Skip to the next loop iteration
            continue

        # Extract the response data for further processing
        response_data = response.json().get("data", {})

        # Get the connector setup state
        setup_state = response_data.get("status", {}).get("setup_state")
        # Get the status for each of the respective setup tests
        setup_tests = [setup_tests.get("status") for setup_tests in response_data.get("setup_tests")]

        # Validate the connector setup state
        if setup_state.upper() == "INCOMPLETE" or any([setup_test == "FAILED" for setup_test in setup_tests]):
            # Log the response data
            LOGGER.error(f"Connector setup test failed: {response_data}")
            # Skip to the next loop iteration
            continue

        # Update the setup state for the current connector
        setup_states[i] = True

    # Validate the setup state of all the specified Fivetran connectors
    if setup_states and all(setup_states):
        # Log the connector setup state information
        LOGGER.info("Successfully configured and tested all Fivetran connectors")

        # Disable to EventBridge event rule triggering the function
        EVENTS.disable_rule(Name = RULE_NAME, EventBusName = "default")
        # Terminate the execution process
        return None

    # Terminate the execution process
    return None
