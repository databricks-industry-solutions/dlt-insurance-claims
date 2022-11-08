# Load the required libraries
import json
import base64
import logging

# Load the classes required for type hinting
from typing import Union
from boto3.session import Session

# Get a handle on the system logger
LOGGER = logging.getLogger()
# Set the threshold for logging to the desired level
LOGGER.setLevel(logging.INFO)


def get_secret(session: Session, secret_name: str) -> Union[dict, None]:
    """
    Get the contents of the encrypted secret values.

    Parameters
    ----------
    session: boto3.session.Session
        Instantiated Session object configured to create AWS service clients and resources.

    secret_name: str
        Name of the secret.

    Returns
    -------
    secret: dict
        Dictionary structure with the decrypted secret key-value pairs. Returns a value of None if the
        secret could not be retrieved or decrypted.
    """

    # Create a reference to the low-level SecretsManager service client
    client = session.client("secretsmanager")

    try:
        # Attempt to retrieve the secret values
        response = client.get_secret_value(SecretId = secret_name)

    except Exception as e:
        # Print an error message to the console logs
        LOGGER.error(f"Caught unhandled exception (error: {e})")
        # Return an empty value response
        return None

    else:
        # Check for the secret string in the retrieved response value
        if "SecretString" in response:
            # Extract the secret value from the retrieved response
            secret = response.get("SecretString")

        else:
            # Extract and decode the binary secret value
            secret = base64.b64decode(response.get("SecretBinary"))

    # Return the decrypted secret values
    return json.loads(secret)
