#!/bin/bash

Help()
{
   # Display basic help information
   echo "Create and configure a MongoDB database collection."
   echo
   echo "Syntax: scriptTemplate [-o|u|p|d|c|h]"
   echo "options:"
   echo "-o | --host          Hostname for the target MongoDB database instance."
   echo "-u | --username      Username for the administrator account."
   echo "-p | --password      Password for the administrator account."
   echo "-d | --database      Name of the target database."
   echo "-c | --collection    Name of the target collection."
   echo
}

# Configure and accept a set of command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -h|--help)
            Help
            break ;;
        -o|--host)
            HOST=$2
            shift ;;
        -u|--username)
            USERNAME=$2
            shift;;
        -p|--password)
            PASSWORD=$2
            shift;;
        -d|--database)
            DATABASE=$2
            shift ;;
        -c|--collection)
            COLLECTION=$2
            shift;;
        *)
            echo "Error: Invalid option ($1)"
            break;;
    esac
    shift
done

# Execute a command to create the required database collection
mongosh "mongodb+srv://$HOST/$DATABASE" \
    --username $USERNAME \
    --password $PASSWORD \
    --eval "db.createCollection('$COLLECTION')"
# Load the source sample data into the target database collection
mongoimport "mongodb+srv://$HOST/$DATABASE" \
    --username $USERNAME \
    --password $PASSWORD \
    --collection $COLLECTION \
    --jsonArray \
    --file data/claims.json