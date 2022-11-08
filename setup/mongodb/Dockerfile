FROM ubuntu:20.04

# Declare a set of arguments for the instance details of the target MongoDB database
ARG HOST
ARG DATABASE
ARG COLLECTION
# Declare a set of arguments for the credentials to access the target MongoDB database
ARG USERNAME
ARG PASSWORD

# Install the prerequisite packages
RUN apt-get update && \
    apt-get install -y gnupg sudo wget

# Install the required MongoDB client and database tools
RUN wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | sudo apt-key add -
RUN echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse" | \
    sudo tee /etc/apt/sources.list.d/mongodb-org-5.0.list
RUN sudo apt-get update && \
    sudo apt-get install -y mongodb-org

# Add the target bash script to the local environment and grant executable permission
ADD config.sh /
RUN chmod +x /config.sh

# Construct the command to execute when the container is run
CMD ["/config.sh", "--host ${HOST}", "--username ${USERNAME}", "--password ${PASSWORD}", "--database ${DATABASE}", "--collection ${COLLECTION}"]