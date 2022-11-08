# Pull the required image
FROM public.ecr.aws/lambda/python:3.9

# Upgrade the installed version of pip
RUN python3.9 -m pip install --upgrade pip

# Copy the Python dependency specification file to the working directory
COPY requirements.txt ./
# Copy the custom application modules to the image
COPY modules ${LAMBDA_TASK_ROOT}/modules
# Copy the Python application script to the image
COPY app.py ${LAMBDA_TASK_ROOT}

# Install the specified dependencies
RUN python3.9 -m pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set the command to the lamnda handler
CMD [ "app.lambda_handler" ]