FROM python:3.9-slim-buster

# Do not cache Python packages
ENV PIP_NO_CACHE_DIR=yes

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# set PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/code/"

# Initialize the new working directory
WORKDIR /code

# Transferring the code and essential data
COPY xetra ./xetra
COPY Pipfile ./Pipfile
COPY Pipfile.lock ./Pipfile.lock
COPY run.py ./run.py

# Pip-Install our image and our packages according to our Pipfile
# We use '--system' so that the packages are installed in 
# the system-wide Python installation
RUN pipenv install --ignore-pipfile --system