FROM cimg/python:3.9

# Copy this first so it's the only file that will trigger rebuild of the pip dependencies layer
COPY .circleci/requirements.txt ci/requirements.txt
RUN echo `whoami`

# RUN sudo apt-get update \
#     && sudo apt-get install -y --no-install-recommends \
#         gcc \
#         g++ \
#         libsasl2-dev

RUN sudo apt-get install -y --no-install-recommends build-essential
RUN sudo apt-get update
RUN sudo apt-get upgrade
RUN sudo apt-get install libsasl2-dev

RUN pip install --no-cache-dir -U -r ci/requirements.txt

COPY . .

RUN pip install -U .[all,tests]
