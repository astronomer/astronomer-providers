FROM cimg/python:3.9

# Copy this first so it's the only file that will trigger rebuild of the pip dependencies layer
COPY .circleci/requirements.txt ci/requirements.txt
RUN echo `whoami`
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libsasl2-dev


USER circleci
RUN pip install --no-cache-dir -U -r ci/requirements.txt

COPY . .

RUN pip install -U .[all,tests]
