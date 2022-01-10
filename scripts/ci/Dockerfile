FROM cimg/python:3.9

# Copy this first so it's the only file that will trigger rebuild of the pip dependencies layer
COPY scripts/ci/requirements.txt scripts/ci/requirements.txt

RUN pip install --no-cache-dir -U -r scripts/ci/requirements.txt

COPY . .

RUN pip install -U .[tests]
