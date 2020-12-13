# Define custom function directory
ARG FUNCTION_DIR="/function"

FROM python:3.8-slim-buster as build-image

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Install aws-lambda-cpp build dependencies
RUN apt-get update && \
  apt-get install -y \
  g++ \
  make \
  cmake \
  unzip \
  libcurl4-openssl-dev \
  fonts-takao-gothic \
  git


# Create function directory
RUN mkdir -p ${FUNCTION_DIR}

# Install the function's dependencies
COPY requirements.txt /
RUN pip install --target ${FUNCTION_DIR} awslambdaric
RUN pip install --target ${FUNCTION_DIR} -r /requirements.txt
WORKDIR ${FUNCTION_DIR}
RUN python -m sudachipy.command_line link -t full
RUN echo "テスト" | python -m sudachipy.command_line -m A

# Copy handler function
COPY app/* ${FUNCTION_DIR}


FROM python:3.8-slim-buster

COPY --from=build-image /usr/share/fonts/truetype/takao-gothic/TakaoGothic.ttf /usr/share/fonts/truetype/takao-gothic/TakaoGothic.ttf

RUN apt-get update -q && \
    apt-get install -yqq --no-install-recommends \
    mecab \
    mecab-ipadic-utf8 \
    mecab-utils \
    && apt-get -y clean \
    && rm -rf /var/lib/apt/lists/*

# Include global arg in this stage of the build
ARG FUNCTION_DIR
# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

# Copy in the built dependencies
COPY --from=build-image ${FUNCTION_DIR} ${FUNCTION_DIR}

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD [ "app.handler" ]