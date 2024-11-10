ARG PYTHON_VARIANT=3.10

FROM python:${PYTHON_VARIANT}

# Choose your desired base image

RUN apt-get update
RUN apt-get install -y openjdk-17-jdk python3-pip
RUN apt-get -y python3-pydot graphviz
RUN python3 -m pip install uv
WORKDIR pyconhk
COPY . pyconhk
RUN python3 -m uv venv & \
    echo source .venv/bin/activate >> ~/.bashrc & \
    source ~/.bashrc & \
    python3 -m uv pip install -r requirements.txt

