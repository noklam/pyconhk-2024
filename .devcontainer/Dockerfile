ARG PYTHON_VARIANT=3.10

FROM python:${PYTHON_VARIANT}

# Choose your desired base image

RUN apt-get update & \
    apt-get install -y openjdk-8-jdk python3-pip
# RUN python3 -m pip install uv & \
#     python3 -m uv venv & \
#     echo source .venv/bin/activate >> ~/.bashrc & \
#     source ~/.bashrc & \
#     python3 -m uv pip install -r requirements.txt

