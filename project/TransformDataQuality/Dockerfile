FROM python:latest

RUN mkdir /home/app
WORKDIR /home/app
COPY requirements.txt /home/app/requirements.txt
RUN pip install --no-cache-dir -r /home/app/requirements.txt
COPY . /home/app

CMD ["great_expectations", "checkpoint", "run", "checkpoint_transformed"]
