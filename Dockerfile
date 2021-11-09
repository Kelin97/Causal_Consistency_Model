FROM python:3
COPY . /app
WORKDIR /app
RUN pip install flask
RUN pip3 install requests
CMD python -u source.py