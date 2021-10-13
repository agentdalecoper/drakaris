FROM python:3

RUN mkdir "app"
WORKDIR /app
COPY . .
RUN ls /app
RUN pip install -r requirements.txt

ENTRYPOINT python -u /app/DataProcessingService.py
