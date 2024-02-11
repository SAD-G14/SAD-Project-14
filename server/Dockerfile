FROM python:3.10

WORKDIR /server
COPY . .
RUN pip install -r requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:/"
EXPOSE 4000
EXPOSE 4005

CMD ["python", "main.py"]