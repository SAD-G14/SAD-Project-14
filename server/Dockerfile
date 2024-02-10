FROM python:3.10

WORKDIR /server
COPY . .
RUN pip install -r requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:/"
EXPOSE 4000

CMD ["python", "main.py"]