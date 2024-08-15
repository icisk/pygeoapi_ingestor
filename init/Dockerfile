FROM python:alpine3.19

COPY req.txt req.txt
RUN pip install -r req.txt

COPY . .

CMD ["python", "main.py"]