FROM python:3

WORKDIR /Python

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./graphql-requests.py" ]