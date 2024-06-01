FROM python:3.12

LABEL maintainer="mmrhaqyt@gmail"

WORKDIR /todo

RUN apt-get update && apt-get install -y protobuf-compiler

COPY . /todo/


RUN pip install poetry

RUN poetry config virtualenvs.create false

RUN poetry install

CMD ["poetry", "run", "uvicorn", "db_service.main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]