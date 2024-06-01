FROM python:3.12

COPY . /code/

RUN apt-get update && apt-get install -y protobuf-compiler

WORKDIR /code


RUN pip install poetry 

RUN poetry config virtualenvs.create false

RUN poetry install

CMD ["poetry", "run", "uvicorn", "todo.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]