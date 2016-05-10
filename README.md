
# Installation

## Client requirements
The client is built in python3.  With python3 and pip installed (possibly via venv), run from inside `client/`

`pip install -r requirements.txt`

## Server requirements
The server is written in javascript for nodejs.  With node and npm installed (npm is installed with node in most systems), run from `server/`

`npm install`

## Other requirements
The benchmark compares our server against Redis and Postgres.  Both Redis and Postgres should be installed.  Redis should be running on `localhost:6379`. Posgres should be running on localhost at the default port.  This benchmark will use a Postgres database named `kvtransactional`.

## Running the benchmarks
In the `client/` directory, run

`python benchmark.py`

Remember that you must be using Python3.