# Video events management task

This repository contians a short interview coding task.

You are asked to implement backend APIs of a video event management system supporting basic operations of:
1. Ingestion of new video events.
2. Aggregation of recorded video events.
3. Alert triggered on unusual activity.

The events are stored in postgres database the backend interacts with. In our case backend are simple python functions.

## Setup
1. Install [docker](https://docs.docker.com/engine/install/) and [docker-compose](https://docs.docker.com/compose/install/)
2. Run the code using `./run.sh`. This script starts the database and runs implemented APIs.

## Notes
Testing was performed in a local poetry environment with the following dev dependencies:
- black = "^23.11.0"
- flake8 = "^6.1.0"
- mypy = "^1.7.1"
- testing-postgresql = "^1.3.0"
