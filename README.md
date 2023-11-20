# Order Book Aggregator

This app uses flink window function to aggregate incoming order by its `side` and `price`, each time the one-second
tumbling
window closes it will output the aggregated result to kafka with topic `order_books` and keep the open order booking for
the next window.

## How To Run

There is a multiple way to run these apps but in all cases you will need to start the included docker compose.

### Using System Python

install the necessary dependency using:

```shell
pip install -r requirement.txt
```

Note: it is recommended to use a virtual environment such as `venv` to isolate the project.

run the app using:

```shell
python order_book_aggregator.py
```
