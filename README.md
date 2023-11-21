# Order Book Aggregator

Generate booking information from order stream every 1-second interval.

## Design parameters

1. the pipeline should output at 1-second interval.
2. each order is group into booking by its `side` and `price`.
3. each time the order is closed it should be deducted from the booking.
4. every output should contain the cumulative sum of each `side`. 
5. the output should be a list booking that is published on another kafka topic (in this case `order_bookings`).

## How To Run

To run this app you will need to install the dependency on `requirement.txt` and start the included docker compose.

### Using System Python
run the app using:

```shell
python order_book_aggregator.py
```

## Limitations

1. this app does not have checkpointing, so each time its restarted it will start from the earliest data.
2. this app still uses host memory to store state.
3. this app uses `created_at` of the event as the event-time.
