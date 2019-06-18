import json, time
from websocket import create_connection

ws = None
for i in range(3):
    try:
        ws = create_connection("wss://ws.kraken.com")
        ws.send(json.dumps({
            "event": "subscribe",
            # "event": "ping",
            # "pair": ["XBT/USD", ],
            "pair": ["ETH/USD", ],
            # "subscription": {"name": "ticker"}
            # "subscription": {"name": "spread"}
            "subscription": {"name": "trade"}
            # "subscription": {"name": "book", "depth": 10}
            # "subscription": {"name": "ohlc", "interval": 5}
        }))
    except Exception as error:
        print('Caught this error: ' + repr(error))
        time.sleep(3)
    else:
        break

if ws is not None:
    while True:
        try:
            result = ws.recv()
            result = json.loads(result)
            print("Received: %s" % result)
        except KeyboardInterrupt:
            break

        except Exception as error:
            print('Caught this error: ' + repr(error))
            time.sleep(3)

    ws.close()
