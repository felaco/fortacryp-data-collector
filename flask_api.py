from flask import Flask, Response, request
from apscheduler.schedulers.background import BackgroundScheduler

from pandas_wrapper import get_pandas_dataframe
import datetime

from surbtc import update_crypto

app = Flask(__name__)

@app.route('/ohlc', methods=['Get'])
def ohlc():
    resample = request.args.get('resample', default='1D')
    df = get_pandas_dataframe(1438387200, index_name='x')
    json = df.ohlc(resample).to_json(include_index=True, timestamp_index=True)

    resp = Response(json, status=200, content_type="application/json")
    return resp


if __name__ == '__main__':
    scheduler = BackgroundScheduler(daemon=True)

    scheduler.add_job(func=update_crypto, trigger='interval',
                      # next_run_time=datetime.datetime.now(),
                      hours=3)

    scheduler.start()
    app.run()
