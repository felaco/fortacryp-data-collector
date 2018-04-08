from flask import Flask, render_template, jsonify, request

from pandas_wrapper import get_pandas_dataframe

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/ohlc', methods=['Get'])
def ohlc():
    since_ts = request.args.get('since', default=1438387200)
    df = get_pandas_dataframe(since_ts, index_name='x')
    return jsonify(df.ohlc('1D').to_json(include_index=True, timestamp_index=True))


if __name__ == '__main__':
    app.run()
