from flask import Flask, render_template, jsonify, request

from pandas_wrapper import get_pandas_dataframe

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/ohlc', methods=['Get'])
def ohlc():
    resample = request.args.get('resample', default='1D')
    df = get_pandas_dataframe(1438387200, index_name='x')
    return jsonify(df.ohlc(resample).to_json(include_index=True, timestamp_index=True))


if __name__ == '__main__':
    app.run()
