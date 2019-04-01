def is_valid_market_json(market_dict: dict):
    return 'btc' in market_dict and \
           'eth' in market_dict and \
           'ltc' in market_dict and \
           'bch' in market_dict
