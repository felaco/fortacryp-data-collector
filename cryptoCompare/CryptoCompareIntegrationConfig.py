from core.configCore import BaseConfig, MarketConfig


class CryptoCompareConfig(BaseConfig):
    def __init__(self):
        super().__init__()
        self.base_url: str = 'https://min-api.cryptocompare.com/data/histohour'
        self.api_key = None

    @classmethod
    def get_instanciator(cls):
        return CryptoCompareConfig()

    @classmethod
    def get_market_config_instance(cls, **kwargs):
        return MarketConfig(**kwargs)

    def __dir__(self):
        return ['baseUrl', 'api_key', 'retrieve_from_onward', 'sleep_time_after_block',
                'sleep_time_sec', 'btc', 'eth', 'ltc', 'bch']
