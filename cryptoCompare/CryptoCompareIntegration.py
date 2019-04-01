import logging

from cryptoCompare import CryptoCompareIntegrationConfig

logger = logging.getLogger('BudaLogger')


class CryptoCompareIntegration:
    def __init__(self, config: CryptoCompareIntegrationConfig):
        self.config = config
