import os
from abc import ABC, abstractmethod

import requests
import logging

logger = logging.getLogger('FortacrypLogger')


class KrakenBaseAlerts(ABC):
    def __init__(self):
        self.logger = logger
        self.should_send = True

    @abstractmethod
    def send_error_alert(self, message: str) -> None:
        pass

    @abstractmethod
    def send_triggered_alert(self, message: str) -> None:
        pass


class KrakenTelegramAlerts(KrakenBaseAlerts):
    def __init__(self):
        super().__init__()
        self.bot_id = os.getenv('FORTACRYP_BOT_ID', None)
        self.chat_id = os.getenv('FORTACRYP_CHAT_ID', None)
        self.should_send = self.bot_id is not None and self.chat_id is not None
        self.url = 'https://api.telegram.org/{}/sendMessage'.format(self.bot_id)
        self.requests = requests

    def send_error_alert(self, message: str) -> None:
        if not self.should_send:
            return

        body = {
            'chat_id': self.chat_id,
            'text': 'Error: {}'.format(message)
        }
        r = self.requests.post(self.url, data=body)
        self._on_response(r)

    def send_triggered_alert(self, message: str) -> None:
        pass

    def _on_response(self, response) -> None:
        if response.status_code != 200:
            self.logger.info('Telegram alert sended successfully')
        else:
            self.logger.warning('Error sending Telegram Alert: {}'.format(response.text))
