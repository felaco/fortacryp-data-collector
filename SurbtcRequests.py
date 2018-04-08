import os
import time
import random
from utils import create_base_config_file, read_config_file, ProyectLogger, save_config_file
from SurBTCPublic import SurBtcPublic, Markets
import logging
from dateutil import tz
from datetime import datetime


def _remove_transactions_past_timestamp(transactions, timestamp):
    for transaction in reversed(transactions):
        if transaction.timestamp <= timestamp:
            transactions.remove(transaction)
        else:
            return


class SurbtcRequest:
    def __init__(self):
        if not os.path.isfile("./config.json"):
            create_base_config_file()

        self.config = read_config_file()

        logging.setLoggerClass(ProyectLogger)
        self.logger = logging.getLogger("SurBtc")
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s %(levelname)s:%(name)s:%(message)s")

    def update_bitcoin(self, save_path):
        if self.config.last_timestamp_bitcoin is None:
            last_timestamp = 1438387200  # 1 de agosto 2015 00:00 UTC
        else:
            last_timestamp = self.config.last_timestamp_bitcoin

        surbtc = SurBtcPublic(Markets.BTC)
        transactions, new_last_ts = self._retrive_transactions(surbtc,
                                                               last_timestamp)

        _remove_transactions_past_timestamp(transactions, last_timestamp)
        self._persist_transactions(save_path, transactions)

        self.config.last_timestamp_bitcoin = new_last_ts
        save_config_file(self.config)
        self.logger.info("BTC: Recuperación de transacciones completa", priority=ProyectLogger.HIGH)

    def update_ether(self, save_path):
        if self.config.last_timestamp_ether is None:
            last_timestamp = 1499385600 # 7 de julio 2017 00:00 UTC
        else:
            last_timestamp = self.config.last_timestamp_ether

        surbtc = SurBtcPublic(Markets.ETH)
        transactions, new_last_ts = self._retrive_transactions(surbtc,
                                                               last_timestamp)

        _remove_transactions_past_timestamp(transactions, last_timestamp)
        self._persist_transactions(save_path, transactions)

        self.config.last_timestamp_ether = new_last_ts
        save_config_file(self.config)
        self.logger.info("ETH: Recuperación de transacciones completa", priority=ProyectLogger.HIGH)

    def _do_update(self, surbtc_api, last_ts, save_path):
        transactions, new_last_ts = self._retrive_transactions(surbtc_api,
                                                               last_ts)

        _remove_transactions_past_timestamp(transactions, last_ts)
        self._persist_transactions(save_path, transactions)

        # self.config.last_timestamp_bitcoin = new_last_ts
        save_config_file(self.config)
        self.logger.info("ETH: Recuperación de transacciones completa", priority=ProyectLogger.HIGH)

    def _retrive_transactions(self, surbtc_api, last_timestamp):
        ask_timestamp = time.time()
        ask_timestamp_ret = ask_timestamp
        transactions_list = []

        while ask_timestamp > last_timestamp:
            try:
                transactions = surbtc_api.get_transactions(ask_timestamp)
            except ConnectionError:
                self.logger.error("Error de conexión. Esperando 5 minutos para reintentar")
                time.sleep(60 * 5)
                continue

            for transaction in transactions:
                transactions_list.append(transaction)

            ask_timestamp = transactions.last_timestamp
            # self._write_transaction_to_disk(save_path, transactions)
            self._do_logging(transactions.last_timestamp)

            # se deja pasar un tiempo antes de realizar una nueva petición para
            # evitar saturar al servidor de peticiones y que nuestra ip
            # se agregue a la lista negra del firewall, o bien cloudflare
            # rechaze nuestra petición
            sleep_time = random.randint(13, 18)
            # sleep_time = random.randint(5,10)
            time.sleep(sleep_time)

        return transactions_list, ask_timestamp_ret

    def _persist_transactions(self, path, transactions):
        if path is None:
            return

        with open(path, mode='a') as file:
            if os.stat(path).st_size == 0:  # archivo vacio, se escriben las cabeceras csv
                file.write("Timestamp,Amount,Price,Direction\n")

            # transactions es una lista con las transacciones mas recientes al inicio y las mas antiguas al final
            for transaction in reversed(transactions):
                file.write("{},{},{},{}\n".format(transaction.timestamp,
                                                  transaction.amount,
                                                  transaction.price,
                                                  transaction.direction))

        transactions.clear()

    def _do_logging(self, timestamp):
        local_tz = tz.gettz("America/Santiago")
        dt = datetime.fromtimestamp(timestamp, tz=local_tz)
        string = dt.strftime("%d/%m/%Y %H:%M")

        self.logger.info("Recuperadas transacciones desde la fecha: " + string)
