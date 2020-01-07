import argparse
import sys
import os
import logging

from Buda.BudaIntegration import BudaIntegration
from core.BaseIntegration import IntegrationMarkets
from core.config import config
from cryptoCompare.CryptoCompareIntegration import CryptoCompareIntegration
from krakenWebSocket.KrakenIntegration import KrakenIntegration, KrakenHistoricalDataIntegration

logging.basicConfig(format='%(asctime)s:%(funcName)s:%(lineno)d - %(levelname)s: %(message)s')
logger = logging.getLogger('FortacrypLogger')
bot_id = os.getenv('FORTACRYP_BOT_ID', None)
chat_id = os.getenv('FORTACRYP_CHAT_ID', None)

if bot_id is None:
    logger.warning('Environment variable FORTACRYP_BOT_ID is not set.'
                   ' Are you sure? Telegram alerts will not work')

if chat_id is None:
    logger.warning('Environment variable FORTACRYP_CHAT_ID is not set.'
                   ' Are you sure? Telegram alerts will not work')


def handle_crypto_compare(parsed_args):
    config_dict = config
    crypto_compare = CryptoCompareIntegration(config_dict.crypto_compare)

    if parsed_args.market == 'btc':
        crypto_compare.recover_btc()
    elif parsed_args.market == 'ltc':
        crypto_compare.recover_ltc()
    elif parsed_args.market == 'eth':
        crypto_compare.recover_eth()
    elif parsed_args.market == 'bhc':
        crypto_compare.recover_bch()
    else:
        print('how did you got here?')  # Seriously


def handle_buda(parsed_args):
    config_dict = config
    buda = BudaIntegration(config_dict.buda)

    if parsed_args.market == 'btc':
        buda.recover_btc()
    elif parsed_args.market == 'ltc':
        buda.recover_ltc()
    elif parsed_args.market == 'eth':
        buda.recover_eth()
    elif parsed_args.market == 'bhc':
        buda.recover_bch()
    else:
        print('how did you got here?')


def handle_kraken_websocket(parsed_args):
    # kraken = KrakenIntegration(config_dict.crypto_compare)
    # kraken.subscribe()
    kraken = KrakenHistoricalDataIntegration()
    kraken.recover(IntegrationMarkets.BTC)


def config_crypto_compare_parser(subparser: argparse.ArgumentParser):
    subparser.allow_abbrev = False
    subparser.set_defaults(func=handle_crypto_compare)
    subparser.usage = 'python %(prog)s cryptoCompare {btc, eth, ltc, eth}'
    subparser.description = 'Recovers data from cryptocompare.com and stores it in a csv file'
    subparser.add_argument('market', choices=['btc', 'eth', 'ltc', 'bch'])


def config_buda_exchange_parser(subparser: argparse.ArgumentParser):
    subparser.allow_abbrev = False
    subparser.set_defaults(func=handle_buda)
    subparser.usage = 'python %(prog)s buda {btc, eth, ltc, eth}'
    subparser.description = 'Recover data from Buda.com, transforms it into ohlc and stores it in a csv file'
    subparser.add_argument('market', choices=['btc', 'eth', 'ltc', 'bch'])


def config_kraken_socket_parser(subparser: argparse.ArgumentParser):
    subparser.allow_abbrev = False
    subparser.set_defaults(func=handle_kraken_websocket)
    subparser.usage = 'pyton %(prog)s kraken'
    subparser.description = 'Subscribe to kraken websocket and launches the alert system'


parser = argparse.ArgumentParser()
parser.usage = 'python %(prog)s <command> [market]'

parser.allow_abbrev = False
# parser.usage = 'python BudaCli.py [market]'
parser.description = 'Description: recovers all transactions from Buda crypto exchange or cryptoCompare.com'

# parser.add_argument('-c', '--config', help='Path to config file. Optional', required=False)

subparsers = parser.add_subparsers(title='Commands', metavar='')

crypto_compare_parser = subparsers.add_parser(
    'cryptoCompare',
    help='Recover historical data from cryptocompare.com'
)
config_crypto_compare_parser(crypto_compare_parser)

buda_parser = subparsers.add_parser('buda', help='Recover historical data from buda exchange')
config_buda_exchange_parser(buda_parser)

kraken_parser = subparsers.add_parser('kraken', help='Subscribe to kraken websocket')
config_kraken_socket_parser(kraken_parser)

argc = len(sys.argv)
if argc <= 1:
    parser.print_help()
else:
    args = parser.parse_args()
    args.func(args)
