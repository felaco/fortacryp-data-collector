import argparse
import sys

from Buda.BudaIntegration import BudaIntegration
from core.config import config


def handler(parsed_args):
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
        print('how did you got here?')  # Seriously


parser = argparse.ArgumentParser()
parser.allow_abbrev = False
# parser.usage = 'python BudaCli.py [market]'
parser.description = 'Description: recovers all transactions from Buda crypto exchange, for btc-clp,' \
                     ' eth-clp, bch-clp or ltc-clp'

parser.add_argument('market', choices=['btc', 'eth', 'ltc', 'bch'])
parser.add_argument('-c', '--config', help='Path to config file. Optional', required=False)
parser.set_defaults(func=handler)

argc = len(sys.argv)
if argc <= 1:
    parser.print_help()
else:
    args = parser.parse_args()
    args.func(args)
