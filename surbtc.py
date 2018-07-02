from SurbtcRequests import SurbtcRequest


def update_crypto():
    surbtc = SurbtcRequest()
    surbtc.update_bitcoin(save_path='./dataset/bitcoin.csv')
    surbtc.update_ether(save_path='./dataset/ether.csv')


if __name__ == '__main__':
    update_crypto()
