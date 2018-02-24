from SurbtcRequests import SurbtcRequest

if __name__ == '__main__':
    surbtc = SurbtcRequest()
    surbtc.update_bitcoin(save_path='./bitcoin.csv')
    surbtc.update_ether(save_path='./ether.csv')
