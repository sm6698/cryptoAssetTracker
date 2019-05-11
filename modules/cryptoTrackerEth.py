from decimal import *
import requests
ETHER_WEI = 1000000000000000000

def eth_process_trans_id(tx_id, cur, con, coin_tx_list, coin_from_exchange, coin_to_exchange, ether_exchange_suspect,
                         wallet_id_number):
    past_ether_addr = []
    wallet_id_number = wallet_id_number + 1
    #print (coinAddr[tx_id])
    # API used access Ethereum transactions
    url = 'https://api.blockcypher.com/v1/eth/main/txs/0x{}'.format(tx_id)
    wallet_value = '0'
    response = requests.get(url)
    transaction = response.json()
    date_time = transaction['confirmed'].replace('T',' ').split('Z')[0]
    tx_value = str(Decimal(transaction['total'] )/ ETHER_WEI)
    fees = str(Decimal(transaction['fees'])/ ETHER_WEI)
    # API used access Ethereum address
    url = 'https://api.blockcypher.com/v1/eth/main/addrs/{}'.format(coin_tx_list[tx_id].addr)
    response = requests.get(url)
    transaction_addr = response.json()
    for txn in transaction_addr['txrefs']:
        if txn['tx_hash'] == tx_id:
            wallet_value = str(Decimal(txn["ref_balance"])/ETHER_WEI)
    #print (transaction_addr)
    #print(date_time)
    cur.execute("INSERT INTO wallets (id, disposition, filename, coin, datetime, tx_id, tx_value, wallet, wallet_value, " +
                " child, fees) VALUES (?,?,?,?,?,?,?,?,?,?,?);",
                (str(wallet_id_number), str(coin_tx_list[tx_id].type), str(coin_tx_list[tx_id].fn), 'ETH', str(date_time), tx_id, str(tx_value),
                 coin_tx_list[tx_id].addr, wallet_value, transaction["addresses"][0], str(fees)))
    con.commit()
    # follow the outgoing transactions until they reach a suspected exchange or unspent
    ether_exchange_suspect = eth_recurs_trans(tx_id, coin_tx_list[tx_id].addr, tx_value, str(wallet_id_number), cur, con,
                                              coin_tx_list, coin_from_exchange, coin_to_exchange, ether_exchange_suspect, past_ether_addr)
    return ether_exchange_suspect

def eth_recurs_trans(tx_id,eth_addr,eth_value, id_number, cur, con, coin_tx_list, coin_from_exchange, ether_exchange_suspect, coin_to_exchange, past_ether_addr):
    disposition = ''
    if eth_addr not in past_ether_addr:
        past_ether_addr.append(eth_addr)
        # add associated transactions to wallet table
        # API used access Ethereum address
        url = 'https://api.blockcypher.com/v1/eth/main/addrs/{}'.format(eth_addr)
        response = requests.get(url)
        transaction = response.json()
        # check to see that the transaction wasn't sent or received by exchange
        if len(transaction['txrefs']) < 30:
            id_number_local = 0
            # work on the oldest transaction first
            for i in range(len(transaction['txrefs'])-1,-1, -1):
                id_number_local = id_number_local  + 1
                transaction_local = transaction['txrefs'][i]
                date_time = str(transaction_local['confirmed']).replace('T',' ').split('Z')[0]
                if transaction_local['tx_input_n'] != -1 :
                    # follow transactions/coins sent out of the wallet
                    url = 'https://api.blockcypher.com/v1/eth/main/txs/{}'.format(transaction_local['tx_hash'])
                    response = requests.get(url)
                    tx_transaction = response.json()
                    # check to see if transaction goes  into one of your exchanges
                    if transaction_local["tx_hash"] in coin_to_exchange:
                        eth2addr = "exchange_"+coin_tx_list[transaction_local["tx_hash"]].type
                        disposition = "exchange_eth"
                    else:
                        eth2addr = tx_transaction['addresses'][1]
                    #print(tx_transaction)
                    cur.execute("INSERT INTO wallets (id, disposition, datetime, coin, tx_id, tx_value, wallet, "
                                "wallet_value, child, fees) VALUES (?,?,?,?,?,?,?,?,?,?);",
                                (str(id_number) + '.' + str(id_number_local), str(disposition), str(date_time),
                                 'ETH', transaction_local["tx_hash"], str(Decimal(-1*transaction_local["value"])/ETHER_WEI),
                                 str(eth_addr), str(Decimal(transaction_local["ref_balance"])/ETHER_WEI),
                                 str(eth2addr), str(Decimal(tx_transaction['fees'])/ETHER_WEI)))
                    con.commit()
                    if transaction_local["tx_hash"] not in coin_to_exchange:
                        eth_recurs_trans(transaction_local['tx_hash'],tx_transaction['addresses'][1],
                                         str(Decimal(tx_transaction['total']) / ETHER_WEI), str(id_number) + '.' +
                                         str(id_number_local), cur, con, coin_tx_list, coin_from_exchange, ether_exchange_suspect,
                                         coin_to_exchange, past_ether_addr)
                else:
                    # coins received by the wallet
                    cur.execute("INSERT INTO wallets (id, datetime, coin, tx_id, tx_value, wallet, wallet_value, " +
                                " child, fees) VALUES (?,?,?,?,?,?,?,?,?);",
                                (str(id_number) + '.' + str(id_number_local), str(date_time), 'ETH',
                                 transaction_local["tx_hash"], str(Decimal(transaction_local["value"])/ETHER_WEI),
                                 str(eth_addr), str(Decimal(transaction_local["ref_balance"])/ETHER_WEI), '', '0'))
                    con.commit()
            # print wallet balance
            #cur.execute("INSERT INTO wallets (datetime, coin, tx_id, tx_value, wallet, wallet_value, " +
            #            " child, fees) VALUES (?,?,?,?,?,?,?,?);", (str(date_time), 'ETH', 'wallet', '',
            #             str(eth_addr), str(Decimal(transaction['final_balance'])/ETHER_WEI), '', '0'))
            #con.commit()
        else:
            # Ethereum wallet is associated with an unknown exchange
            ether_exchange_suspect.append(eth_addr)
            cur.execute("INSERT INTO wallets (datetime, coin, tx_id, tx_value, wallet, wallet_value, " +
                        " child, fees) VALUES (?,?,?,?,?,?,?,?);", ('', 'ETH', '', '', str(eth_addr), '',
                                                                    'exchange', '0'))
            con.commit()
    return ether_exchange_suspect


