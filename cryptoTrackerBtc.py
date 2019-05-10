from decimal import *
from blockcypher import get_transaction_details
from blockcypher import get_address_overview

SATOSHI = 100000000

def btc_process_trans_id(tx_id, cur, con, coin_tx_list, coin_from_exchange, coin_to_exchange, wallet_id_number):
    wallet_id_number = wallet_id_number + 1
    getcontext().prec = 10
    transaction = get_transaction_details(tx_id)
    #print (transaction)
    for address in transaction["outputs"]:
        #print (int(transaction["fees"]))
        #print (address["value"])
        #print (Decimal(coinQty[tx_id]) * -1 - int(transaction["fees"]))
        if ((address["value"]) >= (Decimal(coin_tx_list[tx_id].qty) * -1 - int(transaction["fees"]))) and \
                ((address["value"]) <= (Decimal(coin_tx_list[tx_id].qty) * -1)):
            date_time = str(transaction['received']).split('.')[0]
            #date_time = str(datetime(transaction['received'])).split('.')[0]
            if "spent_by" in address.keys():
                child = address["spent_by"]
            else:
                child = "wallet"
            cur.execute("INSERT INTO wallets ( id, disposition, filename, coin, datetime, tx_id, tx_value, wallet, " +
                        "wallet_value, child, fees) VALUES (?,?,?,?,?,?,?,?,?,?,?);",
                        (str(wallet_id_number), str(coin_tx_list[tx_id].type), str(coin_tx_list[tx_id].fn), 'BTC',  str(date_time), tx_id,
                         str(Decimal(address["value"])/SATOSHI), str(address['addresses'][0]),
                         str(Decimal(get_address_overview(address["addresses"][0])["final_balance"])/SATOSHI),
                         str(child), str(Decimal(transaction["fees"])/SATOSHI)))
            con.commit()
            # date = datetime.date(time_trans)
            # print datetime.time(time_trans)
            #print ("date of transaction:", str(date_time))
            #print ("found it at address:", str(address['addresses'][0]))
            #print ("Inital Value of address:", str(address["value"]))
            #print ("output transaction:", str(child))
            #print ("address final Balance:", str(get_address_overview(address["addresses"][0])
            #                                     ["final_balance"]))
            #                if tx_id == '8615b09eab97769d7e5da6aafac57655bcf1c868e5a75e09a91accabb38a6acd':
            btc_wallet_recurs(child, address['addresses'][0], address["value"], str(wallet_id_number) + '.1', cur, con,
                              coin_tx_list, coin_from_exchange, coin_to_exchange)

def btc_wallet_recurs(tx_id, wallet, value, id_number, cur, con, coin_tx_list, coin_from_exchange, coin_to_exchange):
    child_tx_id = []
    child_wallet = []
    child_value = []
    fees = "0"
    tx_id_total = value
    wallet_trans = wallet
    date_time = ""
    disposition = ''
    if tx_id != "wallet":
        #print ("tx_id:", str(tx_id))
        #print ("wallet:", str(wallet))
        #print ("value:", str(value))
        transaction = get_transaction_details(tx_id)
        tx_id_total = value
        date_time = str(transaction['received']).split('.')[0]
        fees = transaction["fees"]
        wallet_trans = transaction['inputs'][0]["addresses"][0]
        #print (len(transaction['inputs']))
        #print (transaction['inputs'][0]["addresses"][0])
        #print (len(transaction['outputs']))
        if len(transaction['inputs']) > 2:
            # note the tx_id as a possible exchange
            child_tx_id.append("exchange")
        elif len(transaction["outputs"]) > 2:
            # note the tx_id as a possible exchange
            child_tx_id.append("exchange")
        # elif tx_id in coinToExchange :
        #    child_tx_id.append("exchange")
        else:
            # add tx_id to the wallet table
            for address in transaction["outputs"]:
                if "spent_by" in address.keys():
                    # Check to see if TXid and value Matches on in the Exchanges
                    if (tx_id in coin_to_exchange) and (int(coin_tx_list[tx_id].qty) == address["value"]):
                        #print (coinType[tx_id])
                        child_tx_id.append("exchange_"+ str(coin_tx_list[tx_id].type))
                        disposition = coin_tx_list[tx_id].type
                        child_wallet.append("")
                        child_value.append("")
                        cur.execute("UPDATE crypto SET fee = '%s' WHERE tx_id = '%s' ;" %
                                    (str(-1*Decimal(fees)/SATOSHI), tx_id))
                        con.commit()
                        cur.execute("UPDATE crypto SET %s = '%s' WHERE tx_id = '%s' ;" %
                                    ('BTC_'+transaction['inputs'][0]['addresses'][0][0:5],str(-1*(Decimal(fees) +
                                                                                                  Decimal(address['value']))/SATOSHI), tx_id))
                        con.commit()
                    else:
                        child_tx_id.append(address["spent_by"])
                        child_wallet.append(address['addresses'][0])
                        child_value.append(address["value"])
                elif "value" in address.keys():
                    child_tx_id.append("wallet")
                    child_wallet.append(address['addresses'][0])
                    child_value.append(value)
    if len(child_tx_id) == 1:
        child_tx_id_str = child_tx_id[0]
    else:
        child_tx_id_str = ",".join(child_tx_id)
    #print (tx_id)
    #print (Decimal(fees))
    #print (Decimal(fees))
    cur.execute("INSERT INTO wallets (id, disposition, coin, datetime, tx_id, tx_value, wallet, wallet_value, " +
                " child, fees) VALUES (?,?,?,?,?,?,?,?,?,?);",
                (str(id_number), str(disposition), 'BTC', str(date_time), tx_id, str(Decimal(tx_id_total)/SATOSHI),
                 str(wallet_trans), str(get_address_overview(wallet_trans)["final_balance"]),
                 str(child_tx_id_str), str(Decimal(fees)/SATOSHI)))
    con.commit()
    if len(child_tx_id) > 0:
        #print (len(child_tx_id))
        for i in range(len(child_tx_id)):
            if "exchange" not in child_tx_id[i]:
                btc_wallet_recurs(child_tx_id[i], child_wallet[i], child_value[i], str(id_number)+'.'+str(i), cur, con)
    # date = datetime.date(time_trans)
    # print datetime.time(time_trans)
    # print "date of transaction:" + str(date_trans) + " " + str(time_trans).split('.')[0]
    # print "found it at address:" + str(address['addresses'][0])
    # print "Inital Value of address:" + str(address["value"])
    # print "output transaction:" +  str(address["spent_by"])
    # print "address final Balance:" + str(get_address_overview(address["addresses"][0])


