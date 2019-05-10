import csv
import datetime
import sys
from decimal import *
from datetime import timedelta
from blockcypher import get_transaction_details
from datetime import datetime
from cryptoTrackerUtil import *


def cb_coin_to_database(fn,cur,con):
    # Process coinbase Coin csv file
    coin_tx_list = {}
    coin_to_exchange = []
    coin_from_exchange = []
    with open(fn[2], 'r') as fin:  # `with` statement available in 2.5+
        reader = csv.reader(fin)
        for row in reader:
            if (len(row) > 2) and (row[0].find("-") != -1):
                # first columnn includes a date
                qty = row[2]
                dollars = 0 if (row[7] == "") else -1 * Decimal(row[7])
                coin = row[3]
                tx_id = row[21]
                coin_addr = row[4].replace("0x","")
                # print tx_id, dollars
                pac_date_time = row[0].split(" ")
                # adjust for UTC time
                date_time = datetime.strptime(pac_date_time[0]+pac_date_time[1], "%Y-%m-%d%H:%M:%S") + \
                            timedelta(hours=COINBASE_DELTA_TIME_HR)
                if dollars != 0:
                    # print ("usd:", str(row))
                    add_column_if_not_in_table("USD_C", cur, con)
                    name = coin + "_C"
                    add_column_if_not_in_table(name,cur,con)
                    cur.execute("INSERT INTO crypto (filename, datetime, type, %s, USD_C) VALUES (?, ?, ?, ?, ?);" % name,
                                (fn[1], date_time, 'trade', str(qty), str(dollars)))
                    con.commit()
                elif (dollars == 0) and (tx_id != ""):
                    #print ("transfer:", str(row))
                    # name =coin+"_"+tx_id
                    name =  coin.upper() + "_C"
                    add_column_if_not_in_table(name, cur, con)
                    if "BTC" in name:
                        #coinQty[tx_id] = Decimal(qty) * SATOSHI
                        coin_tx_list[tx_id] = coinObject(Decimal(qty) * SATOSHI, fn[1], name, coin_addr)
                    else:
                        #coinQty[tx_id] = qty
                        coin_tx_list[tx_id] = coinObject(Decimal(qty), fn[1], name, coin_addr)
                    if Decimal(qty) < 0:
                        coin_from_exchange.append(tx_id)
                        # add here need to query
                    else:
                        if "BTC" in name:
                            print (tx_id)
                            transaction = get_transaction_details(tx_id)
                            if 'addresses' in transaction['inputs'][0].keys():
                                coin_addr = transaction['inputs'][0]['addresses'][0]
                            else:
                                coin_addr = 'unparsed'
                            print (coin_addr)
                        coin_to_exchange.append(tx_id)
                    wallet = coin.upper() + '_' + coin_addr[0:5]
                    add_column_if_not_in_table(wallet, cur, con)
                    #print (str(qty), ":", tx_id)
                    cur.execute("INSERT INTO crypto (filename, datetime, type, %s, %s, tx_id) VALUES (?, ?, ?, ?, ?,?);"
                                % (name, wallet), (fn[1], date_time, 'xfer', qty, str(Decimal(qty)*-1), tx_id))
                    con.commit()
                    #coinFn[tx_id] = fn[1]
                    #coinType[tx_id] = name
                    #coinAddr[tx_id] = coin_addr

    return [coin_tx_list, coin_from_exchange, coin_to_exchange]

def cb_gdax_to_database(fn, cur, con):
    # define all the keys associated with the meta data (single value)
    # open up the database and insert a line
    #getcontext().prec = 12
    trade = ['match', 'fee']
    trade_dict = {}
    previous_date_time = ''
    previous_qty = Decimal(0.0)
    with open(fn[2], 'r') as fin:  # `with` statement available in 2.5+
        reader = csv.reader(fin)
        for row in reader:
            if (len(row) > 4) and (row[0].find("-") != -1):
                coin = row[1]
                transaction = row[2]
                qty = Decimal(row[4])
                #print (row)
                #print (row[0])
                date_time = row[0].replace('T',' ').split('+')[0]
                #print (date_time)
                if previous_date_time == date_time:
                    # matched with another item
                    if transaction in trade:
                        if trade_dict == {}:
                            if previous_coin == coin:
                                trade_dict[coin] = (Decimal(qty) + Decimal(previous_qty))
                            else:
                                trade_dict[coin] = Decimal(qty)
                                trade_dict[previous_coin] = Decimal(previous_qty)
                        else:
                            if coin in trade_dict.keys():
                                trade_dict[coin] = (Decimal(qty) + Decimal(trade_dict[coin]))
                            else:
                                trade_dict[coin] = Decimal(qty)
                else:
                    if trade_dict != {}:
                        for coin_p in trade_dict:
                            if trade_dict[coin_p] > 0:
                                buy_coin = coin_p
                                add_column_if_not_in_table( buy_coin + "_C", cur, con)
                            else:
                                sell_coin = coin_p
                                add_column_if_not_in_table( sell_coin + "_C", cur, con)
                        cur.execute("INSERT INTO crypto (filename, datetime, type, %s, %s) VALUES (?, ?, ?, ?, ?);" %
                                    ( buy_coin + "_C", sell_coin + "_C"), (fn[1], previous_date_time, 'trade',
                                                                           str(Decimal(trade_dict[buy_coin])), str(Decimal(trade_dict[sell_coin]))))
                        con.commit()
                    trade_dict = {}
                previous_date_time = date_time
                previous_coin = coin
                previous_qty = Decimal(qty)


