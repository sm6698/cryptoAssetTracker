from decimal import *

COINBASE_DELTA_TIME_HR = 7
SATOSHI = 100000000

class coinObject:
    def __init__(self, qty, fn, type, addr):
        self.qty = qty
        self.fn = fn
        self.type = type
        self.addr = addr

def print_database(table,cur,con):
    header_width = []
    # get all the headers from the table
    header = cur.execute("PRAGMA table_info(%s)" % table).fetchall()
    # set width of all columns to 0
    for i in range(len(header)):
        header_width.append(len(header[i][1]))

    # get width of each items in the row and colunmn
    dat = cur.execute("SELECT * FROM %s ORDER BY datetime" % table).fetchall()
    for row in dat:
        for column_no in range(len(row)):
            if row[column_no] != None:
                # get the maximum width of each column
                header_width[column_no] = max(len(row[column_no]),header_width[column_no])

    print("Database Table:",table)
    # print header
    for column_no in range(len(header)):
        formatted_column = '{{:^{0}}} '.format(header_width[column_no])
        print (formatted_column.format(header[column_no][1]), end = '')
    print("")

    # print data
    if table == 'wallets':
        dat = cur.execute("SELECT * FROM %s ORDER BY id" % table).fetchall()
    else:
        dat = cur.execute("SELECT * FROM %s ORDER BY datetime" % table).fetchall()
    for row in dat:
        for column_no in range(len(row)):
            if column_no > 4:
                formatted_column = '{{:>{0}}} '.format(header_width[column_no])
            else:
                formatted_column = '{{:<{0}}} '.format(header_width[column_no])
            if row[column_no] == None:
                print (formatted_column.format(""), end ='')
            else:
                print (formatted_column.format(row[column_no]), end = '')
        print ("")

def print_coin_table(coin,cur,con):
    # find trading pairs
    trade_pair_list = []
    header_width = []
    wallet_list = ""
    header = cur.execute("PRAGMA table_info(crypto)").fetchall()
    dat = cur.execute("SELECT * FROM crypto WHERE type = 'trade' ORDER BY datetime").fetchall()
    for row in dat:
        trade_pair = ''
        wallet_list = ""
        # extract all the wallets from table
        for column_no in range(4,len(row)):
            if coin in  header[column_no][1]:
                wallet_list = wallet_list + ',' + header[column_no][1]
            if row[column_no] != None:
                trade_pair = trade_pair + header[column_no][1] + "."
        trade_pair_list.append(trade_pair[:-1])
    coin_wallet_list = wallet_list[1:]
    where_string = ''
    # create where string for only wallets with coin in name
    for coin_wallet in coin_wallet_list.split(','):
        where_string = where_string + "OR " + coin_wallet + " IS NOT NULL "
    # wallets with coin name and trading pair
    for trade_pair in trade_pair_list:
        if coin in trade_pair:
            for wallet in trade_pair.split('.'):
                if coin not in wallet:
                    if wallet not in wallet_list:
                        wallet_list = wallet_list + ', ' + wallet
    dat = cur.execute("SELECT filename, datetime, type, fee %s FROM crypto WHERE %s ORDER BY datetime;" %
                      (wallet_list, where_string[3:])).fetchall()

    header = str('filename,datetime,type,fee' + wallet_list).split(',')
    for column_no in range(len(header)):
        header_width.append(len(header[column_no]))

    # get width of each items in the row and colunmn
    for row in dat:
        for column_no in range(len(row)):
            if row[column_no] != None:
                # get the maximum width of each column
                header_width[column_no] = max(len(row[column_no]),header_width[column_no])

    print("Coin Table:",coin)
    # print header
    for column_no in range(len(header)):
        formatted_column = '{{:^{0}}} '.format(header_width[column_no])
        print (formatted_column.format(header[column_no]), end = '')
    print("")

    # print data
    for row in dat:
        for column_no in range(len(row)):
            if column_no > 3:
                formatted_column = '{{:>{0}}} '.format(header_width[column_no])
            else:
                formatted_column = '{{:<{0}}} '.format(header_width[column_no])
            if row[column_no] == None:
                print (formatted_column.format(""), end ='')
            else:
                print (formatted_column.format(row[column_no]), end = '')
        print ("")


def add_column_if_not_in_table(coin,cur,con):
    header_list =[]
    header = cur.execute("PRAGMA table_info(crypto)").fetchall()
    for item in header:
        header_list.append(item[1])
    if coin not in header_list:
        cur.execute("ALTER TABLE crypto ADD COLUMN %s TEXT" % coin)
        con.commit()


