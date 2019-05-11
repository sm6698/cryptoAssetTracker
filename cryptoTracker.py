import sqlite3
from pathlib import Path

from modules.cryptoTrackerEth import *
from modules.cryptoTrackerBtc import *
from modules.cryptoTrackerCoinbase import *
from modules.cryptoTrackerBinance import *

# Static Constants
# time difference in Coinbase Report
VALID_COINS = ['BTC','ETH']
DISPOSITION_COMMANDS = [ 'WALLET', 'PURCHASE', 'DONATE', 'EXPENSE']
EXCHANGE_FILETYPES = [ 'COINBASE-COIN', 'COINBASE-GDAX', 'BINANCE-DEPOSIT', 'BINANCE-TRADEHISTORY', 'BINANCE-WITHDRAWAL']

# globals variables
dataBase = "database.db"  # type: str


def import_file_list(exchange_files):
    file_list = []
    with open(exchange_files, 'r') as fin:  # `with` statement available in 2.5+
        reader = csv.reader(fin)
        for item in reader:
            # check to see if it is a comment
            if item[0].lstrip()[0] != '#':
                file_path =item[1].lstrip()
                exchange_file = Path(file_path)
                # check if file exists
                if  exchange_file.is_file():
                    if item[0].upper() in EXCHANGE_FILETYPES:
                        file_list.append([item[0], exchange_file.name , file_path])
                    else:
                        print ('Invalid filetype:', item[0])
                        exit(1)
                else:
                    print ('file not found:',item[1])
                    exit(1)
    return file_list


def create_wallet_table(cur, con, coin_tx_list, coin_from_exchange, coin_to_exchange):
    ether_exchange_suspect = []
    wallet_id_number = 0
    for tx_id in coin_from_exchange:
        if "BTC" in coin_tx_list[tx_id].type:
            btc_process_trans_id(tx_id, cur, con, coin_tx_list, coin_from_exchange, coin_to_exchange, wallet_id_number)
        elif "ETH" in coin_tx_list[tx_id].type:
            eth_process_trans_id(tx_id, cur, con, coin_tx_list, coin_from_exchange, coin_to_exchange, ether_exchange_suspect, wallet_id_number)
        wallet_id_number = wallet_id_number + 1
    #print ("suspect Exchange",etherExchangeSuspect)
    #update table to include notation to addresses that might be exchanges
    for ether_addr in ether_exchange_suspect:
        cur.execute("UPDATE wallets SET child = REPLACE(child, '%s', '%s');" % (ether_addr, "exchange_"+ether_addr))
        con.commit()

def process_user_commands(cur, con):
    command = ['']
    while command[0] != 'Q' and command[0] != 'QUIT':
        command_str = input('enter a command\n>>')
        command = command_str.upper().split(' ')
        if command[0] == 'P' or command[0] == 'PRINT':
            if (len(command) == 2) and (command[1] in VALID_COINS) :
                print_coin_table(command[1], cur, con)
            elif (len(command) == 2) and (command[1] == 'WALLETS' or command[1] == 'CRYPTO') :
                print_database(command[1].lower(), cur, con)
            else:
                print("invalid print command")
        if command[0] == 'D' or command[0] == 'DISPOSITION':
            if (len(command) == 3) and command[1] in DISPOSITION_COMMANDS:
                if command[2].replace('.','').isdigit():
                    cur.execute("SELECT id, disposition, coin, wallet, tx_id FROM wallets WHERE id='%s';" % (command[2]))
                    con.commit()
                    query = cur.fetchall()
                    wallet = query[0][2]+ '_' + query[0][3][0:5]
                    if query[0][1] == None:
                        if command[1] == 'WALLET':
                            cur.execute("UPDATE wallets SET disposition = '%s' WHERE id = '%s';" %
                                        (query[0][2]+ '_' + query[0][3][0:5], command[2]))
                            con.commit()
                            print(wallet, command[1])
                        elif command[1] == 'PURCHASE' or command[1] == 'EXPENSE':
                            # copy disposition to row with matching id
                            cur.execute("UPDATE wallets SET disposition = '%s' WHERE id = '%s';" %
                                        (command[1][:5].lower(), command[2]))
                            # copy type to row with matching id
                            cur.execute("UPDATE crypto SET type = '%s' WHERE tx_id = '%s';" %
                                        (command[1][:5].lower(),query[0][4]))
                            header = cur.execute("PRAGMA table_info(crypto)").fetchall()
                            header_string = ''
                            # remove column with wallet name from crypto table
                            for label in header:
                                if label[1] != wallet:
                                    header_string = header_string + label[1] + ','
                            # move crypto table so it can be copied
                            cur.execute("ALTER TABLE crypto RENAME to _crypto;")
                            # create new crypto table
                            cur.execute("CREATE TABLE crypto (%s);" % (header_string[:-1]))
                            # copy items over to new table
                            cur.execute("INSERT INTO crypto (%s) SELECT %s FROM _crypto;" % (header_string[:-1],
                                                                                             header_string[:-1]))
                            # delete old table
                            cur.execute(''' DROP TABLE _crypto ''')
                            con.commit()
                            print(command[1])
                        elif command[1] == 'DONATE':
                            donate_coin =query[0][2] + '_DONATE'
                            add_column_if_not_in_table(donate_coin, cur, con)
                            # copy disposition to row with matching id
                            cur.execute("UPDATE wallets SET disposition = '%s' WHERE id = '%s';" %
                                        (command[1].lower(), command[2]))
                            # copy type to row with matching id
                            cur.execute("UPDATE crypto SET type = '%s' WHERE tx_id = '%s';" %
                                        (command[1].lower(),query[0][4]))
                            # copy item from wallet to donate column
                            cur.execute("UPDATE crypto SET %s = (%s - %s) WHERE %s IS NOT NULL;" %
                                        (donate_coin,wallet,'fee',wallet))
                            header = cur.execute("PRAGMA table_info(crypto)").fetchall()
                            header_string = ''
                            # remove column with wallet name from crypto table
                            for label in header:
                                if label[1] != wallet:
                                    header_string = header_string + label[1] + ','
                            # move crypto table so it can be copied
                            cur.execute("ALTER TABLE crypto RENAME to _crypto;")
                            # create new crypto table
                            cur.execute("CREATE TABLE crypto (%s);" % (header_string[:-1]))
                            # copy items over to new table
                            cur.execute("INSERT INTO crypto (%s) SELECT %s FROM _crypto;" % (header_string[:-1],
                                                                                             header_string[:-1]))
                            # delete old table
                            cur.execute(''' DROP TABLE _crypto ''')
                            con.commit()
                        print ("line already has a disposition")
                else:
                    print ("invalid disposition item")
            else:
                print ("invalid disposition command")
        #print (command)


# start of main
def main(args):
    coin_to_exchange = []
    coin_from_exchange = []
    coin_tx_list = {}

    con = sqlite3.connect(dataBase)
    cur = con.cursor()
    file_list = []
    if len(args) == 2:
        exchange_list_file = Path(args[1])
        # check if file exists
        if exchange_list_file.is_file():
            file_list = import_file_list(exchange_list_file)
        else:
            print ('file not found:', exchange_list_file)
            exit(1)
    else:
        print ('syntax: cryptoTracker.py exchange_file_list')
        print ('where exchange_file_list is a file that includes a list of files from exchanges')


    # remove old tables before adding new tables
    try:
        cur.execute(''' DROP TABLE crypto ''')
    except:
        print ("no table to delete")
    try:
        cur.execute(''' DROP TABLE wallets ''')
    except:
        print ("no table to delete")

    # create initial tables in database
    cur = con.cursor()
    cur.execute("CREATE TABLE crypto (filename, datetime, type, tx_id, fee);")
    cur.execute( "CREATE TABLE wallets (id, disposition, filename, coin, datetime, tx_id, tx_value, wallet, wallet_value,"
                 "child, fees);")
    con.commit()

    # load up files from the exchanges
    for file_item in file_list:
        if file_item[0] == 'COINBASE-COIN':
            coinbase_data = cb_coin_to_database(file_item, cur, con)
        elif file_item[0] == 'COINBASE-GDAX':
            cb_gdax_to_database(file_item, cur, con)
        elif file_item[0] == 'BINANCE-DEPOSIT':
            binance_dep_data = bin_deposit_coin(file_item, cur, con)
        elif file_item[0] == 'BINANCE-TRADEHISTORY':
            bin_history(file_item, cur, con)
        elif file_item[0] == 'BINANCE-WITHDRAWAL':
            binance_wd_data = bin_widthdraw_coin(file_item, cur, con)
        else:
            print("error in file list")
            exit(1)
    # search blockchain data
    coin_tx_list = coinbase_data[0]
    coin_from_exchange = coinbase_data[1] + binance_wd_data[1]
    coin_to_exchange = coinbase_data[2] + binance_dep_data[1]
    coin_tx_list.update(binance_dep_data[0])
    coin_tx_list.update(binance_wd_data[0])



    create_wallet_table(cur, con, coin_tx_list, coin_from_exchange, coin_to_exchange)
    #process user inputs
    process_user_commands(cur, con)
    con.close()


if __name__ == '__main__':
    main(sys.argv)