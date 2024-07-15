import os

from util import today, fetch, dateList, unzip, writeCSV


def nse():
    zipped = True
    date = today()
    start_date = "20210319"
    end_date = None

    nse_url = "https://archives.nseindia.com/content/historical/EQUITIES/#-#year#-#/#n#month#n#/cm#-#day" \
              "#-##n#month#n##-#year#-#bhav.csv.zip "
    nse_columns = "SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE, TOTTRDQTY, TOTTRDVAL, DATE, TOTALTRADES, " \
                  "ISIN "
    if not start_date:
        dates = [date]
    else:
        if not end_date:
            end_date = today()
        dates = dateList(start_date, end_date)
    print("fetching for dates : %s", dates)
    for process_date in dates:
        if not zipped:
            print("fetching uncompressed NSE bhavcopy for %s" % process_date)
            zipped = False
        else:
            print("fetching compressed NSE bhavcopy for %s" % process_date)
            zipped = True

        nse_root = './nse'
        if not os.path.exists(nse_root):
            os.mkdir(nse_root)

        target_directory = nse_root + '/' + process_date[:4]
        if not os.path.exists(target_directory):
            os.mkdir(target_directory)

        fetch(nse_url, process_date, target_directory, zipped, nse_columns)


nse()