import csv, zipfile, os
import requests
from datetime import datetime, timedelta
from io import TextIOWrapper, StringIO
import pandas as pd

months = {
    '01': {'name': 'JAN', 'days': 31},
    '02': {'name': 'FEB', 'days': 29},
    '03': {'name': 'MAR', 'days': 31},
    '04': {'name': 'APR', 'days': 30},
    '05': {'name': 'MAY', 'days': 31},
    '06': {'name': 'JUN', 'days': 30},
    '07': {'name': 'JUL', 'days': 31},
    '08': {'name': 'AUG', 'days': 31},
    '09': {'name': 'SEP', 'days': 30},
    '10': {'name': 'OCT', 'days': 31},
    '11': {'name': 'NOV', 'days': 30},
    '12': {'name': 'DEC', 'days': 31},
}


def fetch(url_template, date, target_dir, zipped, columns):
    print("\nurl_template = {0}\ndate = {1}\ntarget_dir = {2}\nzipped = {3}\ncolumns = {4}".format(url_template, date,
                                                                                                   target_dir, zipped,
                                                                                                   columns))
    url = url_from_template(url_template, date)
    if (zipped):
        target_zip = target_dir + '/' + date + '.zip'
        if not os.path.exists(target_dir + '/csv'):
            os.mkdir(target_dir + '/csv')
        target_csv = target_dir + '/csv/' + date + '.csv'
    else:
        if not os.path.exists(target_dir + '/csv'):
            os.mkdir(target_dir + '/csv')
        target_zip = target_dir + '/csv' + date + '.csv'
        target_csv = target_zip

    print("fetching from %s into %s " % (url, target_zip))
    # get request

    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 '
                      'Safari/537.36'}
    try:
        response = requests.get(url, headers=header, timeout=5)
        response.raise_for_status()

        with open(target_zip, "wb") as file:
            file.write(response.content)
            file.close()
            if zipped:
                data = unzip(target_zip)
                if columns.find('DATE') == -1:
                    writeCSV(data, target_csv, columns, addDate=True, date=date)
                else:
                    writeCSV(data, target_csv, columns)

    except requests.exceptions.HTTPError as errh:
        # print (errh.response.text)
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("Oops: Something Else", err)


def unzip(source):
    print("unzipping %s" % (source))
    with zipfile.ZipFile(source, "r") as zip_ref:
        for file in zip_ref.namelist():
            print("Extracting %s from zip" % file)
            data = []
            with zip_ref.open(file) as myfile:
                reader = csv.reader(TextIOWrapper(myfile, 'utf-8'))
                for row in reader:
                    data.append(row)
            return data


def modifyFilePath(path):
    dir_name, base_filename = os.path.split(path)

    res = base_filename.split(".")

    base_filename = res[0] + "intermediate"
    path1 = os.path.join(dir_name, base_filename + "." + res[1])

    directory = os.path.join(dir_name, "output")
    if not os.path.exists(directory):
        os.makedirs(directory)

    base_filename = res[0] + "final"
    path2 = os.path.join(directory, base_filename + "." + res[1])
    return path1, path2


def finalFileWrite(path1, path2):
    with open(path1, 'r') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        fields = next(csvreader)
        f = open(path2, 'w')

        # create the csv writer
        writer = csv.writer(f)
        writer.writerow(fields)
        # extracting each data row one by one
        for row in csvreader:
            try:
                if 4.0 < float(row[15]) < 5.0:
                    writer.writerow(row)
                # rows.append(row)

            except ValueError:
                pass
        f.close()
        csvfile.close()

    if os.path.exists(path1):
        os.remove(path1)


def addColumns(target):
    print("Inside addColumn(): target = ", target)
    try:
        """ updated lines"""
        data2 = pd.read_csv(target, index_col=False)

        column1 = []
        column2 = []
        column3 = []

        high = data2["HIGH"]
        series = data2["SERIES"]
        open = data2["OPEN"]
        ltp = data2["LAST"]

        for x in high:
            x = x / 100
            column1.append(x)

        for a, b, c, d in zip(open, ltp, series, high):
            if (c == "EQ") and (a == d):
                c = a - b
                column2.append(c)
            else:
                column2.append("")

        for a, b in zip(column1, column2):
            if b == "":
                column3.append("")
            else:
                c = b / a
                column3.append(c)

        data2["HIGH/100"] = column1
        data2["OPEN-LTP"] = column2
        data2["OPEN-LTP/HIGH/100"] = column3

        data2 = data2.iloc[:, ~data2.columns.str.contains('^Unnamed')]
        # print(data2.head())

        path1, path2 = modifyFilePath(target)
        data2.to_csv(path1, index=False)
        finalFileWrite(path1, path2)

    except Exception as e:
        print(e)


def writeCSV(data, target, header, addDate=False, date=None):
    linecount = 0
    if addDate:
        header = 'DATE,' + header
    f = StringIO(header)
    reader = csv.reader(f, delimiter=',')
    for row in reader:
        print('Setting headers', row)
        header_row = row
    with open(target, "w") as csv_file:
        csv_writer = csv.writer(
            csv_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator='\n'
        )
        for row in data:
            # print(row)
            if linecount == 0:
                print('Incoming headers', row)
                csv_writer.writerow(header_row)
                linecount += 1
            else:
                if addDate:
                    row.insert(0, date[6:] + '/' + date[4:6] + '/' + date[:4])
                csv_writer.writerow(row)
                linecount += 1
        csv_file.close()

    addColumns(target)


def filterAndWriteCSV(source, filters, headers):
    df = pd.read_csv(source, sep=",")
    # print(df)
    filtered = df.query(filters)
    filtered.to_csv(source, sep=',', index=False)


def today():
    return datetime.today().strftime('%Y%m%d')


def url_from_template(template, date):
    template = template.replace('#-#day#-#', date[6:])
    template = template.replace('#-#month#-#', date[4:6])
    template = template.replace('#n#month#n#', months[date[4:6]]['name'])
    template = template.replace('#-#year#-#', date[:4])
    template = template.replace('#-#YY#-#', date[2:4])
    return template


def dateList(start, end):
    start = datetime.strptime(start, "%Y%m%d")
    end = datetime.strptime(end, "%Y%m%d")
    date_generated = [start + timedelta(days=x) for x in range(0, (end - start).days + 1)]
    dates = [x.strftime('%Y%m%d') for x in date_generated]

    return dates
