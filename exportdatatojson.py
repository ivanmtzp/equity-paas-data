import os
import json
import xmltodict
import pprint
import multiprocessing
import copy
import time
import gzip
import zipfile
import datetime
import random

future_reference_date = datetime.datetime(2018, 6, 1)
today = datetime.datetime.now()
date_fmt = "%Y-%m-%d"

def read_dividend(date, item):
    dividends = {}
    delta = date - future_reference_date
    aleat = random.random() / 100
    for subitem in item['Msr']:
        row = subitem['@Row']
        col = subitem['@Col']
        if row not in dividends.keys():
            dividends[row] = {}
        if col == 'EX_DATE':
            bucket_date = datetime.datetime.strptime(subitem['@Val'], date_fmt) + delta
            dividends[row]['ex_date'] = bucket_date.strftime(date_fmt)
        elif subitem['@Col'] == 'PAYMENT_DATE':
            bucket_date = datetime.datetime.strptime(subitem['@Val'], date_fmt) + delta
            dividends[row]['pay_date'] = bucket_date.strftime(date_fmt)
        elif subitem['@Col'] == 'VALUE':
            dividends[row]['net_value'] = float(subitem['@Val']) + aleat
        elif subitem['@Col'] == 'RELATIVE':
            if subitem['@Val'] == 'false':
                dividends[row]['is_relative'] = False
            else:
                dividends[row]['is_relative'] = True
    divs = []
    for div in dividends.values():
        divs.append(div)
    return {'values': divs}


def read_repo(date, item):
    repos = []
    delta_computed = False
    delta = 0
    aleat = random.random() / 100
    for subitem in item['Msr']:
        repo_item = {}
        bucket_date = datetime.datetime.strptime(subitem['@Row'], date_fmt)
        if not delta_computed:
            delta_computed = True
            delta = date - bucket_date
        bucket_date = bucket_date + delta
        repo_item['date'] = bucket_date.strftime(date_fmt)
        repo_item['df'] = float(subitem['@Val']) + aleat
        repos.append(repo_item)
    return {'buckets': repos}


def read_orc_params(date, item):
    orc = {}
    delta = date - future_reference_date
    aleat = random.random() / 100.0
    for subitem in item['Msr']:
        row = subitem['@Row']
        col = subitem['@Col']
        if row not in orc.keys():
            orc[row] = {}
        if col == 'EXPIRY':
            bucket_date = datetime.datetime.strptime(subitem['@Val'], date_fmt) + delta
            orc[row]['date'] = bucket_date.strftime(date_fmt)
        elif subitem['@Col'] == 'REF_FWD':
            orc[row]['ref_fwd'] = float(subitem['@Val']) + aleat
        elif subitem['@Col'] == 'CALL_CURV':
            orc[row]['call_curv'] = float(subitem['@Val']) + aleat
        elif subitem['@Col'] == 'DOWN_CUT':
            orc[row]['down_cut'] = float(subitem['@Val']) + aleat
        elif subitem['@Col'] == 'DOWN_SMOOTH_RT':
            orc[row]['down_smooth'] = float(subitem['@Val']) + aleat
        elif subitem['@Col'] == 'PUT_CURV':
            orc[row]['put_curv'] = float(subitem['@Val']) + aleat
        elif subitem['@Col'] == 'SLOPE_REF':
            orc[row]['slope_ref'] = float(subitem['@Val']) + aleat
        elif subitem['@Col'] == 'UP_CUT':
            orc[row]['up_cut'] = float(subitem['@Val']) + aleat
        elif subitem['@Col'] == 'UP_SMOOTH_RT':
            orc[row]['up_smooth'] = float(subitem['@Val']) + aleat
        elif subitem['@Col'] == 'VOL_REF':
            orc[row]['vol_ref'] = float(subitem['@Val']) + aleat
    buckets = []
    for bucket in orc.values():
        buckets.append(bucket)
    return buckets


def read_atm_params(date, item):
    atm = {}
    delta = date - future_reference_date
    aleat = random.random() / 100.0
    for subitem in item['Msr']:
        row = subitem['@Row']
        col = subitem['@Col']
        if row not in atm.keys():
            atm[row] = {}
            bucket_date = datetime.datetime.strptime(row, date_fmt) + delta
            atm[row]['date'] = bucket_date.strftime(date_fmt)
        if col == 'STRIKE':
            atm[row]['strike'] = float(subitem['@Val']) + aleat
        elif subitem['@Col'] == 'VOL':
            atm[row]['vol'] = float(subitem['@Val']) + aleat
    buckets = []
    for bucket in atm.values():
        buckets.append(bucket)
    return buckets


def create_equity_options(date, und, fixml, fdxml):
    equity_options = {'equity_name': und, 'quotes': {}}
    aleat = random.random()
    for item in fixml['Mkt']['Pnt']:
        if item['Undly']['@PxStrctTyp'] == 'EQ_SPOT':
            equity_options['quotes']['spot'] = float(item['Msr']['@Val']) + aleat
    options = []
    num_options = random.randint(1500, 2500)
    for i in range(0, num_options):
        expiry = date + datetime.timedelta(days=i)
        aleat = random.random() / 100.0
        payoff = "CALL"
        exercise = "AMERICAN"
        if aleat > 0.5:
            payoff = "PUT"
            exercise = "EUROPEAN"

        options.append({"id": "id" + str(i), "name": "name" + str(i), "expiry": expiry.strftime(date_fmt),
                        "strike": equity_options['quotes']['spot'], "payoff": payoff, "exercise": exercise,
                        "periodicity": "anual", "market": "market" + str(i), "currency": "EUR",
                        "bid_price": 0.5 + aleat,
                        "ask_price": 0.65 + aleat, "bid_size": 10000 + aleat * 1000, "ask_size": aleat * 10000})
    equity_options['quotes']['options'] = options
    return equity_options


def create_equity_settings(date, und, fixml, fdxml):
    settings = {'equity_name': und, 'values':{}}

    weekly = False
    itm_call = False
    calibrate_div = False
    rebucket_orc = False
    aleat = random.random()
    if aleat > 0.5:
        weekly = True
    aleat = random.random()
    if aleat > 0.5:
        itm_call = True
    aleat = random.random()
    if aleat > 0.5:
        calibrate_div = True
    aleat = random.random()
    if aleat > 0.5:
        rebucket_orc = True
    settings['values'] = {'benchmark': "eurostoxx_" + und, 'weekly': weekly, 'itm_call': itm_call, 'calibrate_div': calibrate_div, 'rebucket_orc': rebucket_orc, 'extrap_method': 'INVERSE_TIME'}
    return settings


def create_equity_config(date, und, fixml, fdxml):
    config = {'name': und, 'active': True}
    return config


def create_equity(date, und, fixml, fdxml):
    equity = {'name': und, 'repo': {}, 'dividends': {}, 'vol_orc': {}}

    estimation_curve = fdxml['marketdata_equity']['@estimation_ccy_curve']
    equity['currency'] = estimation_curve[0:3]

    aleat = random.random()
    for item in fixml['Mkt']['Pnt']:
        if item['Undly']['@PxStrctTyp'] == 'EQ_SPOT':
            equity['spot'] = {'value': float(item['Msr']['@Val']) + aleat}
        if item['Undly']['@PxStrctTyp'] == 'EQ_LIQUIDITY':
            equity['liquidity'] = {'value': float(item['Msr']['@Val']) + 1000 * aleat}

    atm_buckets = []
    orc_buckets = []
    for item in fixml['Mkt']['Mtrx']:
        if item['Undly']['@PxStrctTyp'] == 'EQ_DIVIDENDS':
            equity['dividends'] = read_dividend(date, item)
        elif item['Undly']['@PxStrctTyp'] == 'EQ_ORC_PARAMS':
            orc_buckets = read_orc_params(date, item)
        elif item['Undly']['@PxStrctTyp'] == 'EQ_ATM_VOL':
            atm_buckets = read_atm_params(date, item)

    item = fixml['Mkt']['Curve']
    if item['Undly']['@PxStrctTyp'] == 'EQ_DIVIDENDS_YIELD':
        equity['repo'] = read_repo(date, item)

    equity['vol_orc'] = {'atm_buckets': atm_buckets, 'orc_buckets': orc_buckets}
    return equity


def create_curve(date, und, fixml):
    temp_curve_name = und
    curve = {'name': temp_curve_name.replace('_', ':'), 'currency': temp_curve_name[0:3]}
    item = fixml['Mkt']['Curve']
    buckets = []
    delta_computed = False
    delta = 0
    for subitem in item['Msr']:
        bucket_item = {}
        bucket_date = datetime.datetime.strptime(subitem['@Row'], date_fmt)
        if not delta_computed:
            delta_computed = True
            delta = date - bucket_date
        bucket_date = bucket_date + delta
        bucket_item['date'] = bucket_date.strftime(date_fmt)
        bucket_item['df'] = float(subitem['@Val']) + (random.random()/100.0)
        buckets.append(bucket_item)
    curve['buckets'] = {'values': buckets}
    return curve


def get_immediate_subdirectories(dir):
    return [name for name in os.listdir(dir)
            if os.path.isdir(os.path.join(dir, name))]


def generate_historical_data(args):
    ts = time.clock()
    data, data_json_output_file_prefix, date = args
    my_data = dict(data)
    my_data['date'] = date
    historical_data_json_file = '{}.{:04d}.{:02d}.{:02d}.json'.format(data_json_output_file_prefix, date["year"], date["month"], date["day"])
    with gzip.GzipFile(filename = historical_data_json_file,
                       mode = 'wb',
                       fileobj = open(historical_data_json_file + ".zip", 'wb'),
                       compresslevel = 9) as outfile:
        outfile.write(json.dumps(my_data).encode('utf-8'))
    #print(time.clock() - ts)


def working_days(start_date, end_date):
    days = []
    date = start_date
    if date.isoweekday() == 7:
        date = date + datetime.timedelta(days=1)
    while date <= end_date:
        if date.isoweekday() == 6:
            date = date + datetime.timedelta(days=1)
        else:
            days.append({'year': date.year, 'month': date.month, 'day': date.day})
        date = date + datetime.timedelta(days=1)
    return days


def process_equity(args):
    ts = time.clock()
    equity_name, equity_data_path, equity_json_out_dir, config_json_out_dir, equity_option_json_out_dir, settings_json_out_dir, historical_days = args

    equity_path = os.path.join(equity_data_path, equity_name)
    equity_fi_data_file = os.path.join(equity_path, "FI.xml")
    equity_fd_data_file = os.path.join(equity_path, "marketdata_equity_fd.xml")
    equity_json_output_file_prefix = os.path.join(equity_json_out_dir, equity_name)
    equity_json_output_file = equity_json_output_file_prefix + '.zip'
    config_json_output_file = os.path.join(config_json_out_dir, equity_name) + '.zip'
    equity_option_json_output_file_prefix = os.path.join(equity_option_json_out_dir, equity_name)
    equity_option_json_output_file = equity_option_json_output_file_prefix + '.zip'
    settings_json_output_file = os.path.join(settings_json_out_dir, equity_name) + '.zip'
    with open(equity_fi_data_file) as fixml_fd:
        with open(equity_fd_data_file) as fdxml_fd:
            fixml = xmltodict.parse(fixml_fd.read())
            fdxml = xmltodict.parse(fdxml_fd.read())
            print("Start : {}".format(equity_name))
            equity = create_equity(today, equity_name, fixml, fdxml)
            with zipfile.ZipFile(file=equity_json_output_file, mode='w', compression=zipfile.ZIP_DEFLATED) as myzip:
                with myzip.open(equity_name + ".json", mode='w') as outfile:
                    outfile.write(json.dumps(equity).encode('utf-8'))
                for date in historical_days:
                    equity = create_equity(datetime.datetime(date["year"], date["month"], date["day"]), equity_name, fixml, fdxml)
                    equity['date'] = date
                    historical_data_json_file = '{}.{:04d}.{:02d}.{:02d}.json'.format(equity_name, date["year"], date["month"], date["day"])
                    with myzip.open(historical_data_json_file, mode='w') as outfile:
                        outfile.write(json.dumps(equity).encode('utf-8'))

            equity_options = create_equity_options(today, equity_name, fixml, fdxml)
            with zipfile.ZipFile(file=equity_option_json_output_file, mode='w', compression=zipfile.ZIP_DEFLATED) as myzip:
                with myzip.open(equity_name + ".json", mode='w') as outfile:
                    outfile.write(json.dumps(equity_options).encode('utf-8'))
                for date in historical_days:
                    equity_options = create_equity_options(datetime.datetime(date["year"], date["month"], date["day"]), equity_name, fixml, fdxml)
                    equity_options['date'] = date
                    historical_data_json_file = '{}.{:04d}.{:02d}.{:02d}.json'.format(equity_name, date["year"], date["month"], date["day"])
                    with myzip.open(historical_data_json_file, mode='w') as outfile:
                        outfile.write(json.dumps(equity_options).encode('utf-8'))

            config = create_equity_config(today, equity_name, fixml, fdxml)
            with zipfile.ZipFile(file=config_json_output_file, mode='w', compression=zipfile.ZIP_DEFLATED) as myzip:
                with myzip.open(equity_name + ".json", mode='w') as outfile:
                    outfile.write(json.dumps(config).encode('utf-8'))

            settings = create_equity_settings(today, equity_name, fixml, fdxml)
            with zipfile.ZipFile(file=settings_json_output_file, mode='w', compression=zipfile.ZIP_DEFLATED) as myzip:
                with myzip.open(equity_name + ".json", mode='w') as outfile:
                    outfile.write(json.dumps(settings).encode('utf-8'))
    print("Finish: {}-{}".format(equity_name, time.clock() - ts))

def process_curve(args):
    ts = time.clock()
    curve_name, curve_data_path, curve_json_out_dir, historical_days = args
    curve_path = os.path.join(curve_data_path, curve_name)
    curve_data_file = os.path.join(curve_path, "FI.xml")
    curve_json_output_file_prefix = os.path.join(curve_json_out_dir, curve_name)
    curve_json_output_file = curve_json_output_file_prefix + '.zip'
    with open(curve_data_file) as fd:
        fixml = xmltodict.parse(fd.read())
        print("Start : {}".format(curve_name))
        date = today
        curve = create_curve(date, curve_name, fixml)
        with zipfile.ZipFile(file=curve_json_output_file, mode='w', compression=zipfile.ZIP_DEFLATED) as myzip:
            with myzip.open(curve_name + ".json", mode='w') as outfile:
                outfile.write(json.dumps(curve).encode('utf-8'))
            for date in historical_days:
                curve = create_curve(datetime.datetime(date["year"], date["month"], date["day"]), curve_name, fixml)
                curve['date'] = date
                historical_data_json_file = '{}.{:04d}.{:02d}.{:02d}.json'.format(curve_name, date["year"],
                                                                                  date["month"], date["day"])
                with myzip.open(historical_data_json_file, mode='w') as outfile:
                    outfile.write(json.dumps(curve).encode('utf-8'))
    print("Finish: {} - {}s".format(curve_name, time.clock() - ts))

if __name__ == '__main__':
    random.seed(1000)

    pool = multiprocessing.Pool(processes=(multiprocessing.cpu_count()-4))
    current_path = os.path.dirname(os.path.abspath(__file__))
    equity_data_path = os.path.join(current_path, 'data', 'equities')
    equities = get_immediate_subdirectories(equity_data_path)

    equity_json_out_dir = os.path.join(current_path, 'out', 'equities')
    config_json_out_dir = os.path.join(current_path, 'out', 'config')
    equity_option_json_out_dir = os.path.join(current_path, 'out', 'snapshots')
    settings_json_out_dir = os.path.join(current_path, 'out', 'settings')
    if not os.path.exists(equity_json_out_dir):
        os.makedirs(equity_json_out_dir)
    if not os.path.exists(config_json_out_dir):
        os.makedirs(config_json_out_dir)
    if not os.path.exists(equity_option_json_out_dir):
        os.makedirs(equity_option_json_out_dir)
    if not os.path.exists(settings_json_out_dir):
        os.makedirs(settings_json_out_dir)

    start_date = datetime.date(2017, 8, 1)
    end_date = datetime.date(2018, 8, 1)

    historical_days = working_days(start_date, end_date)

    print("Creating equity market data, equity option market data, settings and config")
    nargs = [(equity_name, equity_data_path, equity_json_out_dir, config_json_out_dir, equity_option_json_out_dir, settings_json_out_dir, historical_days) for equity_name in equities]
    pool.map(process_equity, nargs)

    curve_data_path = os.path.join(current_path, 'data', 'curves')
    curves = get_immediate_subdirectories(curve_data_path)

    curve_json_out_dir = os.path.join(current_path, 'out', 'curves')
    if not os.path.exists(curve_json_out_dir):
        os.makedirs(curve_json_out_dir)

    print("Creating curves market data")
    nargs = [(curve_name, curve_data_path, curve_json_out_dir, historical_days) for curve_name in curves]
    pool.map(process_curve, nargs)



