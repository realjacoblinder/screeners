import yahoo_fin.stock_info as yf_s
import yahoo_fin.options as yf_o
import math
import pandas as pd
from datetime import datetime
import dateutil.parser as dp
import shutil
import os
import time


def recent_trades_only(df, today):
    df['tmp'] = df['Last Trade Date'].apply(lambda x: (dp.parse(x) - today).days)
    df = df[df['tmp'] <= 2]
    return df.drop(columns = 'tmp').reset_index(drop=True)


start_time = time.time() 

# Import the ticker symbols
tickas_df = pd.read_csv('https://www.cboe.com/us/options/symboldir/equity_index_options/?download=csv')
tickas_list_initial = tickas_df[' Stock Symbol'].to_list()
tickas = [ticka for ticka in tickas_list_initial if len(ticka) < 6]

# set up files and folders
if os.path.isfile(r'options_errors.log'):
    os.remove(r'options_errors.log')

if os.path.isdir(r'repo'):
    shutil.rmtree(r'./repo')
os.mkdir(r'repo')

#for testing:
#tickas = ['gme']#tickas[13:27]

'''Conditions
Solve for any options where the strike is less than spot and the return is >= 50%
'''

required_return = 0.30
spot_modifier = 0.67
spot_upper_limit = 10

for ticka in tickas:
    print(ticka)
    today = datetime.today()
    master_df = pd.DataFrame()
    try:
        spot_price = yf_s.get_live_price(ticka)
        
        if spot_price > spot_upper_limit:
            with open('options_errors.log', 'a') as f:
                timestamp = datetime.now()
                f.write(f'{timestamp} - {ticka} spot was too high at {spot_price}\n')
            continue
        
        for exp_date in yf_o.get_expiration_dates(ticka):
            try:
                data = yf_o.get_puts(ticka, exp_date).replace('-', 0)
                data = data.drop(['Change', '% Change', 'Implied Volatility'], 1)
                # check strikes before doing work
                data['Strike'] = data['Strike'].astype(float)
                data = data[data['Strike'] <= spot_price*spot_modifier]
                if data.empty:
                    with open('options_errors.log', 'a') as f:
                        f.write(f'{timestamp} - {ticka}, was empty after strike vs spot comparison for {exp_date}\n')
                    continue
                
                
                data['Bid'] = data['Bid'].astype(float)
                data['Return'] = data['Bid']/data['Strike']
                data['Expiry'] = exp_date
                data['Days'] = (dp.parse(exp_date) - today).days
                data['Compounded Weekly'] = (data['Strike']*(1+data['Return'])/(data['Strike']))**(1/(data['Days']/7))
                data['Compounded Weekly'] = data['Compounded Weekly'] - 1
                # A = P(1 + x)^((date_exp - today)/7)

                # retain only where strike AND return conditioins met                
                cleaned_df = data[data['Return'] >= required_return]
                
                cleaned_df = cleaned_df.reset_index(drop=True)
                cleaned_df = recent_trades_only(cleaned_df, today)
                master_df = master_df.append(cleaned_df, ignore_index=True)
            except:
                timestamp = datetime.now()
                with open('options_errors.log', 'a') as f:
                    f.write(f'{timestamp} - Failed at options check for {ticka}, expiration {exp_date}\n')
                continue
    except:
        timestamp = datetime.now()
        with open('options_errors.log', 'a') as f:
            f.write(f'{timestamp} - Failed at spot price or expiry list for {ticka}\n')
        continue
    
    # made it through the try/excepts unscathed
    if not master_df.empty:
        master_df = master_df.sort_values(by='Expiry', ascending=False)
        master_df.to_csv(f'repo/{ticka}_infinite tendies_incoming.csv', index=False)
        timestamp = datetime.now()
        with open('options_errors.log', 'a') as f:
            timestamp = datetime.now()
            f.write(f'{timestamp} - {ticka} has those juicy infinite tenders\n')        
     
    else:
        timestamp = datetime.now()
        with open('options_errors.log', 'a') as f:
            f.write(f'{timestamp} - {ticka} tendies are finite\n')
end_time = time.time()
with open('running_time.txt', 'w') as f:
	running_length = end_time - start_time
	f.write(f'Ran for {running_length} seconds')
