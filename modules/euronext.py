import requests
from requests.exceptions import ConnectionError, ReadTimeout
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
import pandas as pd
import time
from datetime import datetime
import pytz

from modules.scraper import SeleniumWrap
from modules import files as fs

class Euronext:
    def __init__(self, agg_file) -> None:
        self.market_open = True
    
        # Read aggregated trend file 
        self.agg_file = agg_file
        self.agg_df = fs.read_sheet(agg_file)
        if len(self.agg_df) == 0:
            self.agg_df = pd.DataFrame(columns=['Timestamp', 'Aggregated Trend'])
        
        self.col_map = {f'{i}m': f'{i + 1}m' for i in range(1, 720)}

    def time_now(self) -> str:
        utc_time = pytz.timezone('UTC').localize(datetime.utcnow())
        return utc_time.strftime("%d %b %Y - %H:%M %Z")        

    def get_index_composition(self, url: str) -> list[dict]:
        se = SeleniumWrap()
        driver = se.setup_driver(headless=False)
        assert driver is not None, 'Driver not found'

        items = []

        soup = se.get_page(url)
        assert soup is not None, 'Soup not found in get_index_composition'

        table = soup.select_one('table')
        assert table is not None, 'Table not found'
        trs = table.select('tr')
        trs = trs[1:]

        print('Total item in index composition: {}'.format(len(trs)))
        for tr in trs:
            a = tr.select_one('a')
            assert a is not None, 'Link not found'

            items.append({
                'Component': a.text,
                'ISIN': a['href'].split('/')[-1]  # type: ignore
            })

        del se
        return items

    def get_last_trade_price(self, ISIN: str) -> Optional[float]:
        for _ in range(3):  # Try 3 times if failed
            try:
                url = "https://live.euronext.com/en/ajax/getDetailedQuote/{}".format(
                    ISIN)

                payload = "theme_name=euronext_live"
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
                    "Accept": "*/*",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "X-Requested-With": "XMLHttpRequest",
                    "Origin": "https://live.euronext.com",
                    "Connection": "keep-alive",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                    "Pragma": "no-cache",
                    "Cache-Control": "no-cache",
                    "TE": "trailers"
                }

                response = requests.request(
                    "POST", url, data=payload, headers=headers, timeout=10)
                assert response.status_code == 200, 'Request failed'

                soup = BeautifulSoup(response.text, 'html.parser')

                price_tag = soup.select_one('#header-instrument-price')
                assert price_tag is not None, 'Price tag not found'
                
                # Check if the market is open
                status_tag = soup.select_one('#instrstatusl1')
                self.market_open = (status_tag.text.strip() == 'CONTINUOUS TRADING') if status_tag else False

                return round(float(price_tag.text.strip().replace(',', '')), 2)

            except ValueError:
                print('Value error', ISIN)
                return None
            except AssertionError as e:
                print(e, ISIN)
            except (ConnectionError, ReadTimeout):
                pass
            except Exception as err:
                print(err.__class__, ISIN)

    def shift_by_1_minute(self, df: pd.DataFrame) -> pd.DataFrame:
        # Delete the last column '720th min' if exists
        if '720m' in df.columns:
            del df['720m']

        # Shift columns by 1 minute
        df.rename(columns=self.col_map, inplace=True)

        return df

    def get_snapshot(self, df: pd.DataFrame) -> pd.DataFrame:

        df = self.shift_by_1_minute(df)

        with ThreadPoolExecutor(max_workers=10) as executor:
            prices = list(executor.map(
                self.get_last_trade_price, df['ISIN'].tolist()))

            df['1m'] = prices
        return df

    def snapshot_scheduler(self, df: pd.DataFrame, snapshot_file: str, force_open: bool = False) -> None:
        count = 0
        self.get_last_trade_price(df['ISIN'][0])
        print('Market is Open.' if self.market_open else 'Market is Closed.')

        while True:
            if self.market_open or force_open:
                start_time = time.time()

                df = self.get_snapshot(df)
            
                new_df = self.aggregated_trend(df)
                fs.write_to_sheet(new_df, snapshot_file)

                count += 1
                print('{} Snapshot is Taken'.format(count))

                remaining_time = max(
                    round(60 - (time.time() - start_time), 2), 0)
                print('Sleeping for {} Seconds...'.format(remaining_time))
                time.sleep(remaining_time)
            else:
                print('Sleeping for 10 min...')
                time.sleep(10*60)
                self.get_last_trade_price(df['ISIN'][0])

            print('')


    def calculate_trend(self, row: pd.Series) -> int:
        trend = 0

        prices = row[2:].values.tolist()

        if max(prices) not in prices[:-5]:
            trend = 1
        elif min(prices) not in prices[:-5]:
            trend = -1
        
        return trend


    def aggregated_trend(self, original_df: pd.DataFrame) -> pd.DataFrame:
        df = original_df.copy()
        df['TREND'] = df.apply(self.calculate_trend, axis=1)

        df.loc['Total'] = ''
        df.loc['Total', '1m'] = 'Aggregated Trend'
        df.loc['Total', 'TREND'] = 0
        df.loc['Total', 'TREND'] = df['TREND'].sum()
        
        
        # Aggregated trend
        self.agg_df.loc[len(self.agg_df)] = [self.time_now(), df.loc['Total', 'TREND']]
        fs.write_to_sheet(self.agg_df, self.agg_file)
        
        return df
