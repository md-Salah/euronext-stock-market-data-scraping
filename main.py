import traceback
import os
import pandas as pd

from modules.euronext import Euronext
from modules import files as fs


def main():
    index_file = 'files/SBF_120_Index.xlsx'
    snapshot_file = 'files/SBF_120_Snapshot.xlsx'
    agg_file = 'files/SBF_120_Aggregated_trend.xlsx'
    if not os.path.exists('files/'):
        os.mkdir('files/')

    try:
        euronext = Euronext(agg_file)
        print('Program is Started at UTC: {}'.format(euronext.time_now()))

        # Get SBF 120 index composition
        if not os.path.exists(index_file):
            print('\nUpdating SBF 120 Index Composition...')
            SBF_120 = 'https://live.euronext.com/popout-page/getIndexComposition/FR0003999481-XPAR'
            items = euronext.get_index_composition(SBF_120)
            fs.write_to_sheet(pd.DataFrame(items), index_file)

        # Read snapshot file
        df = fs.read_sheet(snapshot_file)
        df = df[[col for col in df.columns if 'TREND' not in col]]
        df = fs.read_sheet(index_file) if len(df) == 0 else df[:-1:]
        
        # Snapshot scheduler
        euronext.snapshot_scheduler(df, snapshot_file, True)
        del euronext

    except Exception:
        traceback.print_exc()
        fs.write_to_txt(traceback.format_exc(), 'error.txt') 
    finally:
        print('Program Ends.')


if __name__ == '__main__':
    main()
