import traceback
from datetime import datetime
import os
import pandas as pd

from modules.euronext import Euronext
from modules import files as fs


def main():
    print('Program is Started at {}'.format(datetime.now()))
    index_file = 'files/SBF_120_Index.xlsx'
    snapshot_file = 'files/SBF_120_Snapshot.xlsx'

    try:
        euronext = Euronext()

        # Get SBF 120 index composition
        if not os.path.exists(index_file):
            print('Updating SBF 120 Index Composition...')
            SBF_120 = 'https://live.euronext.com/popout-page/getIndexComposition/FR0003999481-XPAR'
            items = euronext.get_index_composition(SBF_120)
            fs.write_to_sheet(pd.DataFrame(items), index_file)

        # Snapshot scheduler
        df = fs.read_sheet(index_file)
        euronext.snapshot_scheduler(df, snapshot_file)
        del euronext

    except Exception:
        traceback.print_exc()
        fs.write_to_txt(traceback.format_exc(), 'error.txt') 
    finally:
        print('Program Ends.')


if __name__ == '__main__':
    main()
