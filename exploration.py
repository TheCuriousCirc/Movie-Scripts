#!/media/ubuntu/AMSRG/Dabid/Projects/TheCuriousCirc/TSS-Project-1/venv/bin/python
"""
Movie Scripts Data Exploration
"""

import logging

import pandas as pd

logging.basicConfig(
    filename='exploration.log',
    level=logging.DEBUG,
    format='%(levelname)s:%(lineno)d:%(message)s'
)

pd.set_option('display.max_columns', None)


def wrangle_df1(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pivot_table(
            values=['Rank', 'Amount'],
            index=df.index,
            columns='Record',
            aggfunc='first'
        )
        .stack()
        .stack()
        .to_frame()
        .T
    )


def wrangle_df2(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pivot_table(
            values='Value',
            index=df.index,
            columns='Feature',
            aggfunc='first'
        )
        .stack()
        .to_frame()
        .T
    )


def wrangle_df3(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pivot_table(
            values=[
                'Rank',
                'Amount',
                'ChartDate',
                'Days InRelease'
            ],
            index=df.index,
            columns='Record',
            aggfunc='first'
        )
        .stack()
        .stack()
        .to_frame()
        .T
    )


def main():

    # Import data: `detail_full`
    details_data = pd.read_pickle(
        '/media/ubuntu/AMSRG/Dabid/Projects'
        '/TheCuriousCirc/TSS-Project-1/data/raw'
        '/detail_full/movie_full_detail2005.pickle'
    )

    for title in details_data:

        logging.info(f'Movie Title: {title}')

        try:
            dff0 = pd.read_pickle('dff.pickle')
            logging.info('`dff.pickle` file found. Reading pickle file.')
        except FileNotFoundError:
            dff0 = pd.DataFrame()
            logging.info('`dff.pickle` file not found')

        try:
            df1 = details_data[title][1]
            logging.info('df1 exists.')

            df2 = details_data[title][2]
            logging.info('df2 exists')

            df3 = details_data[title][3]
            logging.info('df3 exists')

            dff1 = pd.concat(
                [
                    wrangle_df1(df1),
                    wrangle_df2(df2),
                    wrangle_df3(df3)
                ],
                axis=1
            )
            logging.info('Succesfully wrangled and merged dataframe.')

        except KeyError:
            logging.exception("KeyError")
            pass

        dff = pd.concat([dff0, dff1], ignore_index=True)
        logging.info(
            'Successfully added another row to existing dataframe.'
        )

        # Export to pickle file
        dff.to_pickle('dff.pickle')
        logging.info('Successfuly exported file to `dff.pickle`.')


if __name__ == '__main__':
    main()

    record_list = list()
    for title in details_data:
        try:
            record_list.append(set(details_data[title][3]['Record']))
        except KeyError:
            pass
