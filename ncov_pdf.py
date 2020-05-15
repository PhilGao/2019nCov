from PyPDF2 import PdfFileReader
import tabula
import pandas as pd
from functools import reduce
import logging
import os

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.expand_frame_repr', False)

logger = logging.getLogger("ncov_logger")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('ncov.log')
fh.setLevel(logging.WARNING)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)


def extract_china(df, china_header, filename):
    df_china = df.dropna(axis=0, how='any').applymap(
        lambda x: x.replace("Taipei and environs", "Taiwan").replace("Hong Kong SAR", "HongKong").replace(
            "Macao SAR", "Macao").replace("Inner Mongolia", "InnerMongolia"))
    df_china = pd.concat(
        list(df_china[col].str.split(" ", expand=True) for col in range(0, df_china.shape[1])),
        axis=1, ignore_index=True)
    df_china.columns = china_header
    df_china.to_csv('./output/{0}{1}.csv'.format(filename.split('-')[0], "_china"), index=False)
    logger.warning("{0} processing china data successfully !".format(filename))
    return


def extract_world(dfs, world_header, filename):
    dfs_worlds = [df.dropna(axis=1, how='all') for df in dfs]
    for _ in dfs_worlds:
        _.columns = world_header
    df_world = pd.concat(dfs_worlds, axis=0, ignore_index=True).dropna(axis=0, how='any').iloc[1:, :]
    df_world.to_csv('./output/{0}{1}.csv'.format(filename.split('-')[0], "_world"), index=False)
    logger.warning("{0} processing world data successfully !".format(filename))
    return


def extract_pdftable(filename):
    folder_path = "./data/"
    try:
        logger.info("{0} start processing !".format(filename))
        # extract dataframes from pdf
        dfs = tabula.read_pdf(folder_path + filename, multiple_tables=True,
                              pandas_options={'header': None}, pages='all'
                              )
        # todo: figure out why this is not correct but below is fine
        # def format_china_province(element):
        #     mapping = {"Taipei and environs": "Taiwan", "Hong Kong SAR": "HongKong", "Macao SAR": "Macao",
        #                "Inner Mongolia": "InnerMongolia"}
        #     for k, v in mapping.items():
        #         if k in element:
        #             print(element, element.replace(k, v))
        #             return element.replace(k, v)
        #         else:
        #             print("Not equal！the element is " + element)
        #             return element

        # remove the header & format province name
        # prepare header & df list
        idx_china, idx_world = None, None

        china_header = ["province", "population", "new confirmed case", "new suspected case", "new death",
                        "cumulative confirmed case", "cumulative deaths"]
        world_header = ['country', 'total case', 'total new case', 'death', 'new death', 'transmission classification',
                        'days since last reported case']
        # format the df data , extract china & world separately
        for index, df in enumerate(dfs):
            # using condition :[Hubei in first column] to identify which df is china
            if "Province" in ''.join(list(df.iloc[:, 0].values.astype(str))):
                # formalize china df data
                print(''.join(list(df.iloc[:, 0].values.astype(str))))
                idx_china = index
                continue
            # assuming all left tables would be world data
            if "Country" in ''.join(list(df.iloc[:, 0].values.astype(str))):
                idx_world = index
                break
        # formalize & combine china dfs
        if idx_china is not None:
            extract_china(dfs[idx_china], china_header, filename)
        else:
            logger.warning("{0} not find China Province data !".format(filename))
        # formalize & combine world dfs
        if idx_world is not None:
            extract_world(dfs[idx_world:], world_header, filename)
    except Exception as e:
        logger.warning("while process the file {0},throw exception {1}".format(filename, e))


def preview_pdf(filename):
    folder_path = "./data/"
    dfs = tabula.read_pdf(folder_path + filename, multiple_tables=True,
                          pandas_options={'header': None}, pages='all'
                          )
    for df in dfs:
        print(df)


if __name__ == "__main__":
    # for root, dirs, files in os.walk('./data'):
    #     for file in files:
    #         if file >'20200201' and file < '20200302':
    #             extract_pdftable(file)
    preview_pdf('20200228-sitrep-39-covid-19.pdf')
    extract_pdftable('20200228-sitrep-39-covid-19.pdf')
