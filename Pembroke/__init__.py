"""
This script processes several documents of three different types and verfies contents based on document type.
"""

import logging
import pandas as pd
import azure.functions as func
from datetime import datetime, date
from io import BytesIO

# column headers of each file type
fixed_income_headers = ['AGREEMENT', 'AGR_SERIAL', 'LIVE_DATE', 'COMPLETED_DATE', \
    'STATUS_1', 'STATUS_2', 'FIXED_INCOME_YYYY_MM', 'FIXED_INCOME']

agreements_headers = ['AGREEMENT_NUMBER', 'AGR_SERIAL', 'PRODUCT', 'CUSTOMER_EFFECTIVE_RATE', \
    'CUSTOMER_FLAT_RATE', 'COMPONENT_START_DATE', 'COMPONENT_LIVE_DATE', 'COMPONENT_END_DATE', 'CASH_PRICE', \
        'AGR_DEPOSIT', 'AGR_INDIRECT_DEPOSIT', 'TOTAL_DEPOSIT', 'CONTRA_AMOUNT', 'CONTRA_AGREEMENT', \
            'PART_EXCHANGE_VALUE', 'ADMIN_FEE', 'ADMIN_FEE_B', 'OPTION_FEE', 'TOTAL_FEES', 'ADVANCE', \
                'INTEREST_CHARGES', 'TERM', 'TERM_IN_MONTHS']

customers_headers = ['AGREEMENT_NUMBER', 'AGR_SERIAL', 'PRODUCT', 'TTL_CODE', 'PPL_FORENAMES', \
    'PPL_MIDDLE_NAME', 'PPL_SURNAME', 'COMPANY_PERSON_IND', 'SAD_HOUSE_NAME', 'SAD_HOUSE_NUMBER', \
        'SAD_STREET_NAME', 'SAD_LOCALITY', 'SAD_POST_TOWN', 'SAD_COUNTY_CODE', 'CNT_CODE', 'SAD_POST_CODE']

def main(req: func.HttpRequest):

    # variables
    headers = []
    fileColumns = []
    new_filename = ''
    file_type = ''

    # log filename and path
    filename = req.headers.get('name')
    logging.info('filename: ' + filename)

    # discover file type by checking filename
    if('fixed' in filename.lower() and 'income' in filename.lower()):
        logging.info('file is fixed_income file')
        file_type = 'Fixed_Income_'
        headers = fixed_income_headers
    elif('agreement' in filename.lower()): 
        logging.info('file is agreement file')
        file_type = 'Agreement_'
        headers = agreements_headers
    elif('customer' in filename.lower()):
        logging.info('file is customer file')
        file_type = 'Customer_'
        headers = customers_headers
    else:
        logging.info('file is unknown')
        new_filename = 'unknown file name'
        return new_filename

    # read request body (file contents)
    fileContent = req.get_body()
    logging.info('processed body into fileContent')

    # read csv file to dataframe
    df = pd.read_csv(BytesIO(fileContent), encoding = 'latin-1')
    logging.info('processed fileContent into df')
    logging.info(df.columns)

    # write column headers from dataframe to list
    for column in df.columns:
        fileColumns.append(column.upper())
    logging.info(fileColumns)

    # verify headers against what should be there
    if (fileColumns == headers):
        logging.info('fileColumns == headers')
        new_filename = file_type + str(date.today()) + '_' + str(datetime.now().strftime("%H:%M:%S"))
    else: 
        logging.info('fileColumns != headers')
        new_filename = 'wrong columns'

    logging.info(new_filename)

    return new_filename