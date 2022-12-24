#!/usr/bin/env python3

# Script to read MEA emails and convert attached Excel spreadsheets of
# 15 minute meter readings into CSV files.

from pathlib import Path
import imaplib
import email
from io import BytesIO
import logging
import logging.handlers
import time

import pandas as pd
import numpy as np

import settings

# Set up logging
    
# Log file for the application
log_dir_path = Path(settings.base_dir).expanduser() / 'email-logs'
log_dir_path.mkdir(parents=True, exist_ok=True)
LOG_FILE = str(log_dir_path / 'mea-email.log')

# create base logger for the application.
_logger = logging.getLogger('meadata')

# set the log level
_logger.setLevel(logging.INFO)

# create a rotating file handler
fh = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=200000, backupCount=5)

# create formatter and add it to the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

# add the handler to the logger
_logger.addHandler(fh)

def process_msg(msg, data_path):
    try:

        # Find all the attachments that are Excel files and process
        for part in msg.walk():
            fname = part.get_filename()
            if (fname is not None):
                print(fname)
                # this is an attachement.  assume that it is an Excel file of data
                try:
                    attachment = part.get_payload(decode=True)
                    df = pd.read_excel(BytesIO(attachment)).dropna(how='all')

                    # used to assemble final DataFrame from individual DataFrames from
                    # each row.
                    df_final = pd.DataFrame()
                    for _, row in df.iterrows():
                        row_data = row.dropna().values
                        data_col_ct = len(row_data) - 2     # number of columns containing data
                        interval_secs = int(24 * 3600 / data_col_ct)   # number of seconds in each interval
                        if data_col_ct in (24, 96):
                            vals = row_data[2:].astype(float) * data_col_ct / 24  # to convert to average kW
                            sensor_id = f'mea_{row_data[0]}'

                            # Make timestamps, starting 1/2 interval past midnight and properly spaced
                            day_start = row_data[1].tz_localize('US/Alaska', ambiguous='NaT').value // 10 ** 9
                            seconds = np.array(list(range(interval_secs // 2, 3600 * 24, interval_secs)))
                            ts = day_start + seconds

                            # Put into DataFrame for easy filtering
                            dfr = pd.DataFrame({'ts': ts, 'val': vals, 'id': [sensor_id] * 96})
                            df_final = pd.concat([df_final, dfr])

                    # Remove outliers from data.  No zero values, and no very large values,
                    # more than 2.5 times 95th percentile value.
                    find_good = lambda x: (x > 0) & (x < x.quantile(.95) * 2.5)
                    good_data = df_final.groupby('id')['val'].transform(find_good).astype(bool)
                    df_final = df_final[good_data]
                    print(len(df_final))

                    # Write to a CSV file. Include a timestamp in the file name so file
                    # are unique.
                    fn = f'{time.time():.3f}.csv'
                    out_path = data_path / fn
                    df_final[['id', 'ts', 'val']].to_csv(out_path, index=False)  # Pandas takes Path's directly

                    _logger.info(f'{len(df_final)} records processed from {fname}')

                except Exception as e:
                    print(e)
                    _logger.exception('Error processing MEA data from file %s.' % fname)

    except Exception as e:
        print(e)
        _logger.exception('Error processing MEA data.')

if __name__ == '__main__':
    # variable for the base data directory
    data_path = Path(settings.base_dir).expanduser() / 'data'
    data_path.mkdir(parents=True, exist_ok=True)
    
    mail = imaplib.IMAP4_SSL(settings.imap_url)
    retcode, capabilities = mail.login(settings.user, settings.password)
    mail.list()
    mail.select('inbox')

    retcode, messages = mail.search(None, '(UNSEEN)')
    if retcode == 'OK':

        for num in messages[0].split():

            typ, data = mail.fetch(num,'(RFC822)')
            print('processing a message')
            for response_part in data:

                if isinstance(response_part, tuple):
                    original = email.message_from_bytes(response_part[1])
                    process_msg(original, data_path)

                    # Mark message as read
                    typ, data = mail.store(num,'+FLAGS','\\Seen')
