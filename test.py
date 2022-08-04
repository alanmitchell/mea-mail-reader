# %%

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

# %%
mail = imaplib.IMAP4_SSL(settings.imap_url)
retcode, capabilities = mail.login(settings.user, settings.password)
mail.list()
mail.select('inbox')

retcode, messages = mail.search(None, '(UNSEEN)')

