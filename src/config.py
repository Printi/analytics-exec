import __main__ as main
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import time
import os
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

## Server: BigQuery ou AWS-MySQL
SERVER = 'AWS-MySQL'

## DATAS E TEMPS
NOW = datetime.now(ZoneInfo('America/Sao_Paulo'))
TODAY = NOW.date()
TOMORROW = TODAY + relativedelta(days=1)

## DATA REFERENCIA -> D-1
YESTERDAY= TODAY + relativedelta(days=-1)

SCRIPT_TIMER_INITIAL_VALUE = time.perf_counter()
INITIAL_DB_DATE = datetime(2022, 1, 1)

## MESES - INTERVALO DE ATUALIZAÇÃO
TABLE_UPDATE_INTERVAL=5



