import sys

import pandas as pd

print(sys.argv)

python sample.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=covid_db \
  --table_name=covid_data \
  --url="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/01-01-2021.csv"