import pandas as pd
import numpy as np
from datetime import datetime
from progress.bar import Bar

customers = pd.read_csv('customers_clean.csv')

# column as series
stellar_customers = customers[customers['Email'].str.contains('stellarreviewers')]