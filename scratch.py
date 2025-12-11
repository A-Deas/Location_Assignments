import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
from pathlib import Path
import geopandas as gpd
import pyarrow.dataset as ds
import pandas as pd
import pickle
import pickletools
import numpy

import pickle

file = "Synthetic Population/acts_cohort_21__1_fmt.parquet"
df = pd.read_parquet(file)
print(df.head(5))