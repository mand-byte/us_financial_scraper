import pandas as pd
import numpy as np

s = pd.Series([b"abc", 123, None, np.nan])
s = s.fillna("").astype(str)
print(s.tolist())
print([type(x) for x in s.tolist()])
