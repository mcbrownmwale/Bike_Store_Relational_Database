import pandas as pd

df = pd.read_csv("data/categories.csv")
df = df.to_records(index=False)
for i in df:
    print(i)
def pop_data(df):
    rec = df.to_records(index = False)
    
