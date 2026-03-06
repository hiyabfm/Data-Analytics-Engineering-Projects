import pandas as pd

df = pd.read_excel("D598 Data Set.xlsx")

df2 = df.drop_duplicates()

df3 = df2.groupby("Business State").agg(['mean', 'median', 'min', 'max'])
df3.columns = ['_'.join(col) for col in df3.columns] 
df3.reset_index(inplace=True)

df4 = df2[df2['Debt to Equity'] < 0]

df5 = df2.apply(
    lambda row: 0 if row['Total Revenue'] == 0 else row['Total Long-term Debt'] / row['Total Revenue'],
    axis=1
)
df5 = pd.DataFrame(df5, columns=['Debt-to-Income Ratio'])

df6 = pd.concat([df2, df5], axis=1)
