import pandas as pd


xls = pd.ExcelFile("Evolution_population_2012-2020.xlsx")
df_pop_dep = pd.read_excel(xls,"DEP", skiprows=3)
df_pop_reg = pd.read_excel(xls, "REG")

col_pop_dep = df_pop_dep.columns.values.tolist()

print(df_pop_dep.shape)
print(col_pop_dep)



print(df_pop_dep)