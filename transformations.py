import pandas as pd
import numpy as np


#assuming these are 18 week seasons, tw=currweeks
def add_season(df, csv, tw):
    df2 = pd.read_csv(csv)
    df2 = df2.drop('#',axis=1)
    nw = 0
    for col in df2.columns:
        try:
            oc = int(col)
            nw +=1
            newcol = oc+tw
            df2 = df2.rename(columns={col:newcol})
        except ValueError:
            print("Valueerror"+col)
            continue
    df = pd.merge(df, df2, on=["Player", "Pos", "Team"], how='outer')
    pd.set_option('display.max_columns', None)
    print(df.head())
    return df, tw+nw

#gonna have to fill by hand
def add_opp_cols(df):
    for col in df.columns:
        try:
            oc=int(col)
            newname = 'week_'+str(oc)+'_opp'
            df.insert(df.columns.get_loc(col)+1,newname,"")
        except ValueError:
            print("Valueerror"+col)
            continue
    return df
y2020 = pd.read_csv("project_datasets/FantasyPros_Fantasy_Football_Points_QB_2020.csv")
y2020 = y2020.drop('#',axis=1)
totalweeks = 17
y2020, totalweeks = add_season(y2020, "project_datasets/FantasyPros_Fantasy_Football_Points_QB_2021.csv", totalweeks)
y2020, totalweeks = add_season(y2020, "project_datasets/FantasyPros_Fantasy_Football_Points_QB_2022.csv", totalweeks)
y2020, totalweeks = add_season(y2020, "project_datasets/FantasyPros_Fantasy_Football_Points_QB_2023.csv", totalweeks)
y2020 = add_opp_cols(y2020)
y2020.to_csv("PPpW.csv", index=True)
