import pandas as pd
import numpy as np


#assuming these are 18 week seasons, tw=currweeks, wdf = week df
def add_season(df, csv, tw, wdf, snum):
    df2 = pd.read_csv(csv)
    df2 = df2.drop('#',axis=1)
    df2 = df2.iloc[:,:-2]
    nw = 0
    for col in df2.columns:
        try:
            oc = int(col)
            df2[col] = pd.to_numeric(df2[col], errors='coerce').fillna('')
            nw +=1
            newcol = oc+tw
            df2 = df2.rename(columns={col:newcol})
            new_data= {'Id': newcol, 'Week_Num':nw, 'Season':snum}
            wdf.loc[len(wdf)] = new_data
        except ValueError:
            print("Valueerror"+col)
            continue
    df = pd.merge(df, df2, on=["Player", "Pos", "Team"], how='outer')
    pd.set_option('display.max_columns', None)
    print(wdf.head(25))
    return df, tw+nw, wdf


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

#this is where things get ugly
def fill_op_cols(df: pd.DataFrame):
    cols_to_load = ['season', 'game_week', 'name_display', 'opp_abb']
    mf = pd.read_csv("project_datasets/qbr_week_level_2020.csv", usecols=cols_to_load)
    df.set_index('Player', inplace=True)
    for _, row in mf.iterrows():
        row['game_week'] = (int(row['season'])%10*18)+row['game_week']
        df.loc[row['name_display'], 'week_'+str(row['game_week'])+'_opp'] = row['opp_abb']
    return df


y2020 = pd.read_csv("project_datasets/FantasyPros_Fantasy_Football_Points_QB_2020.csv")
y2020 = y2020.drop('#',axis=1)
y2020 = y2020.iloc[:,:-2]
weeks = pd.read_csv("project_datasets/Weeks.csv")
#remove - & byes
for col_index in range(3,19):
    column_name = y2020.columns[col_index]
    print(column_name)
    y2020[column_name] = pd.to_numeric(y2020[column_name], errors='coerce').fillna('')
totalweeks = 17
#games per season
gps = [17]
y2020, totalweeks, weeks= add_season(y2020, "project_datasets/FantasyPros_Fantasy_Football_Points_QB_2021.csv", totalweeks, weeks, 2021)
y2020, totalweeks, weeks = add_season(y2020, "project_datasets/FantasyPros_Fantasy_Football_Points_QB_2022.csv", totalweeks, weeks, 2022)
y2020, totalweeks,weeks = add_season(y2020, "project_datasets/FantasyPros_Fantasy_Football_Points_QB_2023.csv", totalweeks, weeks, 2023)
y2020 = add_opp_cols(y2020)
y2020 = fill_op_cols(y2020)
y2020.to_csv("PPpW.csv", index=True)
weeks.to_csv("project_datasets/Weeks.csv", index=False)
