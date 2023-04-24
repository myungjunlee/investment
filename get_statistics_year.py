import pandas as pd

# 읽어올 엑셀 파일 지정
filename = './output.xlsx'

# 엑셀 파일 읽어 오기
df = pd.read_excel(filename, engine='openpyxl')

filter = df[df['내용'].isin(['익절성공', '손절발생'])]

column_list = ['년도', '유형', '승','패','승률','평균이익', '평균손실', '손익비', '수익률','최대손실', '최대낙폭']

total_df = pd.DataFrame(columns=column_list)


for i in range(2017,2025):
    option = "롱/숏"
    win = 0
    lost = 0
    total_profit = 0
    total_loss = 0
    buy_num_list = [0,0,0,0,0]
    mdd = 0
    max_low = 0

    if (i != 2024):
        season_df = filter[filter['날짜'].between(str(i)+'-01-01', str(i)+'-12-31')]
    else:
        season_df = filter

    long_df = season_df[season_df['롱/숏'].isin(['long'])]
    short_df = season_df[season_df['롱/숏'].isin(['short'])]

    for j in range(0,3):
        if j == 1:
            season_df = long_df
            option = '롱'
        elif j == 2:
            season_df = short_df
            option = '숏'

        season_df['손익'] = season_df['손익'].str.rstrip('%')
        season_df = season_df.astype({'손익':'float'})


        total_profit = season_df[season_df['손익'] > 0]['손익'].sum()
        total_loss = season_df[season_df['손익'] < 0]['손익'].sum()
        win = len(season_df.loc[season_df['손익'] > 0 ])
        lost = len(season_df.loc[season_df['손익'] < 0 ])
        max_low = season_df['최대 낙폭'].min()
        mdd = season_df['손익'].min()
        last_index = season_df['ROR'].cumprod().index[-1]
        hpr = season_df['ROR'].cumprod()[last_index]
        print(season_df['ROR'].cumprod())

        if total_profit != 0 and win != 0:
            average_profit = round(total_profit/win,2)
            win_lost = round(win/(win+lost)*100,2)
        else:
            average_profit = 0
            win_lost = 0

        if total_loss != 0 and lost != 0:
            average_loss = round(total_loss/lost,2)
        else:
            average_loss = 0

        if total_profit != 0 and total_loss != 0:
            profit_loss = round(average_profit/(average_loss*(-1)),2)
        else:
            profit_loss = 0

        if (j==0):
            total_df = total_df.append(pd.Series(), ignore_index=True)

        if (i==2024):
            i='모든 년도'
        stat_list = [i, option,  win, lost, win_lost,average_profit, average_loss, profit_loss, hpr, mdd, max_low]
        stat_df = pd.DataFrame([stat_list], columns=column_list)
        total_df = pd.concat([total_df,stat_df],axis=0, ignore_index=True)
        
total_df.to_excel("./yearstat_output.xlsx", index=False)
