# -*- coding: utf-8 -*-
"""
Created on Thu Jan 05 10:03:32 2017

@author: user
"""

import pyodbc
import datetime as DT
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
from mpl_toolkits.mplot3d import Axes3D
import cdecimal

import itertools
#list(itertools.permutations(s_capital.columns.values, 2))

##########################################################################################
#Bond Yield Query
##########################################################################################
StartDate=('20130101',)
Bondclass=('1010000', '4000000', '5010110', '7010123', '6010122', '2020000', '3030110') 
#1010000 : 국고, 4000000:통안, 5010110:산금, 7010123:회사(AA-), 6010122:여전(AA0), 2020000:지역개발채, 3030110:공사채
#Bondclass=('5010110',) 
Valuetype=('1',)
#Maturity=('3','6','9','12')

cnxn=pyodbc.connect('DSN=KBXUPDB; UID=STARM2SEL; PWD=starm2sel2*')
cursor=cnxn.cursor()

placeholder= '?' 
placeholders1= ', '.join(placeholder for unused in Bondclass)
#placeholders2= ', '.join(placeholder for unused in Maturity)

cursor.execute("""Select BASE_DT, BOND_SCLASS_CD, REMAIN_M_NUM, RTN From KBFMSDB.DBO.OSBBR23 
WHERE BASE_DT >= ? AND BOND_SCLASS_CD IN (%s) AND VALUAT_CO_TP = ?  
Order by BASE_DT, REMAIN_M_NUM""" % (placeholders1), (StartDate + Bondclass + Valuetype))

rows=cursor.fetchall()
#for row in rows:
#    print row.BASE_DT, row.REMAIN_MTRT, row.MIDDLE_RTN*100

date= [x[0] for x in rows]
date_ix= pd.to_datetime(date)
typ= [x[1] for x in rows]
mat= [x[2] for x in rows]
rtn= [x[3] for x in rows]

yield_long=pd.DataFrame({'date': date_ix,
                        'type': typ, 
                        'maturity' : mat,
                        'return' : rtn})
#yield_wide=yield_long.pivot(index='date', columns=['maturity', 'type'], values='return')
yield_wide=pd.pivot_table(yield_long, values='return', index='date', columns=['maturity', 'type'], aggfunc='first')
#yield_wide.dropna(axis=0, how='any', inplace=True)

########################################################################################################
#Maturity-spread analysis(select asset->compute adjacent maturity spread)
#Maturity_Spread('1010000', True, start, end)
#Maturity_Spread('1010000', False, maturity1, maturity1', ,maturity 2, maturity 2', ...) : input 2*maturity of which you want to see the spread
########################################################################################################
def Maturity_Spread (asset_code, adjacent, *maturity) :
        
    #Extract each asset
    treasury=yield_wide.xs(asset_code, level='type', axis=1)
        
    if adjacent : 
        partial=treasury.ix[:, cdecimal.Decimal(maturity[0]):cdecimal.Decimal(maturity[1])]
        ##########################################################################################
        #Spreads of adjacent maturity(yield_spread)
        mat_list=set(partial.columns)
        mat_cardinal=range(len(list(mat_list)))
        combination=zip(mat_cardinal, mat_cardinal[1:])
        
        treasury_spread=pd.DataFrame(index=partial.index)
        for i in combination:
            temp=partial.ix[:,i[1]]-partial.ix[:,i[0]]
            temp.name=str(partial.ix[:,i[1]].name) + '-' + str(partial.ix[:,i[0]].name)
            treasury_spread = pd.concat([treasury_spread, temp],axis=1)
        ##########################################################################################
        
        #percentile to bp, float type
        treasury_spread = treasury_spread*10000
        treasury_spread = treasury_spread.astype(float)
        #Ranking for each column(maturity)
        #treasury_spread_rank=treasury_spread.rank(axis=0, na_option='keep', pct='True')
        
        #Graph
        treasury_spread.plot()
        #treasury_spread_rank.ix[:,0:5].plot()
    else :
        if len(maturity)%2 == 0 :
            num=len(maturity)/2
            for i in range(num) :
                user_defined=(treasury[cdecimal.Decimal(maturity[2*i+1])]-treasury[cdecimal.Decimal(maturity[2*i])])*10000
                plt.plot(user_defined, label=asset_code+':'+str(maturity[2*i+1]/12)+'Y'+'-'+str(maturity[2*i]/12)+'Y')
            plt.legend(loc='upper right')
            
        else :
            print("Input Propoer Variable. Input number of maturity should be even.")
            #plt.suptitle(asset_code+':'+str(maturity[1])+'-'+str(maturity[0]))



########################################################################################################
#Sector-spread analysis(select two assets->compute spread for each maturity)
########################################################################################################

#Select 2 assets
asset_1='1010000'
asset_2='5010110'

s_treasury=yield_wide.xs(asset_1, level='type', axis=1)
#a=(s_treasury-s_treasury.shift(5))*10000
#a.tail(1)
s_capital=yield_wide.xs(asset_2, level='type', axis=1)
#Compute spread and delete columns whose values are all NA
s_spread=s_capital.subtract(s_treasury, axis=1)
s_spread.dropna(axis=1, how='all', inplace=True)
#Percentile to bp
s_spread = s_spread*10000
s_spread = s_spread.astype(float)

#Graph
start=4
end=5
for i in np.arange(start, min(end, len(s_spread.columns)), 1):
    plt.plot(s_spread.ix[:,i], label=s_spread.columns[i])
plt.suptitle(str(asset_2)+'-'+str(asset_1))
plt.legend(loc='upper left')
plt.margins(0.01, 0)
#plt.savefig(str(asset_2)+'-'+str(asset_1)+'.png')
#s_spread.ix[:,0:6].plot()

########################################################################################################
#Sector-spread analysis(fix maturity -> fix treasury/select other asset)
########################################################################################################

#Select Maturity
s2_maturity=yield_wide.xs(24, level='maturity', axis=1)
#Fix Treasury and compute spread for each asset
asset_num=len(s2_maturity.columns)-1

s2_spread=pd.DataFrame(index=s2_maturity.index)
for i in range(asset_num):
    temp=s2_maturity.ix[:,i+1]-s2_maturity.ix[:,0]
    temp.name=str(s2_maturity.ix[:,i+1].name) + '-' + 'Treasury' + '(2Y)'
    s2_spread = pd.concat([s2_spread, temp], axis=1)

#Percentile to bp
s2_spread = s2_spread*10000
s2_spread = s2_spread.astype(float)

#Graph
s2_spread.plot()


########################################################################################################
#Swap spread analysis for each maturity
########################################################################################################

##########################################################################################
#Swap Yield Query
##########################################################################################
#CCS : KBFMSDB.DBO.OSDBR10
#IRS : KBFMSDB.DBO.OSDBR12
cursor.execute("""Select BASE_DT, REMAIN_MTRT, MIDDLE_RTN From KBFMSDB.DBO.OSDBR12
Where BASE_DT >= ? and NOTI_CO_TP='KMB'
Order by BASE_DT, REMAIN_MTRT""", StartDate)

rows=cursor.fetchall()

date= [x[0] for x in rows]
date_ix= pd.to_datetime(date)
mat= [x[1] for x in rows]
rtn= [x[2] for x in rows]

IRS_yield_long=pd.DataFrame({'date': date_ix,
                             'maturity' : mat,
                             'return' : rtn})
IRS_yield_wide=pd.pivot_table(IRS_yield_long, values='return', index='date', columns='maturity', aggfunc='first')


##########################################################################################
#Match maturity btw Treasury and interest swap
##########################################################################################
swap_treasury=yield_wide.xs('1010000', level='type', axis=1)

swap_spread=pd.DataFrame(index=IRS_yield_wide.index)
for i in range(len(IRS_yield_wide.columns)):
    for j in range(len(swap_treasury.columns)):
        if IRS_yield_wide.columns[i]==swap_treasury.columns[j]/12:
            temp=IRS_yield_wide.ix[:,i]-swap_treasury.ix[:,j]
            temp.name=str(swap_treasury.columns[j]/12) + 'Y'
            swap_spread = pd.concat([swap_spread, temp], axis=1)

#Percentile to bp
swap_spread = swap_spread*10000
swap_spread = swap_spread.astype(float)

#Graph
swap_spread.ix[:,0:3].plot()


##########################################################################################
# score : spread analysis using multiple maturity
##########################################################################################



#percentile to bp
yield_spread = yield_spread*10000



#wide to long type to draw graph
yield_spread_long = yield_spread.stack()
yield_spread_long = yield_spread_long.reset_index()
yield_spread_long.columns = ["Date", "Maturity", "spread(bp)"]
yield_spread_long.ix[:,2] = yield_spread_long.ix[:,2].astype(float)

#Ranking
yield_spread_rank=yield_spread.rank(axis=0, na_option='keep', pct='True').tail(n=1)
yield_spread_rank20=yield_spread.rank(axis=0, na_option='keep', pct='True').tail(n=20)


sns.boxplot(x="Maturity", y="spread(bp)", data=yield_spread_long)
sns.stripplot(x="Maturity", y="spread(bp)", data=yield_spread_long[yield_spread_long.Date == max(yield_spread_long.Date)], palette=[u'#e41a1c']*14)
sns.stripplot(x="Maturity", y="spread(bp)", data=yield_spread_long[yield_spread_long.Date == sorted(set(yield_spread_long.Date))[-20]], palette=['yellow']*14)




##########################################################################################
# Compare adjacent maturity and find z-score
##########################################################################################
asset_1='1010000'

s_treasury=yield_wide.xs(asset_1, level='type', axis=1)
for i in range(len(s_treasury.columns)):
    


##########################################################################################    
fig=plt.figure()
ax=fig.gca(projection='3d')
ax.plot_trisurf(IRS_raw.date, IRS_raw.maturity, IRS_raw.rtn, linewidth=0.2)

#surf=ax.plot_wireframe(date_ix, mtrt, rtn)

plt.show()


x1=np.linspace(IRS_raw['maturity'].min(),IRS_raw['maturity'].max(), len(IRS_raw['maturity'].unique()))