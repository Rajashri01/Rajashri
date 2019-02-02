
import pandas as pd
import numpy as np
from pymongo import MongoClient
from bson import ObjectId
from datetime import date
import datetime as dt

client1                 =  MongoClient('mongodb://rapido-intern:cBhSX63p5K4perE6@139.59.71.134:27017/rapidoProd?authSource=admin')
db1                     =  client1['rapidoProd']
ordersCollection        =  db1['orders']


OrderData               =  pd.io.json.json_normalize(list(ordersCollection.find(
                                {"orderDate":{"$gte":"2019-01-01","$lt":"2019-01-02"},
                                "serviceObj.city":"Bangalore",
                                "$or":[{"serviceObj.service":"Delivery"},{"riderObj.shift":{"$in":["DPMB3","DPMB"]}}]},
                                {"_id":0,"orderDate":1,"rider":1,"status":1,"timeBucket":1,"riderObj.shift":1,
                                "serviceObj.city":1,"serviceObj.service":1}
                           )))


OrderData ['status']    =  ['dropped' if x == 'dropped' else 'incomplete' for x in OrderData ['status']]
OrderData['Hour']       =  OrderData['timeBucket'].apply(lambda x: x.split(':')[0]).astype(int)
#OrderData['timeBucket'].iloc[0]
#OrderData[OrderData['Hour']==12]
OrderData.head()


OrderData ['Netrides']    =  OrderData ['status'].apply(lambda x: 1 if x=="dropped" else 0 )
OrderData ['Incomplete']  =  OrderData ['status'].apply(lambda x: 0 if x=="dropped" else 1 )
OrderData ['NetRider']    =  OrderData[OrderData['Netrides']==1]['rider'].apply(lambda x:1 if pd.isnull(x) else x )
OrderData ['Noon_Orders'] =  OrderData[OrderData['Netrides']==1] ['Hour'].apply(lambda x:1 if (x>=0 and x<17) else 0 )
OrderData ['Evng_Orders'] =  OrderData[OrderData['Netrides']==1] ['Hour'].apply(lambda x:1 if (x>=17 and x<24) else 0 )
OrderData ['Noon_Caps']   =  OrderData[OrderData['Noon_Orders']==1]['rider'].apply(lambda x:1 if pd.isnull(x) else x )
OrderData ['Evng_Caps']   =  OrderData[OrderData['Evng_Orders']==1]['rider'].apply(lambda x:1 if pd.isnull(x) else x )

#OrderData.head()
#len(OrderData)
#OrderData[OrderData['Hour']==12]
#OrderData ['riderObj.shift'].unique()

deliveryShifts           = {"Bangalore":["DPMB3","DPMB"],
                                "Hyderabad":[]}

OrderData ['shift']      =  OrderData ['riderObj.shift'].apply(lambda x:'Delivery' if x in deliveryShifts['Bangalore'] else 'NonDelivery')
#OrderData.head()
DeliveryData             =  OrderData[OrderData["serviceObj.service"]=="Delivery"]
df                       =  DeliveryData.groupby(['serviceObj.city','orderDate']).agg({'rider':'nunique','Netrides':np.sum,
                                                                                 'Incomplete':np.sum})
df['GrossRides']         =  df['Netrides'] + df['Incomplete']
df                       =  df.reset_index()
df

# Link data
userlist                  =  DeliveryData['rider'].tolist()
OrderData1                =  pd.io.json.json_normalize(list(ordersCollection.find(
                                {"orderDate":{"$gte":"2019-01-01","$lt":"2019-01-02"},"rider":{"$in":userlist},
                                "serviceObj.city":"Bangalore","serviceObj.service":"Link"},
                                {"_id":0,"orderDate":1,"rider":1,"status":1,"serviceObj.city":1,"timeBucket":1,"riderObj.shift":1}
                            )))
OrderData1 ['Netrides']   = OrderData1 ['status'].apply(lambda x: 1 if x=="dropped" else 0 )
OrderData1 ['Incomplete'] = OrderData1 ['status'].apply(lambda x: 0 if x=="dropped" else 1 )
df1                       =  OrderData1.groupby(['serviceObj.city','orderDate']).agg({'Netrides':np.sum})
df1                       =  df1.reset_index()
df1                       =  df1.rename({"Netrides":"TaxiRides"},axis = 1)
df1                       =  df1.merge(df, on = ['serviceObj.city','orderDate'], how = 'left')
#df1

df2                       =  DeliveryData.groupby(['serviceObj.city','orderDate','shift']).agg({'rider':'nunique','Netrides':np.sum,
                                                                                  'Incomplete':np.sum,"NetRider":'nunique'})
df2                       =  df2.reset_index()
#df2
Table                     = pd.pivot_table(df2, index=['serviceObj.city','orderDate'], columns='shift', values=['rider','Netrides','NetRider'])
Table                     = Table.reorder_levels([1,0], axis=1).sort_index(axis=1)
#Table
Final_Summary             = Table.merge(df1, on = ['serviceObj.city','orderDate'], how = 'left')
Final_Summary
Final_Summary.columns
Final_Summary             =  Final_Summary.rename({"serviceObj.city":"City","GrossRides":"GrossOrders","Netrides":"NetOrders","rider":"Captains",
                                         ('Delivery', 'NetRider'):'Net_DPMB_Caps',('Delivery', 'Netrides'):'DPMB_Orders',
                                        ('Delivery', 'rider'):'DPMB_Captains',('NonDelivery', 'NetRider'):'Net_Taxi_Caps',
                                        ('NonDelivery', 'Netrides'):'Taxi_Orders',('NonDelivery', 'rider'):'Taxi_Captains'},axis = 1)
Final_Summary.head()

cols                      = ['City','orderDate','GrossOrders','NetOrders','TaxiRides','Captains','DPMB_Captains',
                                'Net_DPMB_Caps','DPMB_Orders','Taxi_Captains','Net_Taxi_Caps','Taxi_Orders']

Final_Summary              = Final_Summary[cols]
Final_Summary['Avg_Orders']      = Final_Summary['NetOrders'] / Final_Summary['Captains']
Final_Summary['Avg_DPMB Orders'] = Final_Summary['DPMB_Orders'] / Final_Summary['DPMB_Captains']
Final_Summary

Comparison                =  DeliveryData.groupby(['serviceObj.city','orderDate']).agg({'Noon_Orders':np.sum,
                                                                'Evng_Orders':np.sum,'Noon_Caps':'nunique','Evng_Caps':'nunique'})

Comparison                =  Comparison.reset_index()
Comparison                =  Comparison.rename({"serviceObj.city":"City"},axis = 1)
#Comparison
Comparison                =  Comparison.reset_index()
Comparison                =  Comparison.merge(Final_Summary, on = ['City','orderDate'], how = 'left')
#Comparison
Comparison['Net_Captains']=  Comparison['Net_DPMB_Caps'] + Comparison['Net_Taxi_Caps']
cols                      =  ['City','orderDate','Net_Captains','NetOrders','Noon_Caps','Noon_Orders','Evng_Caps',
                                        'Evng_Orders','DPMB_Captains','DPMB_Orders','Taxi_Captains','Taxi_Orders']
Comparison                =  Comparison[cols]
Comparison
DPMB3                     =  OrderData[OrderData["riderObj.shift"]=="DPMB3"]
DPMB3.head()
df5                       =  DPMB3.groupby(['orderDate','serviceObj.city','riderObj.shift']).agg({'NetRider':'nunique'})
df5                       =  df5.reset_index()
df5                       =  df5.rename({'NetRider':'DPMB3_Caps','serviceObj.city':'City'},axis = 1)
df5.drop(['riderObj.shift'], axis=1, inplace=True)
df4                       =  DPMB3.groupby(['orderDate','serviceObj.city','riderObj.shift','serviceObj.service']).agg({'Noon_Orders':np.sum,'Netrides':np.sum,
                                                                'Noon_Caps':'nunique'})
df4                       =  df4.reset_index()
df4                       =  df4.rename({"serviceObj.city":"City"},axis = 1)
Table1                    =  pd.pivot_table(df4, index=['orderDate','City','riderObj.shift'], columns='serviceObj.service', values=['Noon_Orders','Netrides','Noon_Caps'])
Table1                    =  Table1.reorder_levels([1,0], axis=1).sort_index(axis=1)
Table1
Comparison                =  Comparison.merge(Table1, on = ['City','orderDate'], how = 'left')
Comparison                =  Comparison.merge(df5, on = ['City','orderDate'], how = 'left')
Comparison

Comparison                =  Comparison.rename({('Link', 'Noon_Orders'):'DPMB3_Noon_Taxi',('Delivery', 'Netrides'):'DPMB3_Orders',
                                  ('Link','Netrides'):'DPMB3_Taxi_Rides',
                                  ('Delivery','Noon_Orders'):'DPMB3_Noon_Orders'},axis = 1)
Comparison.drop([('Delivery','Noon_Caps'),('Link','Noon_Caps')], axis=1, inplace=True)
Comparison['AVG_Taxi_DPMB3']   = Comparison ['DPMB3_Taxi_Rides'] / Comparison['DPMB3_Caps']
Comparison['AVG_Orders_DPMB3'] = Comparison ['DPMB3_Orders'] / Comparison['DPMB3_Caps']
Comparison





