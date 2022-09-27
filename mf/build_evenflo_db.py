import pandas as pd
import numpy as np
import datetime

# d_shopify = pd.read_csv('shopify_2021_sample.csv')
d_shopify_1 = pd.read_csv('shopify data/customers_export_1.csv')
d_shopify_2 = pd.read_csv('shopify data/customers_export_2.csv')
d_shopify_3 = pd.read_csv('shopify data/customers_export_3.csv')
d_shopify_4 = pd.read_csv('shopify data/customers_export_4.csv')
d_shopify_5 = pd.read_csv('shopify data/customers_export_5.csv')
# d_listrak = pd.read_csv('listrak_subscribed.csv')
d_listrak = pd.read_csv('listrak_421_322.csv')
d_friendbuy = pd.read_csv('friendbuy.csv')
d_bazaar = pd.read_csv('bazaarvoice.csv')

start_date = '2021-04-01'
end_date = '2022-03-01'
timespan = 'April 2021 – March 2022'

shopify = [d_shopify_1, d_shopify_2, d_shopify_3, d_shopify_4, d_shopify_5]
d_shopify = pd.concat(shopify, ignore_index=True)

# filter based on time
shopify_timespan = d_shopify

d_listrak["Subscribe Date (UTC-04)"] = pd.to_datetime(d_listrak["Subscribe Date (UTC-04)"])
listrak_timespan = d_listrak[d_listrak["Subscribe Date (UTC-04)"] < end_date] 
listrak_timespan = listrak_timespan[listrak_timespan["Subscribe Date (UTC-04)"] >= start_date]

d_friendbuy["Captured On"] = pd.to_datetime(d_friendbuy["Captured On"])
friendbuy_timespan = d_friendbuy[d_friendbuy["Captured On"] < end_date] 
friendbuy_timespan = friendbuy_timespan[friendbuy_timespan["Captured On"] >= start_date]

# bazaar_sorted = d_bazaar.sort_values(by="Initial Publish Date")
d_bazaar["Initial Publish Date"] = pd.to_datetime(d_bazaar["Initial Publish Date"])
bazaar_timespan = d_bazaar[d_bazaar["Initial Publish Date"] < end_date] 
bazaar_timespan = bazaar_timespan[bazaar_timespan["Initial Publish Date"] >= start_date]

# matches = []

print('Looking for matches during ' + timespan + '...')
print('Shopify Items: %d' % (len(shopify_timespan)))
print('Listrak Items: %d' % (len(listrak_timespan)))
print('Friendbuy Items: %d' % (len(friendbuy_timespan)))
print('Bazaarvoice Items: %d' % (len(bazaar_timespan)))

#reduce 

# # for x in d_listrak.index:
# for x in shopify_timespan.index:
#     for y in listrak_timespan.index:
#         i = 0
#         if shopify_timespan.loc[x, "Email"] == listrak_timespan.loc[y, "Email"]:
#         # if d_listrak.loc[x, "Email"] == d_friendbuy.loc[y, "Email Address"]:
#         # if d_shopify.loc[x, "Email"] == d_friendbuy.loc[y, "Email Address"]:
#             matches.append(shopify_timespan.loc[x, "Email"])
#             print("(%d/%d) Match Found! %s %s" % (x,len(shopify_timespan),shopify_timespan.loc[x, "First Name"],shopify_timespan.loc[x, "Last Name"]))
#             i += 1

matches = listrak_timespan[listrak_timespan["Email"].isin(shopify_timespan["Email"])]
matches = matches["Email"]
print("%d matches found" % (len(matches)))
# matches_df = pd.DataFrame({"Email":matches})
# print(matches.to_string())
# print(matches_df.loc[0].to_string())
# matches_df.drop(columns=['Unnamed: 0'], inplace = True)

matches.to_csv("matches_" + start_date + "_" + end_date + ".csv")

matches = cust[cust["Last Name"].isin(reg["LastName"])]

# trim DBs to the profiles that match between both DBs
shop_matches = shopify_timespan[shopify_timespan["Email"].isin(matches)]
list_matches = listrak_timespan[listrak_timespan["Email"].isin(matches)]
friend_matches = friendbuy_timespan[friendbuy_timespan["Email Address"].isin(matches)]
bazaar_matches = bazaar_timespan[bazaar_timespan["User Email Address"].isin(matches)]

print('Shopify x Listrak Matches: %d' % (len(shop_matches)))
print('Shopify x FriendBuy Matches: %d' % (len(friend_matches)))
print('Shopify x BazaarVoice Matches: %d' % (len(bazaar_matches)))

friend_matches.to_csv("friend_matches_" + start_date + "_" + end_date + ".csv")
bazaar_matches.to_csv("bazaar_matches_" + start_date + "_" + end_date + ".csv")

# sort DBs by Email address
shop_sorted = shop_matches.sort_values(by=['Email'])
list_sorted = list_matches.sort_values(by=['Email'])

# clean up date/time values
list_sorted["Subscribe Date (UTC-04)"] = pd.to_datetime(list_sorted["Subscribe Date (UTC-04)"])
list_sorted["Last Open Date (UTC-04)"] = pd.to_datetime(list_sorted["Last Open Date (UTC-04)"])
list_sorted["Last Read Date (UTC-04)"] = pd.to_datetime(list_sorted["Last Read Date (UTC-04)"])
list_sorted["Last Click Date (UTC-04)"] = pd.to_datetime(list_sorted["Last Click Date (UTC-04)"])
list_sorted["Last Send Date (UTC-04)"] = pd.to_datetime(list_sorted["Last Send Date (UTC-04)"])

# reset the index so they align with each other
shop_sorted.reset_index(inplace = True)
list_sorted.reset_index(inplace = True)

# # compare DBs to see what duplicate columns there are, unfortunately, the titles of columns may not line up
# # shop_trimmed.columns.intersection(list_trimmed.columns)

# remove duplicate or unnecessary columns
# list_trimmed = list_sorted.drop(columns = ["Email", "Country", "Region", "Postal Code", "Subscribe Date (UTC-04).1", "Email Key", "Universal Email Key (SHA256)"])
list_trimmed = list_sorted.drop(columns = ["Email"])

shop_list = pd.concat([shop_sorted, list_trimmed.reset_index()], axis=1)

# export to csv
shop_list.to_csv("shopify+listrak_" + start_date + "_" + end_date + ".csv")