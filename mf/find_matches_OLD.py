import pandas as pd
import numpy as np

d_shopify = pd.read_csv('shopify_subscribed.csv')
d_listrak = pd.read_csv('listrak.csv')
d_friendbuy = pd.read_csv('friendbuy.csv')

results = []

print('Looking for matches...')

# for x in d_listrak.index:
for x in d_shopify.index:
    for y in d_listrak.index:
    # for y in d_friendbuy.index:
        i = 0
        if d_shopify.loc[x, "Email"] == d_listrak.loc[y, "Email"]:
        # if d_listrak.loc[x, "Email"] == d_friendbuy.loc[y, "Email Address"]:
        # if d_shopify.loc[x, "Email"] == d_friendbuy.loc[y, "Email Address"]:
            results = i + 1

print("%d matches found" % (i))