# This script is meant to ingest data from the Evenflo product catalog and assign use and buy ages to each category of products

import pandas as pd
import numpy as np

catalog = pd.read_csv('products.csv')

# parse catalog handle for each product using the categories here

cat_age = {'category': [
    'infant-car-seat',
    'every|all4one|revolve360|symphony|tribute', # how to differentiate between 2-mode (Sonus, Stratos, Sure, and AIO? I think there are specific brands that can be used for this.
    'sonus|stratos|sure-ride|triumph',
    'booster',
    'wagon', #full size stroller – is this just ever other stroller that is not light or convertible?
    'travel-system',
    'lightweight|double', #stroller
    'jogging-stroller',
    'high-chair',
    'gate',
    'bassinet',
    'carrier',
    'exersaucer', #door jumpers and activity centers
    'crib-mattress', # (doesn't exist in current catalog from evenflo.com)
    'health', # (doesn't exist in current catalog from evenflo.com)
    'playards|play-yard'
], 'estimated age': [
    '0 - 1 yr', # infant car seat
    '0 - 4 yrs', # 2-mode convertible car seat
    '0 - 10 yrs', # AIO convertible car seat
    '5 yrs - 10 yrs', # booster
    '0 - 5 yrs', # full-size stroller
    '0 - 5 yrs', # travel system
    '4 mos - 5 yrs', # lightweight stroller
    '4 mos - 5 yrs', # jogger
    '4 mos - 3 yrs', # high chair
    '4 mos - 3 yrs', # gate
    '0 - 1 yr', # bassinet
    '0 - 1 yr', # carrier
    '4 mos - 1 yr', # exersaucer
    '0 - 3 yrs', # crib mattress
    '0 +', # health
    '0 - 2 yrs' # playards
], 'buy age': [
    'expectant - 6 mos', # infant car seat
    '6 mos - 18 mos', # 2-mode convertible car seat
    '6 mos - 18 mos', # AIO convertible car seat
    '4 yrs +', # booster
    'expectant - 6 mos', # full-size stroller
    'expectant - 1 yr', # travel system
    '1 mo - 18 mos', # lightweight stroller
    '4 mos - 2 yrs', # jogger
    'expectant - 1 yr', # high chair
    '4 mos +', # gate
    'expectant - 6 mos', # bassinet
    'expectant - 6 mos', # carrier
    'expectant - 6 mos', # exersaucer
    'expectant - 1 yr', # crib mattress
    'expectant +', # health
    'expectant - 1 yr' # playards
]}

cat_age_key = pd.DataFrame(data=cat_age)

# match registered/purchase SKU with catalog and translate to use-age and buy-age
# how do we handle this for multiple children? Attach it to CHILD N?

# update the catalog with the estimated age and corresponding buy age
for i in cat_age_key.index:
    match_index = catalog['Handle'].str.contains(cat_age_key.loc[i,'category'])
    catalog.loc[match_index, 'Estimated Age'] = cat_age_key.loc[i,'estimated age']
    catalog.loc[match_index, 'Buy Age'] = cat_age_key.loc[i,'buy age']

catalog.to_csv("catalog.csv")


