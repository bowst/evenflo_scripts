# Script for cleaning orders export from Evenflo
# The main goal is to combine orders for purchasers, pull out recipients as new customers, and attach products to each customer type

import pandas as pd
import numpy as np
from datetime import date
from progress.bar import Bar

# original file name was pnl_d2c_d2m
# columns to ignore COMPANY, WAREHOUSE, WHS_CITY, ACCOUNT_NO, DW_ORDER, BAAN_ORDER, LINE_NO, ITEM_NO, ITEMCLASS_DESC, SCAC, SERVICE_LEVEL, UNIT_STD_COST, DW_GROSS_PRICE, BAAN_PRICE_LESS_DISCS, INV_AMOUNT, LINE_DISC_UNIT, ORD_DISC_UNIT, TAX_AMT, ORDER_PROMO_ID, SHIPPING_PRICE, SHIP_PRICEADJ_PROMOID, SHIPLINE_GROSS_PROMO_PRICE, LINE_PRICEADJ_PROMOID, ORDLINE_GROSS_PROMO_PRICE, TRACKING_NO, SOLDTO_NAME2

orders = pd.read_csv('orders2.csv')
catalog = pd.read_csv('products.csv')

# remove all lines referring to freight in ITEM DESCRIPTION
no_freight = orders[orders['ITEM_DESCRIPTION'] != '"Freight added to Invoice      "']

# disregard BRANDS = EFI, GoodBaby Silver, BRU Private Label, Cybex
include_brands = 'Evenflo|Gold|Exersaucer|Urbini|Rollplay'
orders_evenflo = no_freight[no_freight['BRAND'].str.contains(include_brands)]

# disregard CATFAM_DESCR = Replacement Parts?
orders_no_parts = orders_evenflo[orders_evenflo['CATFAM_DESCR'] != '"Replacement Parts             "']

# clean up customers
# strip CONSUMER_NAME and SOLDTO_NAME of unnecessary spaces and ""
orders_no_parts['CONSUMER_NAME'] = orders_no_parts['CONSUMER_NAME'].str.strip('" "')
orders_no_parts['CONSUMER_NAME'] = orders_no_parts['CONSUMER_NAME'].str.title()
orders_no_parts['SOLDTO_NAME'] = orders_no_parts['SOLDTO_NAME'].str.strip('" "')
orders_no_parts['SOLDTO_NAME'] = orders_no_parts['SOLDTO_NAME'].str.title()

# clean up emails
# strip CONSUMER_NAME and SOLDTO_NAME of unnecessary spaces and ""
orders_no_parts['SOLDTO_EMAIL'] = orders_no_parts['SOLDTO_EMAIL'].str.strip('" "')
orders_no_parts['SOLDTO_EMAIL'] = orders_no_parts['SOLDTO_EMAIL'].str.lower()

# clean up phone numbers
# strip CONSUMER_PHONE and SOLDTO_PHONE
orders_no_parts['CONSUMER_PHONE'] = orders_no_parts['CONSUMER_PHONE'].str.strip('" "')
orders_no_parts['SOLDTO_PHONE'] = orders_no_parts['SOLDTO_PHONE'].str.strip('" "')

# clean up addresses
# strip of unnecessary spaces and ""
# CONSUMER_NAME2 is actually more like CONSUMER_ADDR2
orders_no_parts['CONSUMER_ADDR'] = orders_no_parts['CONSUMER_ADDR'].str.strip('" "')
orders_no_parts['CONSUMER_NAME2'] = orders_no_parts['CONSUMER_NAME2'].str.strip('" "')
orders_no_parts['CONSUMER_CITY'] = orders_no_parts['CONSUMER_CITY'].str.strip('" "')
orders_no_parts['CONSUMER_STATE'] = orders_no_parts['CONSUMER_STATE'].str.strip('" "')

orders_no_parts['SOLDTO_ADDR'] = orders_no_parts['SOLDTO_ADDR'].str.strip('" "')
orders_no_parts['SOLDTO_NAME2'] = orders_no_parts['SOLDTO_NAME2'].str.strip('" "')
orders_no_parts['SOLDTO_CITY'] = orders_no_parts['SOLDTO_CITY'].str.strip('" "')
orders_no_parts['SOLDTO_STATE'] = orders_no_parts['SOLDTO_STATE'].str.strip('" "')

# clean up products
orders_no_parts['ITEM_NO'] = orders_no_parts['ITEM_NO'].str.strip('" "')

# convert all zips to strings to avoid breaking when it finds a "number"
orders_no_parts['SOLDTO_ZIP'] = orders_no_parts['SOLDTO_ZIP'].convert_dtypes()
orders_no_parts['CONSUMER_ZIP'] = orders_no_parts['CONSUMER_ZIP'].convert_dtypes()

# create new customers db with the same email address first
customers = pd.DataFrame(columns=[
    'First Name',
    'Last Name',
    'Email',
    'Phone',
    'Source',
    'Address 1',
    'Address 2',
    'City',
    'State',
    'Zip',
    'No. of Orders',
    'No. of Products',
    'Products',
    'Family Role'
    ])

# for testing, disable for full run
# test_run = orders_no_parts.index
# run = test_run[0:50]
# bar = Bar('Cleaning Orders...', max=len(run))

bar = Bar('Cleaning Orders...', max=len(orders_no_parts))

# for x in run:
for x in orders_no_parts.index:

    # create empty data for each line in case there is nothing for it
    first_name = ''
    last_name = ''
    email = ''
    phone = ''
    source = ''
    address1 = ''
    address2 = ''
    city = ''
    state = ''
    zip = ''
    products = ''
    role = ''

    # generate a list of duplicate customers based on SOLDTO_NAME
    dupes = orders_no_parts[orders_no_parts['SOLDTO_NAME'] == orders_no_parts.loc[x,'SOLDTO_NAME']]
    ind = list(dupes.index.values)
    products = orders_no_parts.loc[dupes.index.values, 'ITEM_NO'].array
    no_products = len(products)

    # from the matching SOLDTO_NAME, find all of the associated order numbers and drop the duplicates to determine how many orders they made
    order_ids = orders_no_parts.loc[dupes.index.values, 'DW_ORDER']
    no_orders = len(order_ids.drop_duplicates())

    # this loop is for ensuring we don't loop over duplicate email addresses, so it looks at the first index of the dupes list and if x (the indexer) is larger than it, the loop will not run 
    if ind[0] >= x:

        # store source
        if 'EBAY' in orders_no_parts.loc[x,'ACCOUNT_NAME']:
            source = 'eBay'
        elif 'FACEBOOK' in orders_no_parts.loc[x,'ACCOUNT_NAME']:
            source = 'Facebook'
        elif 'Amazon' in orders_no_parts.loc[x,'ACCOUNT_NAME']:
            source = 'Amazon'
        elif 'Parent Link' in orders_no_parts.loc[x,'ACCOUNT_NAME']:
            source = 'Parent Link'
        elif 'GOOGLE' in orders_no_parts.loc[x,'ACCOUNT_NAME']:
            source = 'Google'
        else:
            source = 'Evenflo (Shopify)'

        # check to see if the CONSUMER_NAME and SOLDTO_NAME are not the same
        # if they are not, then we need to store the consumer as a separate customer
        if orders_no_parts.loc[x,'CONSUMER_NAME'] != orders_no_parts.loc[x,'SOLDTO_NAME']:

            name = orders_no_parts.loc[x,'CONSUMER_NAME']
            name = name.title()
            name = name.split(' ',1)
            if len(name) > 1:
                first_name = name[0]
                # making the assumption that anything after the first space is considered the last name
                # we may need to consider how this works when comparing to existing database – probably want to use the name from another source as the source of truth
                last_name = name[1]
            else:
                first_name = name[0]

            # the consumer doesn't have an email address
            email = ''

            # store phone
            phone = orders_no_parts.loc[x,'CONSUMER_PHONE']
            if (len(phone) > 0) and (phone[0].isdigit()):
                phone_normalized = ''
                if (len(phone) == 9) or (len(phone) == 10):
                    phone_normalized = phone
                else:
                    j = 0
                    while j < len(phone):
                        if phone[j].isdigit():
                            phone_normalized += phone[j]
                        j += 1
            elif (len(phone) > 0) and (phone[0].isdigit() == False):
                if (phone[0] == '+') or (phone[0] == '('):
                    j = 0
                    phone_normalized = ''
                    while j < len(phone):
                        # only include numbers, no special characters
                        if phone[j].isdigit():
                            phone_normalized += phone[j]
                        j += 1

            # store address
            address1 = orders_no_parts.loc[x,'CONSUMER_ADDR']
            address2 = orders_no_parts.loc[x,'CONSUMER_NAME2']
            city = orders_no_parts.loc[x,'CONSUMER_CITY']
            state = orders_no_parts.loc[x,'CONSUMER_STATE']

            # zip codes are sometimes more than 5 digits and other times they are less than because the computer sees a leading zero and removes it, so we need to clean them up a bit
            if (orders_no_parts.loc[x,'CONSUMER_ZIP'] is not pd.NA) and (len(orders_no_parts.loc[x,'CONSUMER_ZIP']) > 5):
                zip = orders_no_parts.loc[x,'CONSUMER_ZIP'][0:5]
            elif (orders_no_parts.loc[x,'CONSUMER_ZIP'] is not pd.NA) and (len(orders_no_parts.loc[x,'CONSUMER_ZIP']) < 5):
                zip = '0' + orders_no_parts.loc[x,'CONSUMER_ZIP']
            else:
                zip = orders_no_parts.loc[x,'CONSUMER_ZIP']

            customer = pd.DataFrame({
                'First Name' : [first_name],
                'Last Name' : [last_name],
                'Email' : [email],
                'Phone' : [phone_normalized],
                'Source' : [source],
                'Address 1' : [address1],
                'Address 2' : [address2],
                'City' : [city],
                'State' : [state],
                'Zip' : ['"' + zip + '"'],
                'No. of Orders' : [0],
                'No. of Products' : [no_products],
                'Products' : [', '.join(products)],
                'Family Role' : [role]
                })
            customers = pd.concat([customers, customer], ignore_index=True)

            # reset certain variables when SOLDTO_NAME who doesn't match CONSUMER_NAME after CONSUMER has been stored
            no_products = 0
            role = 'Gift Giver'

        # since SOLDTO_NAME is only one field, we need to split the name into first and last
        name = orders_no_parts.loc[x,'SOLDTO_NAME'].strip('" "')
        name = name.title()
        name = name.split(' ',1)
        if len(name) > 1:
            first_name = name[0]
            # making the assumption that anything after the first space is considered the last name
            # we may need to consider how this works when comparing to existing database – probably want to use the name from another source as the source of truth
            last_name = name[1]
        else:
            first_name = name[0]

        # WHAT TO DO ABOUT NULL@CYBERSOURCE EMAILS?
        if 'null' in orders_no_parts.loc[x,'SOLDTO_EMAIL']:
            email = ''
        else:
            email = orders_no_parts.loc[x,'SOLDTO_EMAIL']

        # store phone
        phone = orders_no_parts.loc[x,'SOLDTO_PHONE']
        if (len(phone) > 0) and (phone[0].isdigit()):
            phone_normalized = ''
            if (len(phone) == 9) or (len(phone) == 10):
                phone_normalized = phone
            else:
                j = 0
                while j < len(phone):
                    if phone[j].isdigit():
                        phone_normalized += phone[j]
                    j += 1
        elif (len(phone) > 0) and (phone[0].isdigit() == False):
            if (phone[0] == '+') or (phone[0] == '('):
                j = 0
                phone_normalized = ''
                while j < len(phone):
                    # only include numbers, no special characters
                    if phone[j].isdigit():
                        phone_normalized += phone[j]
                    j += 1

        # if the SOLDTO_NAME and CONSUMER_NAME are the same then store the following
        # store address
        address1 = orders_no_parts.loc[x,'SOLDTO_ADDR']
        address2 = orders_no_parts.loc[x,'SOLDTO_NAME2']
        city = orders_no_parts.loc[x,'SOLDTO_CITY']
        state = orders_no_parts.loc[x,'SOLDTO_STATE']

        # zip codes are sometimes more than 5 digits and other times they are less than because the computer sees a leading zero and removes it, so we need to clean them up a bit
        if (orders_no_parts.loc[x,'SOLDTO_ZIP'] is not pd.NA) and (len(orders_no_parts.loc[x,'SOLDTO_ZIP']) > 5):
            zip = orders_no_parts.loc[x,'SOLDTO_ZIP'][0:5]
        elif (orders_no_parts.loc[x,'SOLDTO_ZIP'] is not pd.NA) and (len(orders_no_parts.loc[x,'SOLDTO_ZIP']) < 5):
            zip = '0' + orders_no_parts.loc[x,'SOLDTO_ZIP']
        else:
            zip = orders_no_parts.loc[x,'SOLDTO_ZIP']

        customer = pd.DataFrame({
            'First Name' : [first_name],
            'Last Name' : [last_name],
            'Email' : [email],
            'Phone' : [phone_normalized],
            'Source' : [source],
            'Address 1' : [address1],
            'Address 2' : [address2],
            'City' : [city],
            'State' : [state],
            'Zip' : ['"' + zip + '"'],
            'No. of Orders' : [no_orders],
            'No. of Products' : [no_products],
            'Products' : [', '.join(products)],
            'Family Role' : [role]
            })
        customers = pd.concat([customers, customer], ignore_index=True)
    
    bar.next()

today = date.today()
date_str = today.strftime("%Y_%m_%d")

customers.to_csv("orders_clean_" + date_str + ".csv")
bar.finish()


