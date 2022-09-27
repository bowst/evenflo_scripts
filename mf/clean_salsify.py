# Script for cleaning orders exported from Salsify
# The main goal is to combine orders for customers and map them to the overall customer fields

import pandas as pd
import numpy as np
import datetime
from progress.bar import Bar

salsify = pd.read_csv('salsify.csv')

# remove customers with '-- PII Redacted --'
salsify_shared = salsify[salsify['buyer_name'] != '-- PII Redacted --']

customers = pd.DataFrame(columns=[
    'First Name',
    'Last Name',
    'Email',
    'Phone',
    'Address 1',
    'Address 2',
    'City',
    'State',
    'Zip',
    'Source',
    'Family Role',
    'No. of Children',
    'Child 1 DOB',
    'Child 2 DOB',
    'Child 3 DOB',
    'Child 4 DOB',
    'Child 5 DOB',
    'No. of Orders',
    'No. of Products',
    'Products Purchased',
    'No. of Products Registered',
    'Products Registered',
    'No. of Reviews',
    'Reviewed Products',
    'Average Score',
    'Codes Used'
    ])

# create new registration db to group registrations with the same email address first

bar = Bar('Cleaning Salsify...', max=len(salsify_shared))

for i in salsify_shared.index:

    first_name = ""
    last_name = ""
    phone = ""
    address1 = ""
    address2 = ""
    city = ""
    state = ""
    zip = ""
    source = ""
    family_role = ""
    no_children = ""
    child_1_dob = ""
    child_2_dob = ""
    child_3_dob = ""
    child_4_dob = ""
    child_5_dob = ""
    no_orders = ""
    no_products = ""
    products = ""
    no_registrations = ""
    products_registered = ""
    no_reviews = ""
    products_reviewed = ""
    avg_score = ""
    codes_used = ""

    dupes = salsify_shared[salsify_shared['buyer_name'] == salsify_shared.loc[i,'buyer_name']]
    # print(i)
    # print(salsify_shared.loc[i,'buyer_name'])
    # print(dupes['buyer_name'])
    ind = list(dupes.index.values)
    products = salsify.loc[dupes.index.values, 'seller_product_id'].array
    no_products = len(products)
    orders = salsify.loc[dupes.index.values, 'order_id']
    orders.drop_duplicates(inplace=True)
    no_orders = len(orders)

    # print(ind)

    if ind[0] >= i:

        buyer = salsify_shared.loc[i,'buyer_name'].split()
        first_name = buyer[0]
        last_name = buyer[len(buyer)-1]
        email = salsify_shared.loc[i,'buyer_email']
        phone = salsify_shared.loc[i,'phone']
        source = salsify_shared.loc[i,'channel_type']

        if salsify_shared.loc[i,'buyer_name'] == salsify_shared.loc[i,'recipient_name']:
            address1 = salsify_shared.loc[i,'address_line_1']
            address2 = salsify_shared.loc[i,'address_line_2']
            city = salsify_shared.loc[i,'city']
            state = salsify_shared.loc[i,'state']
            zip = salsify_shared.loc[i,'postal_code']
            family_role = 'Parent'
        else:
            family_role = 'Gift Giver'

        customer = pd.DataFrame({
            'First Name' : [first_name],
            'Last Name' : [last_name],
            'Email' : [email],
            'Phone' : [phone],
            'Address 1' : [address1],
            'Address 2' : [address2],
            'City' : [city],
            'State' : [state],
            'Zip' : [zip],
            'Source' : [source],
            'Family Role' : [family_role],
            'No. of Children' : [no_children],
            'Child 1 DOB' : [child_1_dob],
            'Child 2 DOB' : [child_2_dob],
            'Child 3 DOB' : [child_3_dob],
            'Child 4 DOB' : [child_4_dob],
            'Child 5 DOB' : [child_5_dob],
            'No. of Orders' : [no_orders],
            'No. of Products' : [no_products],
            'Products Purchased' : [', '.join(products)],
            'No. of Products Registered' : [no_registrations],
            'Products Registered' : [products_registered],
            'No. of Reviews' : [no_reviews],
            'Reviewed Products' : [products_reviewed],
            'Average Score' : [avg_score],
            'Codes Used' : [codes_used]
            })
        customers = pd.concat([customers, customer], ignore_index=True)

    bar.next()

customers.to_csv("salsify_clean.csv")