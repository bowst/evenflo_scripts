# Script for cleaning registration export from Evenflo
# The main goal is to combine registrations for customers and attach products to them

import pandas as pd
import numpy as np
from datetime import date
from progress.bar import Bar

reg = pd.read_csv('registrations.csv')
catalog = pd.read_csv('products.csv')

reg['FirstName'] = reg['FirstName'].fillna('missing')
reg['LastName'] = reg['LastName'].fillna('missing')
reg['ModelNum'] = reg['ModelNum'].fillna('missing')
reg['ModelName'] = reg['ModelName'].fillna('missing')
reg['DOP'] = reg['DOP'].fillna('missing')

# create lists of just the names for comparison
reg_names = pd.concat([reg['FirstName'].str.lower(), reg['LastName'].str.lower()], axis=1)
reg_names['FirstName'] = reg_names['FirstName'].str.strip()
reg_names['LastName'] = reg_names['LastName'].str.strip()
reg_names = reg_names.fillna('missing')
reg_names = reg_names['FirstName'] + ' ' + reg_names['LastName']
reg_emails = reg[' '].dropna()

# create new registration db to group registrations with the same email address first
customers = pd.DataFrame(columns=['First Name', 'Last Name', 'Email', 'Address1', 'Address2', 'City', 'Zip', 'No. of Registrations', 'Registered Products'])

for x in reg_emails.index:
    dupes = reg_emails[reg_emails == reg_emails.loc[x]]
    ind = list(dupes.index.values)
    products = reg.loc[dupes.index.values, 'ModelNum'].array
    if ind[0] >= x:
        customer = pd.DataFrame({'First Name': [reg.loc[x,'FirstName']], 'Last Name': [reg.loc[x,'LastName']], 'Email': [reg.loc[x,' ']], 'Address1': [reg.loc[x,'Address1']], 'Address2': [reg.loc[x,'Address2']], 'City': [reg.loc[x,'City']], 'Zip': [reg.loc[x,'Zip']], 'No. of Registrations': [len(dupes)], 'Registered Products': [', '.join(products)]})
        customers = pd.concat([customers, customer], ignore_index=True)

# append to new registration db registrations for people with the same name and zip/address
reg_names_only = reg_names.drop(reg_emails.index.values)

## used for testing purposes only to build a separate dataframe to ensure integrity before combining with the previous dataframe
# reg_customers = pd.DataFrame(columns=['First Name', 'Last Name', 'Email', 'Address1', 'Address2', 'City', 'Zip', 'No. of Registrations', 'Registered Products'])

for x in reg_names_only.index:
    dupes = reg_names_only[reg_names_only == reg_names_only.loc[x]]
    ind_x = list(dupes.index.values)
    matches = reg.loc[dupes.index.values]
    if ind_x[0] >= x:
        for y in dupes.index:
            zip_dupes = dupes[matches['Zip'] == matches.loc[y, 'Zip']]
            ind_y = list(zip_dupes.index.values)
            products = reg.loc[zip_dupes.index.values, 'ModelNum'].array
            if (ind_y[0] >= y) and (len(zip_dupes)):
                customer = pd.DataFrame({'First Name': [reg.loc[y,'FirstName']], 'Last Name': [reg.loc[y,'LastName']], 'Email': [reg.loc[y,' ']], 'Address1': [reg.loc[y,'Address1']], 'Address2': [reg.loc[y,'Address2']], 'City': [reg.loc[y,'City']], 'Zip': [reg.loc[y,'Zip']], 'No. of Registrations': [len(zip_dupes)], 'Registered Products': [', '.join(products)]})
                # print ('Name: %s | Zip: %s | Registrations: %i' % (reg_names.loc[x],matches.loc[y, 'Zip'],len(zip_dupes)))
                customers = pd.concat([customers, customer], ignore_index=True)

customers.to_csv("registrations_clean.csv")


# how do we incorporate registration date for someone who has registered more than one product?
# what do we use to determine the buy age?
# the most recent?
# do we compare the product registered with the previous registered product and date to see if they align with the timeline?

# reg_us = reg[reg['Country'] == 'United States']
# reg_us.dropna(subset=['Source'],inplace=True)
# reg_us_evenflo = reg_us[reg_us['Source'].str.contains('Evenflo|car-seat')]

# for i in reg_us_evenflo.index:
#     prod_match = catalog[catalog['Variant SKU'].isin([reg_us_evenflo.loc[i,'ModelNum']])]
#     print(prod_match)


