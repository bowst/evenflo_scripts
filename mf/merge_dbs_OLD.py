# Script for merging customer data from Listrak, Shopify, Registrations, and the Catalog
# The main goal is to merge together all of these data sources and fill in gaps between them all

import pandas as pd
import numpy as np
import datetime

start_date = '2021-04-01'
end_date = '2022-03-01'
timespan = 'April 2021 – March 2022'

cust = pd.read_csv("shopify+listrak_" + start_date + "_" + end_date + ".csv")
reg = pd.read_csv('registrations.csv')
prod = pd.read_csv('products.csv')
listrak = pd.read_csv('listrak_421_322.csv')
# sfcc = pd.read_csv('sfcc.csv')

# fill nan values with 'missing' to avoid errors
cust['First Name'] = cust['First Name'].fillna('missing')
cust['Last Name'] = cust['Last Name'].fillna('missing')

reg['FirstName'] = reg['FirstName'].fillna('missing')
reg['LastName'] = reg['LastName'].fillna('missing')
reg['ModelNum'] = reg['ModelNum'].fillna('missing')
reg['ModelName'] = reg['ModelName'].fillna('missing')

# create lists of just the names for comparison
cust_names = pd.concat([cust['First Name'].str.lower(), cust['Last Name'].str.lower()], axis=1)
cust_names['First Name'] = cust_names['First Name'].str.strip()
cust_names['Last Name'] = cust_names['Last Name'].str.strip()

reg_names = pd.concat([reg['FirstName'].str.lower(), reg['LastName'].str.lower()], axis=1)
reg_names['FirstName'] = reg_names['FirstName'].str.strip()
reg_names['LastName'] = reg_names['LastName'].str.strip()
reg_names = reg_names.fillna('missing')

# # find duplicates in registration file based on FirstName and LastName only
# # ignores that there may be two people with the same name, but different email and physical addresses
# matching_i = []
# skip = []
# i = 0
# j = 0


# while i < len(reg):
#     if skip.count(i) == 0:
#         while j < len(reg):
#             if matching_i.count(j) == 0:
#                 if [reg.loc[i, 'FirstName'], reg.loc[i, 'LastName']] == [reg.loc[j, 'FirstName'], reg.loc[j, 'LastName']]:
#                     matching_i.append(j)
#                     skip.append(j)
#             j = j + 1
#         if len(matching_i) > 1:
#             print('%s %s matches: %a' % (reg.loc[i, 'FirstName'], reg.loc[i, 'LastName'], matching_i))
#     i = i + 1
#     j = 0
#     matching_i = []

# # append registration information to customer database
# matches = []
# skip = []
# i = 0
# j = 0

# while i < len(cust_names):
#     while j < len(reg_names):
#         if matches.count(j) == 0:
#             if [cust_names.loc[i, 'First Name'], cust_names.loc[i, 'Last Name']] == [reg_names.loc[j, 'FirstName'], reg_names.loc[j, 'LastName']]:
#                 matches.append(j)
#                 skip.append(j)
#         j = j + 1
#     cust.loc[i, 'No. of Registrations'] = len(matches)
#     if len(matches):
#         print('%s %s matches: %a' % (cust.loc[i, 'First Name'], cust.loc[i, 'Last Name'], len(matches)))
#         print('%s vs %s' % (cust.loc[i, 'Email'], reg.loc[matches, ' ']))
#     models = reg.loc[matches, 'ModelNum'].convert_dtypes()
#     products = ', '.join(models.array)
#     cust.loc[i, 'Registered Products'] = products
#     if cust.loc[i, 'Address1'] == '':
#         cust.loc[i, 'Address1'] = reg.loc[matches[0], 'Address1']
#         cust.loc[i, 'Address1'] = reg.loc[matches[0], 'Address2']
#         cust.loc[i, 'City'] = reg.loc[matches[0], 'City']
#         cust.loc[i, 'Province'] = reg.loc[matches[0], 'State']
#         cust.loc[i, 'Zip'] = reg.loc[matches[0], 'Zip']
#     i = i + 1
#     j = 0
#     matches = []

cust.to_csv("shopify+listrak+registrations_" + start_date + "_" + end_date + ".csv")


cust_names = cust_names['First Name'] + ' ' + cust_names['Last Name']
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


for i in customers.index:
    match = d_listrak[d_listrak['Email'] == customers.loc[i,'Email']]
    if len(match):
        print('Match Found')


for i in cust.index:
    email = reg[reg[' '] == cust.loc[i, 'Email']]
    names = reg[reg_names == cust_names.loc[i]]
    if len(email):
        products = ', '.join(email['ModelNum'].array)
        products_name = ', '.join(names['ModelNum'].array)
        nums = ', '.join(email['ModelNum'].array)
        nums_name = ', '.join(names['ModelNum'].array)
        alias = ', '.join(reg_names[reg_names == cust_names.loc[i]].array)
        addresses = ', '.join(names['Address1'].array)
        zips = ', '.join(names['Zip'].array)
        cust.loc[i, 'No. of Registrations'] = len(email)
        cust.loc[i, 'Registered Products'] = products
        # add_products = names[email['ModelName'] != names['ModelName']]
        # add_products = ', '.join(add_products['ModelName'].array)
        print('%s %s' % (cust.loc[i, 'First Name'], cust.loc[i, 'Last Name']))
        print('   > matches by email: %a' % (len(email)))
        print('   > matches by name: %i' % (alias.count(',') + 1))
        print('   > addresses by name: %a' % (addresses))
        print('   > product names by email: %a' % (products))
        print('   > product numbers by email: %a' % (nums))
        print('   > product names by name: %a' % (products_name))
        print('   > product numbers by name: %a' % (nums_name))
        if len(names) > len(email):
            shop_zip = cust.loc[i, 'Zip']
            shop_address = cust.loc[i, 'Address1']
            shop_number = shop_address.split()
            if names[names['Zip'] != shop_zip]:
                new_customer = names[names['Zip'] != shop_zip]
                print('Unmatched Zip Code. Adding new customer to database')


# we want to add the below 2 data points to every customer
# to get these, we need to translate purchasing, registration, and data from customers in order to determine them
# life phase is really just a range 0-10
# buy phase products are the categories of products that the customer could be targeted with based on the life phase

'Buy Phase Products'
'Life Phase'

# first find listrak contacts that have provided Child Bday's\Child-DOB-1 through -5