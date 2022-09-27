# Script for cleaning and merging customer data from Listrak, Shopify, Friendbuy, Bazaarvoice, Registrations, Salsify, and Appointlet
# This is meant to be the ultimate script to merge and update customers with data across all different sources
# The script is broken into pulling all the customers together into one list based on email, and then filling in the missing data
# The second part is meant to add customers that do not have emails, which may come from Registrations
# This second part is not fully working, so the script currently saves the first half, customers with emails

import pandas as pd
import numpy as np
from datetime import date
from progress.bar import Bar

# import data sources
listrak = pd.read_csv('listrak_421_322.csv')
shopify_1 = pd.read_csv('shopify data/customers_export_1.csv')
shopify_2 = pd.read_csv('shopify data/customers_export_2.csv')
shopify_3 = pd.read_csv('shopify data/customers_export_3.csv')
shopify_4 = pd.read_csv('shopify data/customers_export_4.csv')
shopify_5 = pd.read_csv('shopify data/customers_export_5.csv')
friendbuy = pd.read_csv('friendbuy_all_campaigns_2022.csv')
bazaar = pd.read_csv('reviews_clean_2022_08_18.csv')
reg = pd.read_csv('registrations_clean.csv')
salsify = pd.read_csv('salsify_clean.csv')
bookings = pd.read_csv('bookings_clean_2022_08_18.csv')

print('Data imported')

shopify_files = [shopify_1, shopify_2, shopify_3, shopify_4, shopify_5]
shopify = pd.concat(shopify_files, ignore_index=True)

emails_listrak = listrak['Email'].dropna().str.lower() # use listrak as starting point for all emails
emails_shopify = shopify['Email'].dropna().str.lower()
emails_friendbuy = friendbuy['Email Address'].dropna().str.lower()
emails_bazaar = bazaar['Email'].dropna().str.lower()
emails_reg = reg['Email'].dropna().str.lower()
emails_salsify = salsify['Email'].dropna().str.lower()
emails_bookings = bookings['Email'].dropna().str.lower()

print('Emails normalized')

emails = emails_listrak

count = len(emails)
print("Emails from Listrak DB: %i" % (count))

# loop over each data source to build the ultimate customer email list
for i in emails_shopify.index:
    matches = emails[emails == emails_shopify.loc[i]]
    if len(matches):
        # print('Shopify Match Found')
        pass
    else:
        email = pd.Series([emails_shopify.loc[i]])
        emails = pd.concat([emails, email], ignore_index=True)

print("Added emails from Shopify DB: %i" % (len(emails) - count))
count = len(emails)

for i in emails_friendbuy.index:
    matches = emails[emails == emails_friendbuy.loc[i]]
    if len(matches):
        #print('Friendbuy Match Found')
        pass
    else:
        email = pd.Series([emails_friendbuy.loc[i]])
        emails = pd.concat([emails, email], ignore_index=True)

print("Added emails from FriendBuy DB: %i" % (len(emails) - count))
count = len(emails)

for i in emails_bazaar.index:
    matches = emails[emails == emails_bazaar.loc[i]]
    if len(matches):
        # print('Bazaarvoice Match Found')
        pass
    else:
        email = pd.Series([emails_bazaar.loc[i]])
        emails = pd.concat([emails, email], ignore_index=True)

print("Added emails from Bazaarvoice DB: %i" % (len(emails) - count))
count = len(emails)

for i in emails_reg.index:
    matches = emails[emails == emails_reg.loc[i]]
    if len(matches):
        # print('Registration Match Found')
        pass
    else:
        email = pd.Series([emails_reg.loc[i]])
        emails = pd.concat([emails, email], ignore_index=True)

print("Added emails from Registration DB: %i" % (len(emails) - count))
count = len(emails)

for i in emails_salsify.index:
    matches = emails[emails == emails_salsify.loc[i]]
    if len(matches):
        # print('Registration Match Found')
        pass
    else:
        email = pd.Series([emails_salsify.loc[i]])
        emails = pd.concat([emails, email], ignore_index=True)

print("Added emails from Salsify DB: %i" % (len(emails) - count))
count = len(emails)

for i in emails_bookings.index:
    matches = emails[emails == emails_bookings.loc[i]]
    if len(matches):
        # print('Registration Match Found')
        pass
    else:
        email = pd.Series([emails_bookings.loc[i]])
        emails = pd.concat([emails, email], ignore_index=True)

print("Added emails from Appointlet DB: %i" % (len(emails) - count))

listrak.rename(columns={
    "Source\\Popup" : "Popup",
    "Source\\NewPopup" : "NewPopup",
    "Source\\ExitPopup" : "ExitPopup",
    "Source\\NewExitPopup" : "NewExitPopup",
    "Source\\Account" : "Account",
    "Source\\Checkout" : "Checkout",
    "Source\\GuestCheckout" : "GuestCheckout",
    "Source\\Facebook" : "Facebook",
    "Source\\FacebookLeadAd" : "FacebookLeadAd",
    "Source\\NewFacebookLeadAd" : "NewFacebookLeadAd",
    "Source\\Footer" : "Footer",
    "Source\\MiniForm" : "MiniForm",
    "Source\\PreferenceCenter" : "PreferenceCenter",
    "Source\\Sweepstakes" : "Sweepstakes",
    "Source\\SMS" : "SMS",
    "Source\\ContactUs" : "ContactUs",
    "Source\\Parentlink" : "Parentlink",
    "Source\\Registration Form" : "Registration Form",
    "Source\\Friendbuy" : "Friendbuy"
    }, inplace=True)

listrak_sources = [
    'Popup',
    'NewPopup',
    'ExitPopup',
    'NewExitPopup',
    'Account',
    'Checkout',
    'GuestCheckout',
    'Facebook',
    'FacebookLeadAd',
    'NewFacebookLeadAd',
    'Footer',
    'MiniForm',
    'PreferenceCenter',
    'Sweepstakes',
    'SMS',
    'ContactUs',
    'Parentlink',
    'Registration Form',
    'Friendbuy'
    ]

src = pd.Series(listrak_sources)

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
    'Child 1 Age',
    'Child 2 DOB',
    'Child 2 Age',
    'Child 3 DOB',
    'Child 3 Age',
    'Child 4 DOB',
    'Child 4 Age',
    'Child 5 DOB',
    'Child 5 Age',
    'Family Buy Age',
    'No. of Orders',
    'No. of Products Registered',
    'Products Registered',
    'Products Installed',
    'Vehicles',
    'Bookings',
    'No. of Reviews',
    'Reviewed Products',
    'Average Score',
    'Codes Used'
    ])

emails.to_csv("email_list.csv")

bar = Bar('Cleaning...', max=len(emails))

for i in emails.index:

    first_name = ""
    last_name = ""
    source = []
    phone = ""
    address1 = ""
    address2 = ""
    city = ""
    state = ""
    zip = ""
    family_role = ""
    no_children = ""
    child_1_dob = ""
    child_1_age = ""
    child_2_dob = ""
    child_2_age = ""
    child_3_dob = ""
    child_3_age = ""
    child_4_dob = ""
    child_4_age = ""
    child_5_dob = ""
    child_5_age = ""
    buy_age = ""
    no_orders = ""
    no_registrations = ""
    products_registered = ""
    products_installed = ""
    vehicles = ""
    no_bookings = ""
    no_reviews = ""
    products_reviewed = ""
    avg_score = ""
    codes_used = ""

    c_listrak = listrak[listrak['Email'].str.lower() == emails.loc[i]].reset_index(drop=True)
    c_shopify = shopify[shopify['Email'].str.lower() == emails.loc[i]].reset_index(drop=True)
    c_friendbuy = friendbuy[friendbuy['Email Address'].str.lower() == emails.loc[i]].reset_index(drop=True)
    c_bazaar = bazaar[bazaar['Email'].str.lower() == emails.loc[i]].reset_index(drop=True)
    c_reg = reg[reg['Email'].str.lower() == emails.loc[i]].reset_index(drop=True)
    c_bookings = bookings[bookings['Email'].str.lower() == emails.loc[i]].reset_index(drop=True)

    # data from listrak
    if len(c_listrak):
        email = c_listrak.loc[0,'Email']
        for j in src.index:
            if pd.notna(c_listrak.loc[0,src[j]]):
                sources = src[j]
                if sources == 'Registration Form':
                    if len(c_reg):
                        source.append('Registration')
                elif sources == 'Friendbuy':
                    if len(c_friendbuy):
                        source.append('Friendbuy')
                else:
                    source.append(src[j])
        if pd.notna(c_listrak.loc[0,'Customer Info\MobilePhone']):
            phone = c_listrak.loc[0,'Customer Info\MobilePhone']
        date_of_birth = c_listrak.loc[0,'Customer Info\DOB']
        if pd.notna(c_listrak.loc[0,"Are You\Expecting"]):
            family_role = "Expecting"
        elif pd.notna(c_listrak.loc[0,"Are You\A Parent"]):
            family_role = "Parent"
        elif pd.notna(c_listrak.loc[0,"Are You\A Gift Giver"]):
            family_role = "Gift Giver"
        if pd.notna(c_listrak.loc[0,"Child Bday's\Child-DOB-1"]):
            no_children = 1
            child_1_dob = c_listrak.loc[0,"Child Bday's\Child-DOB-1"]
            if pd.notna(c_listrak.loc[0,"Child Bday's\Child-DOB-2"]):
                no_children += 1
                child_2_dob = c_listrak.loc[0,"Child Bday's\Child-DOB-2"]
                if pd.notna(c_listrak.loc[0,"Child Bday's\Child-DOB-3"]):
                    no_children += 1
                    child_3_dob = c_listrak.loc[0,"Child Bday's\Child-DOB-3"]
                    if pd.notna(c_listrak.loc[0,"Child Bday's\Child-DOB-4"]):
                        no_children += 1
                        child_4_dob = c_listrak.loc[0,"Child Bday's\Child-DOB-4"]
                        if pd.notna(c_listrak.loc[0,"Child Bday's\Child-DOB-5"]):
                            no_children += 1
                            child_5_dob = c_listrak.loc[0,"Child Bday's\Child-DOB-5"]
        product_interests = []
        if pd.notna(c_listrak.loc[0,'Products Interested In\Car Seats']):
            product_interests.append(c_listrak.loc[0,'Products Interested In\Car Seats'])
        if pd.notna(c_listrak.loc[0,'Products Interested In\Travel Systems']):
            product_interests.append(c_listrak.loc[0,'Products Interested In\Travel Systems'])
        if pd.notna(c_listrak.loc[0,'Products Interested In\Playards and Gates']):
            product_interests.append(c_listrak.loc[0,'Products Interested In\Playards and Gates'])
        if pd.notna(c_listrak.loc[0,'Products Interested In\Stroller']):
            product_interests.append(c_listrak.loc[0,'Products Interested In\Stroller'])
        if pd.notna(c_listrak.loc[0,'Products Interested In\Jogger']):
            product_interests.append(c_listrak.loc[0,'Products Interested In\Jogger'])

    # data from shopify
    if len(c_shopify):
        email = c_shopify.loc[0,'Email']
        source.append('Shopify')
        first_name = c_shopify.loc[0,'First Name']
        last_name = c_shopify.loc[0,'Last Name']
        if pd.notna(c_shopify.loc[0,'Phone']):
            phone = c_shopify.loc[0,'Phone']
        if c_shopify.loc[0,'Country'] == 'United States':
            address1 = c_shopify.loc[0,'Address1']
            address2 = c_shopify.loc[0,'Address2']
            city = c_shopify.loc[0,'City']
            state = c_shopify.loc[0,'Province']
            zip = c_shopify.loc[0,'Zip']
        no_orders = c_shopify.loc[0,'Total Orders']

    # data from friendbuy
    if len(c_friendbuy):
        email = c_friendbuy.loc[0,'Email Address']
        source.append('Friendbuy')
        codes_used = c_friendbuy.loc[0,'Coupon Code(s)'] # if this is empty they've never used a code

    # data from bazaarvoice
    if len(c_bazaar):
        email = c_bazaar.loc[0,'Email']
        source.append(c_bazaar.loc[0,'Source'])
        no_reviews = c_bazaar.loc[0,'No. of Reviews']
        products_reviewed = c_bazaar.loc[0,'Reviewed Products']
        avg_score = c_bazaar.loc[0,'Average Score']
        role = c_bazaar.loc[0,'Family Role']

    # data from registrations
    if len(c_reg):
        email = c_reg.loc[0,'Email']
        source.append('Registration')
        if first_name:
            pass
        else:
            first_name = c_reg.loc[0,'First Name']
        if last_name:
            pass
        else:
            last_name = c_reg.loc[0,'Last Name']
        if address1:
            pass
        else:
            address1 = c_reg.loc[0,'Address1']
            address2 = c_reg.loc[0,'Address2']
            city = c_reg.loc[0,'City']
            zip = c_reg.loc[0,'Zip']
        no_registrations = c_reg.loc[0,'No. of Registrations'] 
        products_registered = c_reg.loc[0,'Registered Products']

    # data from appointlet
    if len(c_bookings):
        email = c_bookings.loc[0,'Email']
        source.append(c_bookings.loc[0,'Source'])
        if first_name:
            pass
        else:
            first_name = c_bookings.loc[0,'First Name']
        if last_name:
            pass
        else:
            last_name = c_bookings.loc[0,'Last Name']
        no_children = c_bookings.loc[0,'No. of Children']
        child_1_age = c_bookings.loc[0,'Child 1 Age']
        products_installed = c_bookings.loc[0,'Products Installed']
        vehicles = c_bookings.loc[0,'Vehicles']
        no_bookings = c_bookings.loc[0,'Bookings']

    #assemble customer
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
        'Source' : [', '.join(source)],
        'Family Role' : [family_role],
        'No. of Children' : [no_children],
        'Child 1 DOB' : [child_1_dob],
        'Child 1 Age' : [child_1_age],
        'Child 2 DOB' : [child_2_dob],
        'Child 2 Age' : [child_2_age],
        'Child 3 DOB' : [child_3_dob],
        'Child 3 Age' : [child_3_age],
        'Child 4 DOB' : [child_4_dob],
        'Child 4 Age' : [child_4_age],
        'Child 5 DOB' : [child_5_dob],
        'Child 5 Age' : [child_5_age],
        'Family Buy Age' : [buy_age],
        'No. of Orders' : [no_orders],
        'No. of Products Registered' : [no_registrations],
        'Products Registered' : [products_registered],
        'Products Installed' : [products_installed],
        'Vehicles' : [vehicles],
        'Bookings' : [no_bookings],
        'No. of Reviews' : [no_reviews],
        'Reviewed Products' : [products_reviewed],
        'Average Score' : [avg_score],
        'Codes Used' : [codes_used]
        })
    customers = pd.concat([customers, customer], ignore_index=True)
    bar.next()

today = date.today()
date_str = today.strftime("%Y_%m_%d")

customers.to_csv("customers_clean_" + date_str + ".csv")
bar.finish()

reg_no_email = reg[pd.isna(reg['Email'])]
names_reg = pd.concat([reg_no_email['First Name'].str.lower(), reg_no_email['Last Name'].str.lower()], axis=1)
names_reg['First Name'] = names_reg['First Name'].str.strip()
names_reg['Last Name'] = names_reg['Last Name'].str.strip()
names_reg = names_reg['First Name'] + ' ' + names_reg['Last Name']

names_shopify = pd.concat([shopify['First Name'].str.lower(), shopify['Last Name'].str.lower()], axis=1)
names_shopify['First Name'] = names_shopify['First Name'].str.strip()
names_shopify['Last Name'] = names_shopify['Last Name'].str.strip()
names_shopify = names_shopify['First Name'] + ' ' + names_shopify['Last Name']

bar = Bar('Adding customers without email addresses...', max=len(names_reg))

for i in names_reg.index:
    # include this here because the customers list will continue to grow and needs to be updated each time for the appropriate index
    names_customers = pd.concat([customers['First Name'].str.lower(), customers['Last Name'].str.lower()], axis=1)
    names_customers['First Name'] = names_customers['First Name'].str.strip()
    names_customers['Last Name'] = names_customers['Last Name'].str.strip()
    names_customers = names_customers['First Name'] + ' ' + names_customers['Last Name']
    # find match for name in the following lists
    match_customer = customers[names_customers == names_reg.loc[i]]
    match_shopify = shopify[names_shopify == names_reg.loc[i]]
    if len(match_customer):
        source = [customers.loc[match_customer.index.values, 'Source']]
        customers.loc[match_customer.index.values, 'Source'] = source.append('Registration')
        # if the registered address is the same as what is in shopify, then we just need to update the registration information
        address_no_customer = customers.loc[match_customer.index.values[0], 'Address 1']
        address_no_reg = reg.loc[i, 'Address1']
        # is there an address for this customer
        if (pd.notna(address_no_customer)) and (pd.notna(address_no_reg)):
            # if there is an address and the number matches, just add the registration information
            if address_no_customer.split()[0] == address_no_reg.split()[0]:
                customers.loc[match_customer.index.values[0], 'No. of Products Registered'] = reg.loc[i, 'No. of Registrations']
                customers.loc[match_customer.index.values[0], 'Products Registered'] = reg.loc[i, 'Registered Products']
            # if there is an address and the number doesn't match, add a new customer
            else:
                customer = pd.DataFrame({
                    'First Name' : [reg.loc[i,'First Name']],
                    'Last Name' : [reg.loc[i,'Last Name']],
                    'Email' : [""],
                    'Phone' : [""],
                    'Address 1' : [reg.loc[i,'Address1']],
                    'Address 2' : [reg.loc[i,'Address2']],
                    'City' : [reg.loc[i, 'City']],
                    'State' : [""],
                    'Zip' : [reg.loc[i, 'Zip']],
                    'Source' : ['Registration'],
                    'Family Role' : [""],
                    'No. of Children' : [""],
                    'Child 1 DOB' : [""],
                    'Child 1 Age' : [""],
                    'Child 2 DOB' : [""],
                    'Child 2 Age' : [""],
                    'Child 3 DOB' : [""],
                    'Child 3 Age' : [""],
                    'Child 4 DOB' : [""],
                    'Child 4 Age' : [""],
                    'Child 5 DOB' : [""],
                    'Child 5 Age' : [""],
                    'Family Buy Age' : [""],
                    'No. of Orders' : [""],
                    'No. of Products Registered' : [reg.loc[i, 'No. of Registrations']],
                    'Products Registered' : [reg.loc[i, 'Registered Products']],
                    'Products Installed' : [""],
                    'Vehicles' : [""],
                    'Bookings' : [""],
                    'No. of Reviews' : [""],
                    'Reviewed Products' : [""],
                    'Average Score' : [""],
                    'Codes Used' : [""]
                    })
                customers = pd.concat([customers, customer], ignore_index=True)    
        # if there is no address, then we need to add the customer information from the registration file
        else:
            customers.loc[match_customer.index.values[0], 'Address 1'] = reg.loc[i, 'Address1']
            customers.loc[match_customer.index.values[0], 'Address 2'] = reg.loc[i, 'Address2']
            customers.loc[match_customer.index.values[0], 'City'] = reg.loc[i, 'City']
            customers.loc[match_customer.index.values[0], 'Zip'] = reg.loc[i, 'Zip']
            customers.loc[match_customer.index.values[0], 'No. of Products Registered'] = reg.loc[i, 'No. of Registrations']
            customers.loc[match_customer.index.values[0], 'Products Registered'] = reg.loc[i, 'Registered Products'] 
    # if there's no match in the existing customers, we need to check the Shopify customers without an email address for matches
    # if there's a match by name there, then we can add a new customer with the collected information 
    elif len(match_shopify):
        address_no_shopify = shopify.loc[match_shopify.index.values[0], 'Address1']
        address_no_reg = reg.loc[i, 'Address1']
        if (pd.notna(address_no_shopify)) and (pd.notna(address_no_reg)):
            # if there is an address in shopify, we need to match it to make sure it's not a duplicate
            if address_no_shopify.split()[0] == address_no_reg.split()[0]:
                customer = pd.DataFrame({
                    'First Name' : [reg.loc[i,'First Name']],
                    'Last Name' : [reg.loc[i,'Last Name']],
                    'Email' : [""],
                    'Phone' : [""],
                    'Address 1' : [reg.loc[i,'Address1']],
                    'Address 2' : [reg.loc[i,'Address2']],
                    'City' : [reg.loc[i, 'City']],
                    'State' : [""],
                    'Zip' : [reg.loc[i, 'Zip']],
                    'Source' : ['Registration'],
                    'Family Role' : [""],
                    'No. of Children' : [""],
                    'Child 1 DOB' : [""],
                    'Child 1 Age' : [""],
                    'Child 2 DOB' : [""],
                    'Child 2 Age' : [""],
                    'Child 3 DOB' : [""],
                    'Child 3 Age' : [""],
                    'Child 4 DOB' : [""],
                    'Child 4 Age' : [""],
                    'Child 5 DOB' : [""],
                    'Child 5 Age' : [""],
                    'Family Buy Age' : [""],
                    'No. of Orders' : [shopify.loc[match_shopify.index.values,'Total Orders']],
                    'No. of Products Registered' : [reg.loc[i, 'No. of Registrations']],
                    'Products Registered' : [reg.loc[i, 'Registered Products']],
                    'Products Installed' : [""],
                    'Vehicles' : [""],
                    'Bookings' : [""],
                    'No. of Reviews' : [""],
                    'Reviewed Products' : [""],
                    'Average Score' : [""],
                    'Codes Used' : [""]
                    })
                customers = pd.concat([customers, customer], ignore_index=True)
            else:
                customer_shopify = pd.DataFrame({
                    'First Name' : [shopify.loc[match_shopify.index.values,'First Name']],
                    'Last Name' : [shopify.loc[match_shopify.index.values,'Last Name']],
                    'Email' : [""],
                    'Phone' : [""],
                    'Address 1' : [shopify.loc[match_shopify.index.values,'Address1']],
                    'Address 2' : [shopify.loc[match_shopify.index.values,'Address2']],
                    'City' : [shopify.loc[match_shopify.index.values,'City']],
                    'State' : [shopify.loc[match_shopify.index.values,'Province']],
                    'Zip' : [shopify.loc[match_shopify.index.values,'Zip']],
                    'Source' : ['Shopify'],
                    'Family Role' : [""],
                    'No. of Children' : [""],
                    'Child 1 DOB' : [""],
                    'Child 1 Age' : [""],
                    'Child 2 DOB' : [""],
                    'Child 2 Age' : [""],
                    'Child 3 DOB' : [""],
                    'Child 3 Age' : [""],
                    'Child 4 DOB' : [""],
                    'Child 4 Age' : [""],
                    'Child 5 DOB' : [""],
                    'Child 5 Age' : [""],
                    'Family Buy Age' : [""],
                    'No. of Orders' : [shopify.loc[match_shopify.index.values,'Total Orders']],
                    'No. of Products Registered' : [""],
                    'Products Installed' : [""],
                    'Vehicles' : [""],
                    'Bookings' : [""],
                    'Products Registered' : [""],
                    'No. of Reviews' : [""],
                    'Reviewed Products' : [""],
                    'Average Score' : [""],
                    'Codes Used' : [""]
                    })
                customer_reg = pd.DataFrame({
                    'First Name' : [reg.loc[i,'First Name']],
                    'Last Name' : [reg.loc[i,'Last Name']],
                    'Email' : [""],
                    'Phone' : [""],
                    'Address 1' : [reg.loc[i,'Address1']],
                    'Address 2' : [reg.loc[i,'Address2']],
                    'City' : [reg.loc[i, 'City']],
                    'State' : [""],
                    'Zip' : [reg.loc[i, 'Zip']],
                    'Source' : ['Registration'],
                    'Family Role' : [""],
                    'No. of Children' : [""],
                    'Child 1 DOB' : [""],
                    'Child 1 Age' : [""],
                    'Child 2 DOB' : [""],
                    'Child 2 Age' : [""],
                    'Child 3 DOB' : [""],
                    'Child 3 Age' : [""],
                    'Child 4 DOB' : [""],
                    'Child 4 Age' : [""],
                    'Child 5 DOB' : [""],
                    'Child 5 Age' : [""],
                    'Family Buy Age' : [""],
                    'No. of Orders' : [""],
                    'No. of Products Registered' : [reg.loc[i, 'No. of Registrations']],
                    'Products Registered' : [reg.loc[i, 'Registered Products']],
                    'Products Installed' : [""],
                    'Vehicles' : [""],
                    'Bookings' : [""],
                    'No. of Reviews' : [""],
                    'Reviewed Products' : [""],
                    'Average Score' : [""],
                    'Codes Used' : [""]
                    })
            customers = pd.concat([customers, customer_shopify, customer_reg], ignore_index=True)
        else:
            customer = pd.DataFrame({
                'First Name' : [reg.loc[i,'First Name']],
                'Last Name' : [reg.loc[i,'Last Name']],
                'Email' : [""],
                'Phone' : [""],
                'Address 1' : [reg.loc[i,'Address1']],
                'Address 2' : [reg.loc[i,'Address2']],
                'City' : [reg.loc[i, 'City']],
                'State' : [""],
                'Zip' : [reg.loc[i, 'Zip']],
                'Source' : ['Registration'],
                'Family Role' : [""],
                'No. of Children' : [""],
                'Child 1 DOB' : [""],
                'Child 1 Age' : [""],
                'Child 2 DOB' : [""],
                'Child 2 Age' : [""],
                'Child 3 DOB' : [""],
                'Child 3 Age' : [""],
                'Child 4 DOB' : [""],
                'Child 4 Age' : [""],
                'Child 5 DOB' : [""],
                'Child 5 Age' : [""],
                'Family Buy Age' : [""],
                'No. of Orders' : [shopify.loc[match_shopify.index.values,'Total Orders']],
                'No. of Products Registered' : [reg.loc[i, 'No. of Registrations']],
                'Products Registered' : [reg.loc[i, 'Registered Products']],
                'Products Installed' : [""],
                'Vehicles' : [""],
                'Bookings' : [""],
                'No. of Reviews' : [""],
                'Reviewed Products' : [""],
                'Average Score' : [""],
                'Codes Used' : [""]
                })
    bar.next()

customers.to_csv("customers_clean_all_" + date_str + ".csv")
bar.finish()  

    # date_of_purchase = []

    # family information
    # parent, expecting, gift giver (cool/fun uncle) - determine gift giver based on differing address/name?
    # family_members = []
    # today = datetime.today()
    # birthday_obj = datetime.strptime(child_1_dob, '%m/%d/%Y')
    # age = today - birthday_obj
    # if age < 0:
    #     child_1_life_st = ['Expected']

    # age = pd.Series({
    #     'Expectant' : 'Infant Car Seat, Bassinets, Carriers, Full Size Stroller, Door Jumper & Exersaucer, Playards, Crib Mattress, Travel System, High Chair',
    #     '0 - 4 mo' : 'Infant Car Seat, Bassinets, Carriers, Full Size Stroller, Door Jumper & Exersaucer, Playards, Crib Mattress, Travel System'
    #     columns = ['Age','Relevant Products']
    #     })

    # child_2_life_st = []
    # child_3_life_st = []
    # child_4_life_st = []
    # child_5_life_st = []

    # shopping information
    # buy_stage = [] # NEW could be more than one if there are multiple children
    
    # no_products = []
    # products_purchased = [] #skus

    # for marketing
    # interests = []
    # vehicle_manufacturer = []
    # vehicle_model = []
    # vehicle_year = []