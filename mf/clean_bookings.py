# Script for cleaning appointment bookings exported from Appointlet
# The main goal is to determine the customers' family size and ae of children
# This unfortunately requires a lot of assumptions and could be very complex, but tried to keep it "simple" for this initial round

import pandas as pd
import numpy as np
from datetime import date
from progress.bar import Bar

# bookings = pd.read_csv('bookings.csv')
bookings = pd.read_csv('appointlet-2020-present.csv')

# create new registration db to group bookings with the same email address first
customers = pd.DataFrame(columns=[
    'First Name',
    'Last Name',
    'Email',
    'Phone',
    'Source',
    'No. of Children',
    'Child 1 Age',
    'Child 2 Age',
    'Child 3 Age',
    'Child 4 Age',
    'Child 5 Age',
    'Products Installed',
    'Vehicle',
    'Bookings'
    ])

bar = Bar('Cleaning Bookings...', max=len(bookings))

# start looping over bookings by individual appointment
for i in bookings.index:

    # create empty data for each line in case there is nothing for it
    first_name = ""
    last_name = ""
    email = ""
    phone = ""
    source = "Appointlet"
    no_children = ""
    child_1_age = ""
    child_2_age = ""
    child_3_age = ""
    child_4_age = ""
    child_5_age = ""
    products_installed = ""
    vehicles = ""
    no_bookings = ""

    # generate a list of duplicate email addresses, which will tell us if someone had more than one booking and hence the number of bookings (no_bookings) they had
    book_emails = bookings['email'].str.lower()
    dupes = bookings[book_emails == book_emails[i]]
    no_bookings = len(dupes)

    # use the index of the duplicate rows to collect the other information for that customer
    ind = list(dupes.index.values)

    vehicles = bookings.loc[dupes.index.values,'type_of_vehicle_yearmakemodel'].array
    # remove duplicate vehicles
    vehicles_deduped = list(dict.fromkeys(vehicles.dropna()))

    products = bookings.loc[dupes.index.values,'type_of_car_seat'].array
    # remove duplicate products
    products_installed = list(dict.fromkeys(products.dropna()))

    children_ages = bookings.loc[dupes.index.values,'age_of_child'].str.lower()
    # there are 322 different types of ages entered into the field, some of which include the word "DOG" instead of a child's age
    # additionally, the entered terms vary from "unborn" to "0 unborn" to "may 29 due" to "unbron" to "-1 month"
    # for now, we will assume just one child

    # we would also need to do a translation from the date of the appointment and the age of the child, which is difficult given the variance in the entered terms for age

    # match ages of children
    # if they are the same, then consider only 1 child as part of the family
    # problem with this assumption is that some people have requested appointments for the same child over a period of time, which could result in multiple different ages for the same child
    if len(children_ages) == 1:
        no_children = len(children_ages)
        children_ages.reset_index(drop=True,inplace=True)
        child_1_age = children_ages[0]
    elif len(children_ages) > 1:
        ages = children_ages.drop_duplicates()
        no_children = len(ages)
        ages.reset_index(drop=True,inplace=True)
        child_1_age = ages[0]
        j = 0
        if no_children == 2:
            child_2_age = ages[1]
        elif no_children == 3:
            child_2_age = ages[1]
            child_3_age = ages[2]
        elif no_children == 4:
            child_2_age = ages[1]
            child_3_age = ages[2]
            child_4_age = ages[3]
        elif no_children > 4:
            child_2_age = ages[1]
            child_3_age = ages[2]
            child_4_age = ages[3]
            child_5_age = ages[4]

    # since 'name' is only one field, we need to split the name into first and last
    name = bookings.loc[i,'name'].split(' ',1)
    if len(name) > 1:
        first_name = name[0]
        # making the assumption that anything after the first space is considered the last name
        # we may need to consider how this works when comparing to existing database – probably want to use the name from another source as the source of truth
        last_name = name[1]
    else:
        first_name = name[0]
    
    email = bookings.loc[i,'email']

    # need a way to standardize/clean phone number since the data is not all actual 9 digit numbers
    # remove all non-integer symbols to normalize to just a string of numbers
    phone = bookings.loc[i,'your_phone_number']
    if phone[0].isdigit():
        phone_normalized = ''
        if (len(phone) == 9) or (len(phone) == 10):
            phone_normalized = phone
        else:
            j = 0
            while j < len(phone):
                if phone[j].isdigit():
                    phone_normalized += phone[j]
                j += 1
    elif phone[0].isdigit() == False:
        if (phone[0] == '+') or (phone[0] == '('):
            j = 0
            phone_normalized = ''
            while j < len(phone):
                # only include numbers, no special characters
                if phone[j].isdigit():
                    phone_normalized += phone[j]
                j += 1

    if ind[0] >= i:
        customer = pd.DataFrame({
            'First Name' : [first_name],
            'Last Name' : [last_name],
            'Email' : [email],
            'Phone' : [phone],
            'Source' : [source],
            'No. of Children' : [no_children],
            'Child 1 Age' : [child_1_age],
            'Child 2 Age' : [child_2_age],
            'Child 3 Age' : [child_3_age],
            'Child 4 Age' : [child_4_age],
            'Child 5 Age' : [child_5_age],
            'Products Installed' : [', '.join(products_installed)],
            'Vehicles' : [', '.join(vehicles_deduped)],
            'Bookings' : [no_bookings]
            })
        customers = pd.concat([customers, customer], ignore_index=True)
    
    bar.next()

today = date.today()
date_str = today.strftime("%Y_%m_%d")

customers.to_csv("bookings_clean_" + date_str + ".csv")
bar.finish()

# age_dict = pd.DataFrame()