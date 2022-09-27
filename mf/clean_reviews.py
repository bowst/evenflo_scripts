# Script for cleaning reviews export from Bazaarvoice
# The main goal is to combine reviews for customers, attach products to them, determine their customer roles

import pandas as pd
import numpy as np
from datetime import date
from progress.bar import Bar

# reviews = pd.read_csv('reviews.csv')
reviews = pd.read_csv('reviews_allreviewsreport.csv')

# products reviewed
# number of reviews
# average score
# accepted percentage?
# publis dates? It's another data point on the timeline...

# create lists of just the names for comparison
reviews_emails = reviews['User Email Address'].dropna()

# create new registration db to group registrations with the same email address first
customers = pd.DataFrame(columns=['Email', 'No. of Reviews', 'Average Score', 'Reviewed Products', 'Source', 'Family Role'])

role_data = {'key': [
    'Expectant',
    'Experienced|First-Time|Firsttime',
    'Grandparent|Other'
], 'value': [
    'Expecting',
    'Parent',
    'Gift Giver'
]}

role_dict = pd.DataFrame(data=role_data)

bar = Bar('Cleaning...', max=len(reviews_emails))

for x in reviews_emails.index:
    dupes = reviews_emails[reviews_emails == reviews_emails.loc[x]]
    ind = list(dupes.index.values)
    products = reviews.loc[dupes.index.values, 'Product ID'].array
    ratings = reviews.loc[dupes.index.values, 'Overall Rating'].array
    status = reviews.loc[dupes.index.values, 'Moderation Status']
    approved = len(status[status == 'APPROVED']) / len(status)
    avg_rating = np.mean(ratings)
    seeded = reviews.loc[dupes.index.values, 'Received free product']
    if seeded.isin(['Yes']).any():
        source = 'Bazaarvoice, Seeding Sample'
    else:
        source = 'Bazaarvoice'
    cust_type = reviews.loc[dupes.index.values]
    for y in role_dict.index:
        if cust_type['Type'].str.contains(role_dict.loc[y,'key']).any():
            role = role_dict.loc[y,'value']
        else:
            role = ''
    if ind[0] >= x:
        # print ('Email: %s | Reviews: %i | Avg Score: %f | Products: %a' % (reviews_emails.loc[x],len(ratings),avg_rating,', '.join(products)))
        customer = pd.DataFrame({
            'Email': [reviews.loc[x,'User Email Address']],
            'No. of Reviews': [len(ratings)],
            'Average Score': avg_rating,
            'Reviewed Products': [', '.join(products)],
            'Source' : [source],
            'Family Role' : [role]
            })
        customers = pd.concat([customers, customer], ignore_index=True)

    bar.next()

today = date.today()
date_str = today.strftime("%Y_%m_%d")

customers.to_csv("reviews_clean_" + date_str + ".csv")
bar.finish()