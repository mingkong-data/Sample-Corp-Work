# -*- coding: utf-8 -*-
#A/B testing was done by a vendor. however, user splitting was spotty (sometimes could split users correctly and sometimes could not)
#when user splitting did not work, a user would see different versions of a page when navigating. So, Page A -> Page B -> Page A'
#where Page A' is an altered version of Page A
#This provides the %s and samples charts for the user split using parameter inputs.
#The value here is the SQL logic, which is dynamic depending on inputs

import sys
sys.path.insert(1,'C:/Users/KongM/Desktop/Python/Critical Code')
import FoxBIConnect as fox
import pandas as pd
from datetime import datetime
import dateutil
import matplotlib.pyplot as plt

#parameter inputs
date_start = '2021-11-06'
date_end = '2021-11-11'
experiment_name = 'NAME REDACTED'
control_name = 'baseline'
variant_name = 'NSL'


def year_month_lists(date_start_str, date_end_str):
    date_str1 = datetime.strptime(date_start_str,'%Y-%m-%d').replace(day=1)
    date_str2 = datetime.strptime(date_end_str,'%Y-%m-%d').replace(day=1)
    a_month = dateutil.relativedelta.relativedelta(months=1)
    output_y = set([])
    output_m = set([])
    if date_str1 > date_str2:
        date_str_t = date_str1
        date_str1 = date_str2
        date_str2 = date_str_t
    i = date_str1
    while i <= date_str2:
        output_y.add(str(i.year))
        output_m.add(str(i.month))
        i = i + a_month
    return {'year':','.join(list(output_y)),'month':','.join(list(output_m))}


query = """
WITH users_in_test AS (
SELECT DISTINCT (post_visid_high || post_visid_low) AS user_id
FROM TABLE NAME REDACTED
WHERE year IN (""" + year_month_lists(date_start, date_end).get('year') + """)
AND month IN (""" + year_month_lists(date_start, date_end).get('month') + """)
AND date_trunc('day',date_time) >= '""" + date_start + """'
AND date_trunc('day',date_time) <= '""" + date_end + """'
AND post_evar65 ilike '%""" + experiment_name + """%'
AND (
(post_page_url <> '' and post_page_url is not null)
OR
(post_pagename <> '' and post_pagename is not null)
) -- This determine whether it's a Page View hit
AND (COALESCE(post_prop40,'') <> 'video:v:video-embed.html') --Alt Pages Filter
AND (COALESCE(post_prop40,'') <> 'video:v:video-embed-amp.html')
), user_grouping AS (
SELECT A.user_id, 
COUNT(CASE WHEN post_evar65 ilike '%""" + experiment_name + """;""" + control_name + """%' THEN 1 END) AS baseline_pvs,
COUNT(CASE WHEN post_evar65 ilike '%""" + experiment_name + """;""" + variant_name + """%' THEN 1 END) AS variant_pvs,
COUNT(CASE WHEN post_evar65 not ilike '%""" + experiment_name + """%' THEN 1 END) AS non_test_pvs
FROM users_in_test A
INNER JOIN TABLE NAME REDACTED B ON A.user_id = (B.post_visid_high || B.post_visid_low)
WHERE year IN (""" + year_month_lists(date_start, date_end).get('year') + """)
AND month IN (""" + year_month_lists(date_start, date_end).get('month') + """)
AND date_trunc('day',date_time) >= '""" + date_start + """'
AND date_trunc('day',date_time) <= '""" + date_end + """'
AND (
(post_page_url <> '' and post_page_url is not null)
OR
(post_pagename <> '' and post_pagename is not null)
) -- This determine whether it's a Page View hit
AND (COALESCE(post_prop40,'') <> 'video:v:video-embed.html') --Alt Pages Filter
AND (COALESCE(post_prop40,'') <> 'video:v:video-embed-amp.html')
GROUP BY A.user_id
)
SELECT 
CASE WHEN baseline_pvs > 0 AND variant_pvs = 0 AND non_test_pvs = 0 THEN '1base_only'
WHEN baseline_pvs = 0 AND variant_pvs > 0 AND non_test_pvs = 0 THEN '2variant_only'
WHEN baseline_pvs = 0 AND variant_pvs = 0 AND non_test_pvs > 0 THEN '3nontest_only'
WHEN baseline_pvs > 0 AND variant_pvs > 0 AND non_test_pvs = 0 THEN '6baseline_and_variant'
WHEN baseline_pvs = 0 AND variant_pvs > 0 AND non_test_pvs > 0 THEN '5variant_and_nontest'
WHEN baseline_pvs > 0 AND variant_pvs = 0 AND non_test_pvs > 0 THEN '4baseline_and_nontest'
WHEN baseline_pvs > 0 AND variant_pvs > 0 AND non_test_pvs > 0 THEN '7baseline_and_variant_and_nontest'
ELSE '8other' END AS user_type, COUNT(*) AS users
FROM user_grouping
GROUP BY user_type
ORDER BY user_type
"""

output = fox.redshiftconnect1(query)
labels = output['user_type']
sizes = output['users']


plt.pie(sizes, labels=labels, 
autopct='%1.1f%%', shadow=True, startangle=90)

plt.axis('equal')
plt.show()


