# -*- coding: utf-8 -*-
"""
Provides Google search results and the page performance (via pagespeed insights) for each search result

@author: mingk
"""

import requests
import json
from googlesearch import search
import pandas as pd
import time

#Parameters below
g_api_key = 'AIzaSyCl6FnPd2-WkNDRg781i6_0SoEkHpY6jzg'
strategies = ['Desktop','Mobile']
search_terms = ['credit cards with $10,000 limit guaranteed approval',
'cfna credit cards',
'best credit cards for airline miles',
'barclays credit card',
'jetblue credit card'
]
num_results = 25
pause_s = 3
tries = 3 #keep this low or it will take forever to run
##Parameters finished



#reads in list, search_terms, and returns the search results, num_results
#the search function is from googlesearch and (I think) uses beautifulsoup webscraping
def auto_search(search_terms,num_results=20):
    column_titles = ['search term', 'result number', 'result URL']
    results_df = pd.DataFrame(columns = column_titles)
    
    for search_t in search_terms:
        i=1
        print('Searching: ' + search_t)
        for result in search(search_t, tld="com", num=num_results, stop=num_results, pause=2):
            row = [search_t, i, result]
            results_df.loc[len(results_df)] = row
            i=i+1
    return results_df

#reads in URL, site_urls, api key, desktop and/or mobile, and output score for pagespeedinsights
#pagespeedinsights: https://pagespeed.web.dev/
#API is provided by Google. API key is required for many requests
def auto_site_speed1(url,g_api_key,strat):
    try:
        request_url = 'https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url='  + url + \
                    '&strategy=' + strat + '&key=' + g_api_key
        response_site = requests.get(request_url)
        response_dict = response_site.json()
        row = [response_dict['id'],
        strat,
        response_dict['lighthouseResult']['categories']['performance']['score'],
        response_dict["originLoadingExperience"]["metrics"]["FIRST_CONTENTFUL_PAINT_MS"]["category"],
        response_dict["originLoadingExperience"]["metrics"]["FIRST_INPUT_DELAY_MS"]["category"],
        response_dict["originLoadingExperience"]["metrics"]["LARGEST_CONTENTFUL_PAINT_MS"]["category"],
        response_dict["originLoadingExperience"]["metrics"]["CUMULATIVE_LAYOUT_SHIFT_SCORE"]["category"],
        ]
        print('Performance Found: ' + response_dict['id'] + ' ||| ' + strat + ' ||| Speed=' + str(response_dict['lighthouseResult']['categories']['performance']['score']*100))
        return row
        #Pause is so program doesn't overload the Google API.
        #Not sure why but, despite documentation, api calls need to be spaced much more than documented
    except:
        print('Failed Check: ' + url + ' ||| ' + strat + ' ||| ERROR')
        return [url,strat,'ERROR','ERROR','ERROR','ERROR','ERROR'] #not handled very well :(
    
def auto_site_speed_multiple(site_urls, g_api_key,strategies, pause_s=5, tries=3):
    df_cols = ['URL', 'strategy','score','first_contentful_paint','first_input_delay_ms','largest_contentful_paint','cumulative_layout_shift']
    results_df = pd.DataFrame(columns = df_cols)
    j=1
    for url in site_urls:
        for strat in strategies:
            i=1
            #loop should be handled better...
            while i <= tries:
                row = auto_site_speed1(url=url,g_api_key=g_api_key,strat=strat)
                if row[3] == 'ERROR':
                    if i == tries:
                        results_df.loc[len(results_df)] = row
                    i=i+1
                    time.sleep(5) #baking in 5 seconds to buffer and "cool down"
                else:
                    results_df.loc[len(results_df)] = row
                    i=i+10000000 #Logic handled kind of poorly. Should fix
                time.sleep(pause_s) #baking in 5 seconds in case API calls are too close. Frequest API calls seem to throw errors a lot
            print(str(j))
            j=j+1
    return results_df
      
#first, find the search results for the searches you want to use
searches_df = auto_search(search_terms=search_terms,num_results=num_results)

#select the distinct URLs in the list
url_list = list(set(searches_df['result URL']))

#create dataframe of the performance in PageSpeedInsights
speed_df = auto_site_speed_multiple(site_urls = url_list,g_api_key = g_api_key,strategies = strategies, pause_s=pause_s,tries=tries)



#combine!
final_df = searches_df.merge(speed_df,how="left",left_on="result URL",right_on="URL")

final_df.to_excel('C:\\Users\\KongM\\Desktop\\Search Output\\credit_cards_25_top5_30days.xlsx')


#Trouble Shoooting Scratch below

#A=auto_site_speed1('https://usa.visa.com/pay-with-visa/find-card/apply-credit-card/personal',g_api_key,strat='Desktop')


   
# url = 'https://usa.visa.com/pay-with-visa/find-card/apply-credit-card/personal'
# request_url = 'https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url='  + url + \
#             '&strategy=' + 'Desktop' + '&key=' + g_api_key

# response_site = requests.get(request_url)

# response_dict = response_site.json()

# print(response_dict)


A = auto_search(['more rewards credit cards'],num_results=30)
