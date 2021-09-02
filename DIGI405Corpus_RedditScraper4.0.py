# -*- coding: utf-8 -*-
"""
Created on Fri Aug 13 15:06:43 2021

@author: Ryan
"""

import pandas as pd
from pmaw import PushshiftAPI
import time
from pathlib import Path

api = PushshiftAPI()

#Collection of UTC date-times for coronavirus case 0 in each country
month_after_dict = dict({"hongkong":"1582333200", 
               "singapore":"1582369200", 
               "newzealand":"1585306800", 
               "australia":"1582542000",
               "canada":"1582542000",
               "unitedkingdom":"1583060400",
               "ireland":"1585443600"})

first_case_dict = dict({"hongkong":"1579654800", 
               "singapore":"1579690800", 
               "newzealand":"1582801200", 
               "australia":"1579863600",
               "canada":"1579863600",
               "unitedkingdom":"1580382000",
               "ireland":"1582938000"})

month_before_dict = dict({"hongkong":"1576976400", 
               "singapore":"1577012400", 
               "newzealand":"1580122800", 
               "australia":"1577185200",
               "canada":"1577185200",
               "unitedkingdom":"1577703600",
               "ireland":"1580259600"})

def get_post_ids(subreddit, query, limit, time_period):
    """Takes search parameters and returns df of post ids"""
    t = time.perf_counter()
    if time_period == "precovid":
        before = int(first_case_dict[subreddit.lower()])
        after = int(month_before_dict[subreddit.lower()])
    else:
        before = int(month_after_dict[subreddit.lower()])
        after = int(first_case_dict[subreddit.lower()])
    #Retrieve only id number and num_comments fields, reduce load on api
    post_ids = api.search_submissions(q=query, 
                               subreddit=subreddit, 
                               limit=limit, 
                               after=after, 
                               before=before, 
                               fields=['id','num_comments'])
    if len(post_ids)>0:                                                        
        print(f'Retrieved {len(post_ids)} posts from Pushshift')
    else:
        print('No posts found matching the search criteria')
        return
    elapsed_time = time.perf_counter() - t
    print(f"Time taken: {elapsed_time:.0f} second(s)")
    post_df = pd.DataFrame(post_ids) #Turn response object into df to process
    return(post_df)


def get_comment_ids(post_ids):
    """Collect ids of all comments in given list of post ids"""
    t = time.perf_counter()
    # filtering low num of comment posts for quality and reduce load on api
    filtered_ids = post_ids[post_ids['num_comments'] > 1]
    print(f"Collecting comment IDs from {len(filtered_ids)} post(s) with > 1 comments")
    print("This may take some time...")   
    #searches pushshift for all posts in post_ids list
    comment_ids = api.search_submission_comment_ids(ids=filtered_ids['id']) 
    comment_ids = list(comment_ids)
    print(f"Collected {len(comment_ids)} comment IDs") 
    elapsed_time = time.perf_counter() - t
    print(f"Time taken: {elapsed_time:.0f} second(s)")
    return comment_ids


def get_comments(comment_ids):
    """Takes list of comment ids and returns df of comments from api"""
    columns = ['author','body','created_utc','permalink']
    comments_df = pd.DataFrame(columns=columns)
    #api hangs if request is excessive so split into chunks
    if len(comment_ids) > 300:
        n=1
        print("Chunking comment ids as >300 comments found")
        chunked = chunkList(comment_ids, chunkSize= 300)
        for chunk in chunked:
            t = time.perf_counter()
            print(f"Collecting {len(chunk)} comments. Chunk {n} of {len(chunked)}.")
            comments = api.search_comments(ids=chunk, fields=columns)
            comment_df = pd.DataFrame(comments)
            comments_df = pd.concat([comment_df, comments_df], axis=0, ignore_index=True)
            elapsed_time = time.perf_counter() - t
            print(f"Time taken: {elapsed_time:.0f} second(s)")
            n+=1
    else:
        t = time.perf_counter()
        comments = api.search_comments(ids=comment_ids, fields=columns)
        comments_df = pd.DataFrame(comments)
        elapsed_time = time.perf_counter() - t
        print(f"Time taken: {elapsed_time:.0f} second(s)")
    print(f"Collected {len(comments_df)} comments")
    
    return comments_df


def chunkList(initialList, chunkSize):
    """Chunks a list into sub lists that have a length equal to chunkSize."""
    finalList = []
    for i in range(0, len(initialList), chunkSize):
        finalList.append(initialList[i:i+chunkSize])
    return finalList


def clean_df(df):
    """Add any extra necessary cleaning steps here..."""
    clean_df = df.drop_duplicates(subset='body')
    clean_df = (clean_df[clean_df['author']!='[deleted]']).reset_index()
    if input("Erase usernames from data? Y/N ").upper() == "Y":
        clean_df = clean_df.drop('author', axis=1)
    return clean_df

def save_csv(df, subreddit, query, folder):
    try:
        Path(folder).mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        print("Folder already exists")
    else:
        print(f"Folder: ../{folder}")
    file = f"{subreddit}_comments_{query}.csv"
    df = clean_df(df)
    df.to_csv(f'{folder}/{file}', header=True, index=False, columns=list(df.axes[1]))
    print(f"All comments saved in {folder}/{file}")

def save_corpus(df, folder) :
    """Saves individual text files for each comment body of input df"""
    if input("Save comment text as raw corpus? Y/N ").upper() == "Y":
        t = time.perf_counter()
        print(f"Saving {len(df)} comments as corpus...")
        count = 0
        for comment in df['body']:
            file = f"{count}.txt"
            with open(f'{folder}/{file}', 'w', encoding='utf-8') as file: 
                file.writelines(comment)
                count+=1
                
        print(f"Corpus saved as {count} txt files.")
        elapsed_time = time.perf_counter() - t
        print(f"Time taken: {elapsed_time:.0f} second(s)")

def get_inputs():
    subreddit = input("Subreddit name to search through? ")
    query = input("Keyword to search post titles for? ")
    limit = int(input("Limit number of posts to: "))
    if input("Search before first local covid case? Y/N ").upper() == "Y":
        time_period = "precovid"
    else:
        time_period = "postcovid"
    print(f"Search period: {time_period}")
    
    return(subreddit, query, limit, time_period)
    
# def main(): 
done = False
while not done:
    subreddit, query, limit, time_period = get_inputs()
    t_total = time.perf_counter()
    pids = get_post_ids(subreddit, query, limit, time_period)
    if len(pids) > 0:
        cids = get_comment_ids(pids)
        comms = get_comments(cids)
        elapsed_time = time.perf_counter() - t_total
        print(f"TOTAL TIME TAKEN: {elapsed_time:.0f} second(s)")
        folder = f"{subreddit}_{time_period}_keyword_{query}"
        save_csv(comms, subreddit, query, folder) #df cleaning happens here
        save_corpus(comms, folder)
        done = True
    else:
        done = False
        print("Please try again with different search critera (hit enter) or quit (\"q\")")
        response = input()
        if response == 'q':
            break
        
# main()