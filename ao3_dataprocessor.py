import AO3
import AO3.works

import time
import pickle
import blosc
import pandas as pd


with open("queried_works_dict_list.dat", "rb") as file:
    compressed_pickle=file.read()
    pickled_data = blosc.decompress(compressed_pickle)
    dict_list = pickle.loads(pickled_data)
    

# For each work, extract metadata and access time

data_dict = {}

normal_fields = (
    "bookmarks", 
    "categories",
    "nchapters",
    "characters",
    "complete",
    "comments",
    "expected_chapters",
    "fandoms",
    "hits",
    "kudos",
    "language",
    "rating",
    "relationships",
    "restricted",
    "status",
    "summary",
    "tags",
    "title",
    "warnings",
    "id",
    "words",
    "collections"
)
datetime_fields = (
    "date_edited",
    "date_published",
    "date_updated",
    "date_queried"
)

for f in normal_fields:
    data_dict[f] = []
    
for f in datetime_fields:
    data_dict[f] = []
    
data_dict['searchable_tags'] = []
    

def safe_attribute(o,attr):
    try:
        return getattr(o,attr)
    except:
        return pd.NA

counter = 0
total_entries = sum(sum(len(i) for i in inner_list) for inner_list in dict_list)
for inner_list in dict_list:
    for works_dict in inner_list:
        for work in works_dict.values():
            counter+=1
            print(f"Processing work #{counter} of {total_entries} ({(100*counter)//total_entries}% done)")
            for att in normal_fields:
                data_dict[att].append(safe_attribute(work,att))
            for att in datetime_fields:
                data_dict[att].append(pd.Timestamp(safe_attribute(work,att)))
            data_dict['searchable_tags'].append(work.search_tags)
            


works_df = pd.DataFrame.from_dict(data_dict)
# Missing values for these are just zeros
works_df.bookmarks.replace(pd.NA,0,inplace=True)
works_df.comments.replace(pd.NA,0,inplace=True)
works_df.kudos.replace(pd.NA,0,inplace=True)
works_df.hits.replace(pd.NA,0,inplace=True)


unique_works = works_df['id'].unique()
nobs = [sum(works_df['id']==u) for u in unique_works]

temp_df = works_df.loc[works_df['id']==51525724]
min_time_idx = temp_df['date_queried'].argmin()
max_time_idx = temp_df['date_queried'].argmax()

# Taking the last period
# This should be an hour between queries, but we'll double check

change_dict = {}
# Intialize
change_dict['id'] = []

change_dict['kudos_delta'] = []
change_dict['hits_delta'] = []
change_dict['comments_delta'] = []
change_dict['bookmarks_delta'] = []
change_dict['words_delta'] = []
change_dict['chapters_delta'] = []

change_dict['modified_before_window'] = []

change_dict['hits'] = []
change_dict['kudos'] = []
change_dict['comments'] = []
change_dict['bookmarks'] = []

change_dict['words'] = []
change_dict['chapters'] = []
change_dict['language'] = []
change_dict['restricted'] = []
change_dict['complete'] = []

change_dict['date_queried'] = []
change_dict['time_elapsed'] = []

change_dict['searchable_tags'] = []

counter=0
for c_id in unique_works:
    temp_df = works_df.loc[works_df['id']==c_id]

    change_dict['id'].append(c_id)

    change_dict['modified_before_window'].append(len(temp_df)>2)
    
    
    final = temp_df.iloc[-1]

    if len(temp_df)>1:
        
        # Actually changed to first entry, but havent changed name
        penultimate = temp_df.iloc[0]

        change_dict['kudos_delta'].append(final.kudos-penultimate.kudos)
        change_dict['hits_delta'].append(final.hits-penultimate.hits)
        change_dict['comments_delta'].append(final.comments-penultimate.comments)
        change_dict['bookmarks_delta'].append(final.bookmarks-penultimate.bookmarks)
        change_dict['words_delta'].append(final.words-penultimate.words)
        change_dict['chapters_delta'].append(final.nchapters-penultimate.nchapters)

        change_dict['time_elapsed'].append(final.date_queried-penultimate.date_queried)

    else:
        change_dict['kudos_delta'].append(pd.NA)
        change_dict['hits_delta'].append(pd.NA)
        change_dict['comments_delta'].append(pd.NA)
        change_dict['bookmarks_delta'].append(pd.NA)
        change_dict['words_delta'].append(pd.NA)
        change_dict['chapters_delta'].append(pd.NA)
        
        change_dict['time_elapsed'].append(pd.Timedelta(0,'s'))

    change_dict['hits'].append(final.hits)
    change_dict['kudos'].append(final.kudos)
    change_dict['comments'].append(final.comments)
    change_dict['bookmarks'].append(final.bookmarks)

    change_dict['words'].append(final.words)
    change_dict['chapters'].append(final.nchapters)
    change_dict['language'].append(final.language)
    change_dict['restricted'].append(final.restricted)
    change_dict['complete'].append(final.complete)

    change_dict['date_queried'].append(final.date_queried)


change_df = pd.DataFrame.from_dict(change_dict)
change_df.set_index('id',inplace=True)

# Cut off time deltas without sufficient time elapsed
change_df = change_df.loc[change_df.time_elapsed>pd.Timedelta(22,'h')]




# Messing around
change_df['kudos_ratio'] = change_df.kudos_delta/(change_df.hits_delta+1)

change_df['kudos_ratio2'] = change_df.kudos/(change_df.hits)

change_df_longfics = change_df.loc[(change_df.words>=40000) &
                                   (change_df.language=='English') &
                                   (change_df.kudos_delta==0) &
                                    (change_df.kudos_ratio2<0.005)]