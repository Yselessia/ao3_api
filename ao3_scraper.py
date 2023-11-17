import AO3
import AO3.works

import time
import pickle
import blosc
from functools import reduce
from math import ceil

def main():
    # Put results in this list
    # Each snapshot should go in a dictionary or its own
    queried_works_dict_list = []

    # Get time
    start_time=time.time()
    
    # Set snapshot period in hours
    snapshot_period = 1
    
    # Time between snapshot starts in hours
    snapshot_wait = 1
    
    # Buffer period
    buffer = 5/60
    
    # Number of snapshots
    n_shapshots = 2

    for c_snapshot in range(0,n_shapshots):
        print(f"Starting iteration {c_snapshot}")
        works_dict = {}
        query_start = None
        counter = 0
        
        if c_snapshot==1:
            # get the minimum and maximum work id observed so far
            # we can use this to 
            visited_ids = [w.id for w in queried_works_dict_list[0].values()]
            max_workid=max(visited_ids)
            min_workid = min(visited_ids)
        if c_snapshot>0:
            # Delay if need be
            amt_to_wait = 3600*snapshot_wait*c_snapshot - (time.time()-start_time)  - buffer
            if  amt_to_wait >0 :
                print(f"  Waiting {int(amt_to_wait/60)} minutes until next period starts.")
                while amt_to_wait>0:
                    partial_wait = min(amt_to_wait,60*5)
                    time.sleep(partial_wait)
                    amt_to_wait-=partial_wait
                    print(f"  {int(amt_to_wait/60)} minutes remaining.")
            
            # How long has it been since the beginning in hours?
            hours_since_start = int((time.time()-start_time)/3600)
            
        
        period_start = time.time()
    
        
        for search_count in range(0,snapshot_period):
            print(f"  Starting search {search_count}. {int((snapshot_period*3600-(time.time()-period_start))/60)} minutes remaining in period.")
            
            if search_count:
                # Delay if need be
                
                amt_to_wait = (period_start+search_count*3600) - time.time()
                if  amt_to_wait >0 :
                    print(f"  Waiting {int(amt_to_wait)/60} minutes. {int(((period_start+snapshot_period*3600) - time.time())/60)} minutes remaining in period.")
                    while amt_to_wait>0:
                        partial_wait = min(amt_to_wait,60*5)
                        time.sleep(partial_wait)
                        amt_to_wait-=partial_wait
                        print(f"  {int(amt_to_wait)/60} minutes remaining.")
            
            query_start = time.time()
            # make query
            if c_snapshot>0:
                # Make sure we get the whole window for sure,
                # but we can throw out a range of work ids to save time
                search = AO3.Search(revised_at=f"{max(0,hours_since_start)}-{hours_since_start+2} hours",
                                    any_field=f"id:<={max_workid} id:>={min_workid}",
                                    sort_column='revised_at',
                                    sort_direction='desc')
            else:
                # Add a buffer to how far in the past to look.
                # Should terminate early once hit overlap
                search = AO3.Search(revised_at=f"< {1+int(search_count>1)} hours",sort_column='revised_at',sort_direction='desc')
            c_page = 0
            while (not c_page) or c_page < search.pages:
                c_page+=1
                search.page = c_page
                
                
                search.update()
                print(f"    Querying page {c_page}")
                
                # Add all results to the dictionary
                all_in_dict = True
                for res in search.results:
                    # Check to see if this id is accounted for
                    in_dict = res.id in works_dict
                    all_in_dict = all_in_dict and in_dict
                    if not in_dict and (c_snapshot==0 or res.id in queried_works_dict_list[0]):
                            works_dict[res.id]=res
                            
                        
                # if everything on the page is in the dictionary, we're probably overlapping
                if all_in_dict:
                    break
                
            # Finished processing query
            print(f"  Done with search {search_count}.")

        # Add works dict to the list
        queried_works_dict_list.append(works_dict)
    

    # Save results
    pickled_data = pickle.dumps(queried_works_dict_list)
    compressed_pickle = blosc.compress(pickled_data)

    with open("queried_works_dict_list.dat", "wb") as file:
        file.write(compressed_pickle)
    
                
    
    

if __name__ == '__main__':
    main()