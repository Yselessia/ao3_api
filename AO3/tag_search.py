from math import ceil

from bs4 import BeautifulSoup

from . import threadable, utils
from .common import get_work_from_banner
from .requester import requester
from .series import Series
from .users import User
from .works import Work
from .tags import Tag

from .utils import ImproperSearchError, tagname_from_href

import re
from datetime import datetime
from urllib.parse import quote_plus

DEFAULT = "_score"
BEST_MATCH = "_score"
AUTHOR = "authors_to_sort_on"
TITLE = "title_to_sort_on"
DATE_POSTED = "created_at"
DATE_UPDATED = "revised_at"
WORD_COUNT = "word_count"
RATING = "rating_ids"
HITS = "hits"
BOOKMARKS = "bookmarks_count"
COMMENTS = "comments_count"
KUDOS = "kudos_count"

DESCENDING = "desc"
ASCENDING = "asc"

#https://stackoverflow.com/questions/18495098/python-check-if-an-object-is-a-list-of-strings
def is_list_of_strings(lst):
        return bool(lst) and not isinstance(lst, str) and all(isinstance(elem, str) for elem in lst)


class TagSearch:
    def __init__(
        self,
        any_field="",
        tag_name = "",
        fandoms="",
        category = "",
        canonical = "",
        page=1,
        sort_column="name",
        sort_direction="asc",
        session=None):



        self.any_field=any_field
        self.tag_name = tag_name
        self.fandoms=fandoms
        if category not in ['Fandom', 'Character', 'Relationship', 'Freeform', 'ArchiveWarning', 'Category', 'Rating','']:
            raise ImproperSearchError("Tag Category must be 'Fandom', 'Character', 'Relationship', 'Freeform', 'ArchiveWarning', 'Category', 'Rating', or ''.")
        self.category = category
        self.canonical = canonical
        if sort_column not in ['uses', 'created_at', 'name']:
            raise ImproperSearchError("Sort Column must be 'uses', 'created_at' or 'name.")
        self.sort_column=sort_column
        if sort_direction not in ['desc', 'asc']:
            raise ImproperSearchError("Sort Direction must be desc (descending) or asc (ascending).")
        self.sort_direction=sort_direction
        self.page = page
        
        self.session = session

        self.results = None
        self.pages = 0
        self.total_results = 0

    @threadable.threadable
    def update(self):
        """Sends a request to the AO3 website with the defined search parameters, and updates all info.
        This function is threadable.
        """

        soup = tag_search(
            any_field=self.any_field,
            tag_name=self.tag_name,
            fandoms=self.fandoms,
            page=self.page,
            category=self.category,
            canonical=self.canonical,
            sort_column=self.sort_column,
            sort_direction=self.sort_direction,
            session=self.session)

        # Pull time for building tags
        c_time = datetime.now()

        results = soup.find("ol", {"class": ("tag", "index", "group")})
        # Fix the next thing probably
        if results is None and soup.find("p", text="No results found. You may want to edit your search to make it less specific.") is not None:
            self.results = []
            self.total_results = 0
            self.pages = 0
            return

        tags = []
        for tag in results.find_all("li"):
               
            canonical =  tag.find("span",{'class':'canonical'}) is not None
            category, _, n_works = re.findall(r"([A-Za-z]+): (.+) \u200e\((\d+)\)",tag.find("span").getText())[0]
            n_works = int(n_works)
            
            tag_name = tagname_from_href(tag.find("span").a['href'])
            
            # Add tag to cache, but dont load?
            # Not sure if this is necessary or a good idea
            # Doing it since we do it for works
            c_tag = Tag(tag_name,load=False,session=self.session)
            if not c_tag.loaded and not c_tag.query_error:
                # Set what we do know, but don't update loaded status
                setattr(c_tag,'canonical',canonical)
                # need to add space to ArchiveWarning
                if category == 'ArchiveWarning':
                    setattr(c_tag,'category','Archive Warning')
                else:
                    setattr(c_tag,'category',category)
            # Tags don't hold count info as of 2024-08-20
            # I'd rather not include it there since you can't get that
            # data from the Tag page itself, and you get different results]
            # from the 'tag search' page and from filtering by works on that tag
            # (probably due to unlisted/restricted works)
            # IDK
            setattr(c_tag,'works',n_works)
            setattr(c_tag,'date_tag_search',c_time)
            tags.append(c_tag)

        self.results = tags
        self.total_results = int(soup.find("h3",{"class":"heading"}).getText().replace(',','')[:-8])
        self.pages = min(ceil(self.total_results / 50),2000) # Pages cap out at 2000

def tag_search(
    any_field="",
    tag_name = "",
    fandoms="",
    page=1,
    category = "",
    canonical = "",
    sort_column="name",
    sort_direction="asc",
    session=None):
    """Returns the results page for the search as a Soup object

    Args:
        any_field (str, optional): Generic search. Defaults to "".
        tag_name (str, optional) : Name of tag. Defaults to "".
        fandoms (str or list or strs, optional) : Name of parent fandom. Must be an exact match. Defaults to "".
        category (str, optional) : Type of tag. Options are Fandom, Character, Relationship, Freeform, ArchiveWarning, Category, Rating. If input type is not "" or one of the preceeding types, will raise exception.
        canonical (bool, optional) :  If specified, if false, exclude canocial, if true, include only canonical.
        sort_column (str, optional): Which column to sort on. Defaults to 'name'. If not 'name', 'date_created', or 'uses' will raise exception.
        sort_direction (str, optional): Which direction to sort. Defaults to asc. If not 'desc' or 'asc' will raise exception.
        page (int, optional): Page number. Defaults to 1.
        session (AO3.Session, optional): Session object. Defaults to None.

    Returns:
        bs4.BeautifulSoup: Search result's soup
    """

    query = utils.Query()
    query.add_field(f"tag_search[query]={any_field if any_field != '' else ' '}")
    if page != 1:
        query.add_field(f"page={page}")
    if tag_name != "":
        tag_search_str = quote_plus(tag_name)
        query.add_field(f"tag_search[name]={tag_search_str}")
    if fandoms != "":
        # Check if it's a list of strings
        if is_list_of_strings(fandoms):
            fandom_search_string = ",".join(quote_plus(f) for f in fandoms)
        elif fandoms != "" and isinstance(fandoms, str):
            fandom_search_string = quote_plus(fandoms)
        else:
            raise ImproperSearchError("Fandoms must be a string or a list of strings")
        query.add_field(f"tag_search[fandoms]={fandom_search_string}")
    if category != "":
        if category not in ['Fandom', 'Character', 'Relationship', 'Freeform', 'ArchiveWarning', 'Category', 'Rating']:
            raise ImproperSearchError("Tag Category must be 'Fandom', 'Character', 'Relationship', 'Freeform', 'ArchiveWarning', 'Category', 'Rating', or ''.")
        query.add_field(f"tag_search[type]={category}")
    if canonical != "":
        query.add_field(f"tag_search[canonical]={'T' if canonical else 'F'}")
    if sort_column not in ['uses', 'created_at', 'name']:
        raise ImproperSearchError("Sort Column must be 'uses', 'created_at' or 'name.")
    if sort_column != "":
        query.add_field(f"tag_search[sort_column]={sort_column}")
    if sort_direction not in ['desc', 'asc']:
        raise ImproperSearchError("Sort Direction must be 'desc' or 'asc'.")
    if sort_direction != "":
        # Options are
        # uses
        # created_at
        # name
        query.add_field(f"tag_search[sort_direction]={sort_direction}")

    url = f"https://archiveofourown.org/tags/search?commit=Search+Tags&{query.string}"
    if session is None:
        req = requester.request("get", url)
    else:
        req = session.get(url)
    if req.status_code == 429:
        raise utils.HTTPError("We are being rate-limited. Try again in a while or reduce the number of requests")
    soup = BeautifulSoup(req.content, features="lxml")
    return soup
