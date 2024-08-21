from math import ceil

from bs4 import BeautifulSoup

from . import threadable, utils
from .common import get_work_from_banner
from .requester import requester
from .series import Series
from .users import User
from .works import Work
from .tags import Tag

from utils import ImproperSearchError

import re
from datetime import datetime

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
        if category not in ['Fandom', 'Character', 'Relationship', 'Freeform', 'ArchiveWarning', 'Category', 'Rating']:
            raise ImproperSearchError("Tag Category must be 'Fandom', 'Character', 'Relationship', 'Freeform', 'ArchiveWarning', 'Category', 'Rating', or ''.")
        self.tag_category = category
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
            tag_category=self.tag_category,
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
               
            canonical = tag.find("span")['class']
            tag_category, tag_name, n_works = re.findall(r"([A-Za-z]+): (.+) \u200e\((\d+)\)",tag.find("span").getText())[0]
            n_works = int(n_works)
            
            # Add tag to cache, but dont load?
            # Not sure if this is necessary or a good idea
            # Doing it since we do it for works
            c_tag = Tag(name=tag_name,load=False,session=self.session)
            if not c_tag.loaded and not c_tag.query_error:
                # Set what we do know, but don't update loaded status
                setattr(c_tag,'canonical',canonical)
                # need to add space to ArchiveWarning
                if tag_category == 'ArchiveWarning':
                    setattr(c_tag,'category','Archive Warning')
                else:
                    setattr(c_tag,'category',tag_category)
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
    tag_category = "",
    canonical = "",
    sort_column="name",
    sort_direction="asc",
    session=None):
    """Returns the results page for the search as a Soup object

    Args:
        any_field (str, optional): Generic search. Defaults to "".
        tag_name (str, optional) : Name of tag. Defaults to "".
        fandoms (str, optional) : Name of parent fandom. Must be an exact match. Defaults to "".
        tag_category (str, optional) : Type of tag. Options are Fandom, Character, Relationship, Freeform, ArchiveWarning, Category, Rating. If input type is not "" or one of the preceeding types, will raise exception.
        canonical (bool, optional) :  If specified, if false, exclude canocial, if true, include only canonical.
        sort_column (str, optional): Which column to sort on. Defaults to 'name'. If not 'name', 'date_created', or 'uses' will raise exception.
        sort_direction (str, optional): Which direction to sort. Defaults to asc. If not 'desc' or 'asc' will raise exception.
        page (int, optional): Page number. Defaults to 1.
        session (AO3.Session, optional): Session object. Defaults to None.

    Returns:
        bs4.BeautifulSoup: Search result's soup
    """

    query = utils.Query()
    query.add_field(f"work_search[query]={any_field if any_field != '' else ' '}")
    if page != 1:
        query.add_field(f"page={page}")
    if tag_name != "":
        query.add_field(f"work_search[title]={tag_name}")
    if fandoms != "":
        query.add_field(f"work_search[fandoms]={fandoms}")
    if tag_category != "":
        if tag_category not in ['Fandom', 'Character', 'Relationship', 'Freeform', 'ArchiveWarning', 'Category', 'Rating']:
            raise ImproperSearchError("Tag Category must be 'Fandom', 'Character', 'Relationship', 'Freeform', 'ArchiveWarning', 'Category', 'Rating', or ''.")
        query.add_field(f"work_search[type]={tag_category}")
    if canonical is not None:
        query.add_field(f"tag_search[canonical]={'T' if canonical else 'F'}")
    if sort_column not in ['uses', 'created_at', 'name']:
        raise ImproperSearchError("Sort Column must be 'uses', 'created_at' or 'name.")
    if sort_column != "":
        query.add_field(f"work_search[sort_column]={sort_column}")
    if sort_direction not in ['desc', 'asc']:
        raise ImproperSearchError("Sort Direction must be 'desc' or 'asc'.")
    if sort_direction != "":
        # Options are
        # uses
        # created_at
        # name
        query.add_field(f"work_search[sort_direction]={sort_direction}")

    url = f"https://archiveofourown.org/tags/search?commit=Search+Tags&{query.string}"

    if session is None:
        req = requester.request("get", url)
    else:
        req = session.get(url)
    if req.status_code == 429:
        raise utils.HTTPError("We are being rate-limited. Try again in a while or reduce the number of requests")
    soup = BeautifulSoup(req.content, features="lxml")
    return soup
