from datetime import date
from functools import cached_property
import datetime
import re
import time
from bs4 import BeautifulSoup

from . import threadable, utils
from .common import get_work_from_banner
from .requester import requester
from .users import User
from .works import Work


class Workgroup:
    def __init__(self, url, session=None, load=True):
        """Creates a new 'Work Group' object from a url

        Args:
            url (str): ao3-url for a group of works; can be user-work-page, search results etc; can also be series or collection url !!
            session (AO3.Session, optional): Session object. Defaults to None.
            load (bool, optional): If true, loaded on initialization. Defaults to True.

        Raises:
            utils.InvalidIdError: Invalid url
        """

        self._session = session
        self._soup_start = None
        self.group_url = url
        self._work_ids = None
        self._pages = None
        #self._name = None

        if load:
            self.reload()
            
    def __eq__(self, other):
        return isinstance(other, __class__) and other.group_url == self.group_url
    
    def __repr__(self):
        try:
            return f"<Group of works from [{self.name}]>"
        except:
            return f"<Group of works from [{self.group_url}]>"
        
    def __getstate__(self):
        d = {}
        for attr in self.__dict__:
            if isinstance(self.__dict__[attr], BeautifulSoup):
                d[attr] = (self.__dict__[attr].encode(), True)
            else:
                d[attr] = (self.__dict__[attr], False)
        return d

    def __setstate__(self, d):
        for attr in d:
            value, issoup = d[attr]
            if issoup:
                self.__dict__[attr] = BeautifulSoup(value, "lxml")
            else:
                self.__dict__[attr] = value
                
    def set_session(self, session):
        """Sets the session used to make requests for this series

        Args:
            session (AO3.Session/AO3.GuestSession): session object
        """
        
        self._session = session 
        
    @threadable.threadable
    def reload(self):
        """
        Loads information about this series.
        This function is threadable.
        """
        
        for attr in self.__class__.__dict__:
            if isinstance(getattr(self.__class__, attr), cached_property):
                if attr in self.__dict__:
                    delattr(self, attr)
                    
        self._soup_start = self.request(f"{self.group_url}&page=1")
        # ??? self._soup_start = self.request(self.url_start)
        #self._pages = self.pages
        #self._name = self.name()
        if "Error 404" in self._soup_start.text:
            raise utils.InvalidIdError("Cannot find page")

    @cached_property
    def url_start(self):
        """Returns the URL to this series

        Returns:
            str: series URL
        """

        return f"{self.group_url}&page=1"

    @property
    def loaded(self):
        """Returns True if this series has been loaded"""
        return self._soup_start is not None
        
    @cached_property
    def authenticity_token(self):
        """Token used to take actions that involve this work"""
        
        if not self.loaded:
            return None
        
        token = self._soup_start.find("meta", {"name": "csrf-token"})
        return token["content"]

    @cached_property
    def name(self):
        """"""

        if not self.loaded:
            return None

        heading = self._soup_start.find("h2", {"class": "heading"})
        return heading.text.strip()

    @cached_property
    def pages(self):
        pages = self._soup_start.find("ol", {"aria-label": "Pagination"})
        if pages is None:
            return 1
        n = 1
        for li in pages.find_all("li"):
            text = li.getText()
            if text.isdigit():
                n = int(text)

        if n > 10:
            print(f"WARNING: this group of works contains more than {(n-1)*20} items on {n} pages.")
        return n


    
    def get_work_ids(self, hist_sleep=3, start_page=0, max_pages=None, timeout_sleep=60):
        """
        Arguments:
            sleep (int): The time to wait between page requests
            timeout_sleep (int): The time to wait after the rate limit is hit

        Returns:
            works (list): All marked for later works
        """

        if self._work_ids is None:

          self._work_ids = {}
          #self._pagecount = self.pages()


          for page in range(start_page, self.pages):
                print(f"Processing page {page+1} of {self.pages} pages.")
                #print(str(page))
                # If we are attempting to recover from errors then
                # catch and loop, otherwise just call and go
                if timeout_sleep is None:
                  self._load_work_ids(page=page+1)

                else:
                    loaded=False
                    while loaded == False:
                        try:
                            self._load_work_ids(page=page+1)
                            print(f"Read marked-for-later page {page+1}")
                            loaded = True

                        except utils.HTTPError:
                            print(f"Loading being rate limited, sleeping for {timeout_sleep} seconds")
                            time.sleep(timeout_sleep)


                  # Check for maximum history page load
                if max_pages is not None and page >= max_pages:
                    return self._work_ids 

                # Again attempt to avoid rate limiter, sleep for a few
                # seconds between page requests.
                if hist_sleep is not None and hist_sleep > 0:
                    print(f"Sleeping for {hist_sleep} seconds")
                    time.sleep(hist_sleep)
                    

        return self._work_ids 
    
    def _load_work_ids(self, page=1):   
        url = f"{self.group_url}&page={page}"
        soup = self.request(url)
        #all_works_soup = soup.find("ol", {"class": ["index", "group"]})
        #works_soup = all_works_soup.find_all("li", {"role": "article"})
        works_soup = soup.find_all("li", {"role": "article"})

        #read_later = worksRaw.find("ol", {"class": "reading work index group"})

        for item in works_soup:
            # authors = []
            workname = None
            workid = None
            for a in item.h4.find_all("a"):
                if a.attrs["href"].startswith("/works"):
                    workname = str(a.string)
                    workid = utils.workid_from_url(a["href"])
                    
            if workname is not None and workid is not None:
                self._work_ids[workid]= workname

    
    def get(self, *args, **kwargs):
        """Request a web page and return a Response object"""  
        
        if self._session is None:
            req = requester.request("get", *args, **kwargs)
        else:
            req = requester.request("get", *args, **kwargs, session=self._session.session)
        if req.status_code == 429:
            raise utils.HTTPError("We are being rate-limited. Try again in a while or reduce the number of requests")
        return req

    def request(self, url):
        """Request a web page and return a BeautifulSoup object.

        Args:
            url (str): Url to request

        Returns:
            bs4.BeautifulSoup: BeautifulSoup object representing the requested page's html
        """

        req = self.get(url)
        soup = BeautifulSoup(req.content, "lxml")
        return soup
