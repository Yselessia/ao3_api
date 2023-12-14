import warnings
from datetime import datetime
from functools import cached_property
from .requester import requester
import threading
from bs4 import BeautifulSoup
from datetime import datetime
import re
from pathlib import Path

import pickle

from . import threadable, utils
import os

class Tag:
    """
    AO3 tag object
    
    Tags are cached based on their unique tag name.
    One of the primary use cases for the Tag class is to quickly populate inherited tags.
    Cacheing resolves problems caused multiple Tag objects being created for the same Tag name.
    For example, most generic descriptor tags (e.g. 'Fluff', 'Smut') have 'No Fandom' as a parent.
    Cacheing keeps the 'No Fandom' Tag object unique and reduces the number of queries to AO3.
    
    For more info on tags, see: https://archiveofourown.org/wrangling_guidelines/2
    """
    _cache = {}
    _cache_lock = threading.Lock() # Lock for cache access
    _cache_counter = 0
    
    _lazy_evaluation = False          
    
    @classmethod
    def lazyEvaluation(cls,val):
        '''
        Sets whether the reload method will automatically parse the data,
        then delete the downloaded webpage.
        False by default. 
        '''
        cls._lazy_evaluation = val
    
    @classmethod
    def getCacheAccesses(cls):
        return cls._cache_counter
    
    @classmethod
    def __inCache(cls,tagname):
        with cls._cache_lock:
            return tagname in Tag._cache
    
    @classmethod
    def dumps(cls):
        '''
        pickle.dumps wrappper for the Tag cache
        '''
        with Tag._cache_lock:
            pickled_data = pickle.dumps(Tag._cache)
        return pickled_data
            
            
    @classmethod
    def loads(cls,pickled_data):
        '''
        pickle.loads wrappper for the Tag cache
        '''
        Tag._cache = pickle.loads(pickled_data)
    
    @classmethod
    def deleteCache(cls):
        with cls._cache_lock:
            Tag._cache = {}

        
    @classmethod
    def unique_visited(cls):
        '''
        Iterates over the cached dictionary values and returns all names.
        No tags that have made synonyms are returned

        Returns
        -------
        list: List of tag names

        '''
        with cls._cache_lock:
            return [t.name for t in Tag._cache.values()]
        
    
    @classmethod
    def __getCache(cls,tagname):
        with cls._cache_lock:
            return Tag._cache[tagname]

    @classmethod
    def printCache(cls):
        with cls._cache_lock:
            print(cls._cache)

    @classmethod
    def __getCachedTagNames(cls):
        with cls._cache_lock:
            return Tag._cache.keys()
        
    @classmethod
    def tagnameCached(cls,tagname):
        with cls._cache_lock:
            return tagname in cls._cache
       
    def _addToCache(self):
        with self._cache_lock:
            self._cache[self.name]=self
        
    @classmethod
    def _addSynonymsToCache(cls,tag):
        with cls._cache_lock:
            for name in tag.synonym_names:
                cls._cache[name]=tag

    def __new__(cls, tagname, *args, **kwargs):
        with cls._cache_lock:
            if tagname in cls._cache:
                cls._cache_counter+=1
                return cls._cache[tagname]
        tag = super(Tag, cls).__new__(cls)
        return tag
        
    def __hash__(self):
        # Two tags will hash the same if their names are the same
        return hash(self.name)
        
    def __lt__(self, other):
        return self.name < other.name    
    
    def __init__(self, name, session=None, load=True) -> None:
        if not isinstance(name,str):
            raise TypeError
        
        with Tag._cache_lock:
            if name in self._cache:
                return
            self.name = name
            self._cache[self.name] = self
        
        self._session = session
        self._soup = None
        self.date_queried = None
        
        if load:
            self.reload()


            
    def __getnewargs__(self):
        # Trying to get unpickling to populate the cache
        return (self.name,)
        
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
    
    
    def __eq__(self, other):
        # Could modify this to look at date_queried if that's of interest to anyone
        # But this is following the convention put forth in the work class
        # I apologize to anyone trying to do longitudinal tag analysis
        return isinstance(other, __class__) and other.name == self.name
    
    def __repr__(self):
        return f"<Tag [{self.name}]>"
    
    @threadable.threadable
    def reload(self):
        """
        Loads information about this work.
        This function is threadable.
        
        Args:
            load_chapters (bool, optional): If false, chapter text won't be parsed, and Work.load_chapters() will have to be called. Defaults to True.
        """
        for attr in self.__class__.__dict__:
            if isinstance(getattr(self.__class__, attr), cached_property):
                if attr in self.__dict__:
                    delattr(self, attr)
        
        self.date_queried = datetime.now()
        try:
            self._soup = self.request(f"https://archiveofourown.org/tags/{self.url}")
        except Exception as exc:
            print('%r generated an exception: %s' % (self, exc))
        
        if self.query_error:
            if not Tag._lazy_evaluation:
                # Get all the metadata and delete the BeautifulSoup
                self.parse()
            raise utils.InvalidIdError("Cannot find work")
        
        
        # if merged, load the tag it was merged with
        if self.merged_name:
            warnings.warn(f"<{self.name}> has been merged with {self.get_merged()}. Redirecting cache to new entry", stacklevel=2)
            merged = self.get_merged()
            
            # Load merged and add its syns if need be
            # if not merged, point any tags that have been merged with it to this one in memory
            if not merged.loaded:
                merged.reload()
                Tag._addSynonymsToCache(merged)
                
            # occasionally, the list of synonyms isn't complete.
            # append this tag to the list if it's not
            if self.name not in merged.synonym_names:
                merged.synonym_names.append(self.name)

            # some tags don't show up in the main syn page e.g. "https://archiveofourown.org/tags/wwii%20supernatural"
            # Add point this tag's name to the merged tag in the cache
            with Tag._cache_lock:
                Tag._cache[self.name] = merged
                
        elif len(self.synonym_names)>0:
            # if not merged, point any tags that have been merged with it to this one in memory
            Tag._addSynonymsToCache(self)

        if not Tag._lazy_evaluation:
            # Get all the metadata and delete the BeautifulSoup
            self.parse()
            
        
        
    @cached_property
    def query_error(self):
        if isinstance(self._soup,BeautifulSoup):
            if self._soup.find("div", {"class","flash error"}) is not None:
                return 303
            h = self._soup.find("h2", {"class", "heading"}).text
            if "Error 404" in h:
                return 404
            else:
                return False
        else:
            return False
    
    # Purely for testing
    def addParentTagnames(self,tagname_list):
        self.parent_tagnames = tagname_list
        return True

    # Purely for testing
    def addMetaTagnames(self,tagname_list):
        self.meta_tagnames = tagname_list
        return True
    
    @cached_property
    def parent_names(self):
        # Would load from soup in final
        if not self.loaded:
            raise utils.UnloadedError("Cannot load parent tags if Tag not loaded")
        elif self.query_error:
            raise utils.UnexpectedResponseError(f"Cannot load parent tags if Tag query threw Error {self.query_error}")
        else:
            html = self._soup.find("div", {"class": re.compile("(parent|parent fandom) listbox group")})
            if html is None:
                return []
            return [str(t.text) for t in html.find_all('li')]


    def get_parents(self,immediate=False):
        '''
        Returns the unique metatags after handling merges.
        When a Tag is found to be merged, the cache redirects the old
        reference to the new main tag. Using the plaintext
        name to fetch from the cache avoids getting deprecated tags.
        '''
        if not self.parent_names:
            return []
        if immediate:            
            return list(set(map(lambda t: Tag(t,load=False,session=self._session),self.immediate_parent_names)))
        return list(set(map(lambda t: Tag(t,load=False,session=self._session),self.parent_names)))
    
    @property
    def parsed(self):
        return isinstance(self._soup,bool)
    
    @property
    def loaded(self):
        return self._soup is not None
        
    @cached_property
    def category(self):
        if not self.loaded:
            raise utils.UnloadedError("Cannot load category if Tag not loaded")
        elif self.query_error:
            raise utils.UnexpectedResponseError(f"Cannot load category if Tag query threw Error {self.query_error}")
        else:
            return str(re.search(r'This tag belongs to the (.+) Category\.',self._soup.find("div",{'class':'tag home profile'}).find('p').text).group(1))
    
    @cached_property
    def canonical(self):
        if not self.loaded:
            raise utils.UnloadedError("Cannot tell if tag is canonical if Tag not loaded")
        elif self.query_error:
            raise utils.UnexpectedResponseError(f"Cannot tell if tag is canonical if Tag query threw Error {self.query_error}")
        else:
            # On tag pages, tags are still listed as common not canonical.
            # The method matches the terminology found elsewhere
            return len(re.findall(r"It\'s a common tag",self._soup.find("div",{'class':'tag home profile'}).find('p').text)) == 1
    
    @cached_property
    def metatag_names(self):
        '''
        Returns the names of all metatags.

        Returns
        -------
        list: list of metatags names.

        '''
        if not self.loaded:
            raise utils.UnloadedError("Cannot load metatags if Tag not loaded")
        elif self.query_error:
            raise utils.UnexpectedResponseError(f"Cannot load metatags if Tag query threw Error {self.query_error}")
        else:
            html = self._soup.find("div", {"class": "meta listbox group"})
            if html is None:
                return []
            # If statement to check if on the upper level of the tree
            #return [t.a.text for t in html.ul.children if t.contents[0].name=='a']
            return [str(t.text) for t in html.find_all("a")]
    
    #@cached_property
    #def immediate_parent_names(self):
        '''
        Returns the names of all metatags immediately above this one in the hierarchy.

        Returns
        -------
        list: list of metatags names.

        '''
        if not self.loaded:
            raise utils.UnloadedError("Cannot load parents if Tag not loaded")
        elif self.query_error:
            raise utils.UnexpectedResponseError(f"Cannot load parents if Tag query threw Error {self.query_error}")
        else:
            html = self._soup.find("div", {"class": re.compile("(parent|parent fandom) listbox group")})
            if html is None:
                return []
            # If statement to check if on the upper level of the tree
            return [str(t.a.text) for t in html.ul.children if t.contents[0].name=='a']
            #return [t.text for t in html.find_all("a")]

    
    @cached_property
    def immediate_metatag_names(self):
        '''
        Returns the names of all metatags immediately above this one in the hierarchy.

        Returns
        -------
        list: list of metatags names.

        '''
        if not self.loaded:
            raise utils.UnloadedError("Cannot load metatags if Tag not loaded")
        elif self.query_error:
            raise utils.UnexpectedResponseError(f"Cannot load metatags if Tag query threw Error {self.query_error}")
        else:
            html = self._soup.find("div", {"class": "meta listbox group"})
            if html is None:
                return []
            # If statement to check if on the upper level of the tree
            return [str(t.a.text) for t in html.ul.children if t.contents[0].name=='a']
            #return [t.text for t in html.find_all("a")]


    def get_metatags(self,immediate=False):
        '''
        Returns the unique metatags after handling merges.
        When a Tag is found to be merged, the cache redirects the old
        reference to the new main tag. Using the plaintext
        name to fetch from the cache avoids getting deprecated tags.
        '''
        if not self.metatag_names:
            return []
        if immediate:
            return list(set(map(lambda t: Tag(t,load=False,session=self._session),self.immediate_metatag_names)))
        return list(set(map(lambda t: Tag(t,load=False),self.metatag_names)))
    
    @cached_property
    def synonym_names(self):
        '''
        Returns the names of all tags made synonymous to this one.
        Once a tag has been merged, it becomes deprecated. The Tag cache automatically prunes
        these, so this method only returns the names.
        
        If a tag is later made synonymous to another tag, its list of synonyms will
        be updated to include the new name.
        -------
        list: list of synonym names.
        '''
        if not self.loaded:
            raise utils.UnloadedError("Cannot load synonyms if Tag not loaded")
        elif self.query_error:
            raise utils.UnexpectedResponseError(f"Cannot load synonyms if Tag query threw Error {self.query_error}")
        else:
            html = self._soup.find("div", {"class": "synonym listbox group"})
            if html is None:
                return []
            return [str(t.text) for t in html.find_all('li')]
    
    @cached_property
    def merged_name(self):
        '''
        Checks if the current tag has been merged with another. If it has, it returns that Tag's name.
        '''
        if not self.loaded:
            raise utils.UnloadedError("Cannot load merges if Tag not loaded")
        elif self.query_error:
            raise utils.UnexpectedResponseError(f"Cannot load merges if Tag query threw Error {self.query_error}")
        else:
            html = self._soup.find("div", {"class": "merger module"})
            if html is None:
                return False
            else:
                search = re.search(r'has been made a synonym of (.+). Works and bookmarks tagged with',str(html.p.text))
                return (str(search.group(1)))
    

    def get_merged(self):
        '''
        Finds the merged tag in the cache. Not a cached property on the off-chance the tag
        this tag got merged to also got merged. I'm not sure if this can happen with how AO3 is
        set up, but until I find out otherwise, this method's here.
        '''
        m = self.merged_name
        if m:
            return Tag(m,load=False,session=self._session)
        else:
            return False
    
    @cached_property
    def children_names(self):
        '''
        Returns child tags organized into a dictionary by tag category
        Gives a warning if the 300 tag display limit is met.

        Returns
        -------
        dict: dictionary of lists of child tags.

        '''
        if not self.loaded:
            raise utils.UnloadedError("Cannot load child tags if Tag not loaded")
        elif self.query_error:
            raise utils.UnexpectedResponseError(f"Cannot load child tags if Tag query threw Error {self.query_error}")
        else:
            html = self._soup.find("div", {"class": "child listbox group"})
            
            
            children_dict = {}
            if html is None:
                return children_dict
            
            children_html = html.find_all("div", {"class": re.compile("(.+) listbox group")})
            categories = [str(t.attrs['class'][0][0:-1]).capitalize() for t in children_html]
            # Freeform Category is now the Additional Tags Category, so make the change here
            try:
                i=categories.index("Freeform")
                categories[i]="Additional Tags"
            except:
                pass
            
            
            for (category,cat_html) in zip(categories,children_html):
                children_dict[category]=[str(t.text) for t in cat_html.find_all('li')]
                if len(children_dict[category]) >= 300:
                    warnings.warn(f"The <{category}> child tags of <{self.name}> were truncated to 300.\n Additional tags may exist.", stacklevel=2)
            return children_dict
        

    def get_children(self):
        '''
        Wraps the names of all children in Tag objects and returns unique Tags.
        This method isn't cached to better handle merges.
        '''
        d = {}
        for (cat,name_list) in self.children_names.items():
            d[cat] = list(set(map(lambda t: Tag(t,load=False,session=self._session),name_list)))
        return d
        
    @cached_property
    def subtag_names(self):
        '''
        Returns all metatags immediatey below in the heirarchy.
        
        For example, "SHAKESPEARE William - Works" has "Twelfth Night - Shakespeare" as a subtag,
        and inherits the "Twelfth Night (1988)" subtag from "Twelfth Night - Shakespeare"
        
        The Tag class as constructed is intended to be graph-like, so it just makes more sense
        to only return "Twelfth Night - Shakespeare" even if the number of requests to AO3 is larger
        under this implementation should you use the children_recursive method (which hasn't been implemented yet).
        
        Note this returns the plaintext names. When a Tag is found to be merged,
        the cache redirects the old reference to the new main tag. Using the plaintext
        name to fetch from the cache avoids getting deprecated tags.
        
        Returns
        -------
        list: list of subtag names.
        '''
        if not self.loaded:
            raise utils.UnloadedError("Cannot load subtags if Tag not loaded")
        elif self.query_error:
            raise utils.UnexpectedResponseError(f"Cannot load subtags if Tag query threw Error {self.query_error}")
        else:
            html = self._soup.find("div", {"class": "sub listbox group"})
            if html is None:
                return []
            # If statement to check if on the upper level of the tree
            #return [t.a.text for t in html.ul.children if t.contents[0].name=='a']
            return [str(t.text) for t in html.find_all("a")]
    
    
    @cached_property
    def immediate_subtag_names(self):
        '''
        Returns all metatags immediatey below in the heirarchy.
        
        For example, "SHAKESPEARE William - Works" has "Twelfth Night - Shakespeare" as a subtag,
        and inherits the "Twelfth Night (1988)" subtag from "Twelfth Night - Shakespeare"
        
        The Tag class as constructed is intended to be graph-like, so it just makes more sense
        to only return "Twelfth Night - Shakespeare" even if the number of requests to AO3 is larger
        under this implementation should you use the children_recursive method (which hasn't been implemented yet).
        
        Note this returns the plaintext names. When a Tag is found to be merged,
        the cache redirects the old reference to the new main tag. Using the plaintext
        name to fetch from the cache avoids getting deprecated tags.
        
        Returns
        -------
        list: list of subtag names.
        '''
        if not self.loaded:
            raise utils.UnloadedError("Cannot load subtags if Tag not loaded")
        elif self.query_error:
            raise utils.UnexpectedResponseError(f"Cannot load subtags if Tag query threw Error {self.query_error}")
        else:
            html = self._soup.find("div", {"class": "sub listbox group"})
            if html is None:
                return []
            # If statement to check if on the upper level of the tree
            return [str(t.a.text) for t in html.ul.children if t.contents[0].name=='a']
            #return [t.text for t in html.find_all("a")]
    
    def get_subtags(self,immediate=False):
        '''
        Returns the unique subtags after handling merges.
        When a Tag is found to be merged, the cache redirects the old
        reference to the new main tag. Using the plaintext
        name to fetch from the cache avoids getting deprecated tags.
        '''
        if not self.subtag_names:
            return []
        if immediate:
            return list(set(map(lambda t: Tag(t,load=False,session=self._session),self.immediate_subtag_names)))
        return list(set(map(lambda t: Tag(t,load=False,session=self._session),self.subtag_names)))
    
    @cached_property
    def url(self):
    # Do the following character substititions, but reversed
    # '/' with '*s*'
    # '&' with '*a*'
    # '.' with *d*
    # '?' with *q*
        return utils.urlext_from_tagname(self.name)

    @property
    def metadata(self):
        metadata = {}
        
        if self.loaded and not self.query_error:
            normal_fields = (
                "loaded",
                "query_error",
                "canonical"
            )
            string_fields = (
                "name",
                "category",
                "date_queried"
            )
            string_list_fields = (
                "parent_names",
                "metatag_names",
                "subtag_names",
                "immediate_metatag_names",
                "immediate_subtag_names",
                "synonym_names"
            )
        else:
            normal_fields = (
                "loaded",
                "query_error"
            )
            string_fields = (
                "name"
            )
            string_list_fields = ()
        for field in string_fields:
            try:
                metadata[field] = str(getattr(self, field))
            except AttributeError:
                pass
            
        for field in normal_fields:
            try:
                metadata[field] = getattr(self, field)
            except AttributeError:
                pass
        for field in string_list_fields:
            try:
                metadata[field] = list(map(str,getattr(self, field)))
            except AttributeError:
                pass

        if self.loaded and not self.query_error:
            d = self.children_names
            metadata["children"] = dict(zip(list(d.keys()),list(map(lambda l: list(map(str,l)),d.values()))))

        return metadata
    
        
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
        if len(req.content) > 650000:
            warnings.warn("This work is very big and might take a very long time to load.", stacklevel=2)
        soup = BeautifulSoup(req.content, "lxml")
        return soup

    def parse(self):
        if self.loaded:
            # Compute all cached properties involving _soup
            _ = self.metadata
            # Override _soup to make loaded read as true
            self._soup = True