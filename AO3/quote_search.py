from math import ceil

from bs4 import BeautifulSoup

from . import threadable, utils
from .common import get_work_from_banner
from .requester import requester
from .series import Series
from .users import User
from .works import Work
from .search import Search
from .chapters import Chapter

"""
quote_search = QuoteSearch(user_quote, Search.pages, Search.results)
"""

class QuoteSearch:
    def __init__(
        self,
        user_quote="",
        pages_to_search=0,
        works_to_search=None):

        #search results are passed in
        self.user_quote = user_quote
        self.pages_to_search = pages_to_search
        self.works_to_search = works_to_search

        self.results = None
        self.pages = 0
        self.total_results = 0

    def _get_snippets(self, chapter_text):
        positions = []
        snippets = []
        search_start = 0
        snippet_start = -1
        while True:
            search_start = chapter_text.find(self.user_quote, search_start)
            #this section sets the start position of the snippet that is going to be added next
            if snippet_start == -1:
                snippet_start = search_start - 60 if search_start - 60 > 0 else 0
            else:
                #this section adds any completed snippet to the list
                #if its the last occurence of the quote, or if the next is outside of the adjacency radius
                if search_start == -1 or search_start > snippet_start+len(self.user_quote)+60:
                    snippet_end = positions[-1] + len(self.user_quote) + 60
                    #ellipses are added at start/end if its not the start/end of the chapter
                    snippets.append("..." if snippet_start - 60 < 0 else "" + chapter_text[
                        snippet_start - 60 if snippet_start - 60 > 0 else 0
                        :snippet_end if snippet_end < len(chapter_text) else len(chapter_text)
                        ] + "..." if snippet_end >= len(chapter_text) else "")
                    snippet_start = -1
            if search_start == -1:
                break
            positions.append(search_start)
            if len(positions) > 4:
                #No snippets are returned to save space on the page
                return ["Multiple occurrences"]
            
            search_start += len(self.user_quote)

        return snippets

    @threadable.threadable
    def update(self):
        if self.pages_to_search != 1:
            self.results = []
            self.total_results = 0
            self.pages = 0
            return

        works = []
        for work in self.works_to_search:
            work.load_chapters()
            chapters_with_quote = []
            for chapter in work.chapters:
                chapter = Chapter(chapter)
                snippets = self._get_snippets(self, chapter.text)
                #if there are no occurrences of the quote in the chapter text an empty list is returned
                #which evaluates to False
                if snippets:
                    chapters_with_quote.append([chapter.str_no_work, snippets])
            if chapters_with_quote:
                work.snippets = chapters_with_quote
                works.append(work)

        self.results = works
        self.total_results = len(self.results)    
        self.pages = 1
