import datetime

from . import utils


def __setifnotnone(obj, attr, value):
    if value is not None:
        setattr(obj, attr, value)

def get_work_from_banner(work):
    #* These imports need to be here to prevent circular imports
    #* (series.py would requite common.py and vice-versa)
    from .series import Series
    from .users import User
    from .works import Work
    
    authors = []
    try:
        for l in work.h4.find_all("a"):
            if 'rel' in l.attrs.keys():
                if "author" in l['rel']:
                    authors.append(User(str(l.string), load=False)) # cast as string for pickling
            elif l.attrs["href"].startswith("/works"):
                workname = str(l.string)
                workid = utils.workid_from_url(l['href'])
    except AttributeError:
        pass
            
    new = Work(workid, load=False)

    fandoms = []
    try:
        for l in work.find("h5", {"class": "fandoms"}).find_all("a"):
            fandoms.append(str(l.string))
    except AttributeError:
        pass

    warnings = []
    relationships = []
    characters = []
    freeforms = []
    try:
        for l in work.find(attrs={"class": "tags"}).find_all("li"):
            if "warnings" in l['class']:
                warnings.append(utils.tagname_from_href(l.a['href']))
            elif "relationships" in l['class']:
                relationships.append(utils.tagname_from_href(l.a['href']))
            elif "characters" in l['class']:
                characters.append(utils.tagname_from_href(l.a['href']))
            elif "freeforms" in l['class']:
                freeforms.append(utils.tagname_from_href(l.a['href']))
    except AttributeError:
        pass

    reqtags = work.find(attrs={"class": "required-tags"})
    if reqtags is not None:
        rating = reqtags.find(attrs={"class": "rating"})
        if rating is not None:
            rating = rating.text
        categories = reqtags.find(attrs={"class": "category"})
        if categories is not None:
            categories = categories.text.split(", ")
    else:
        rating = categories = None

    summary = work.find(attrs={"class": "userstuff summary"})
    if summary is not None:
        summary = summary.text

    series = []
    series_list = work.find(attrs={"class": "series"})
    if series_list is not None:
        for l in series_list.find_all("a"):
            seriesid = int(l.attrs['href'].split("/")[-1])
            seriesname = l.text
            s = Series(seriesid, load=False)
            setattr(s, "name", seriesname)
            series.append(s)

    stats = work.find(attrs={"class": "stats"})
    if stats is not None:
        language = stats.find("dd", {"class": "language"})
        if language is not None:
            language = language.text
        words = stats.find("dd", {"class": "words"})
        if words is not None:
            words = words.text.replace(",", "")
            if words.isdigit(): words = int(words)
            else: words = None
        bookmarks = stats.find("dd", {"class": "bookmarks"})
        if bookmarks is not None:
            bookmarks = bookmarks.text.replace(",", "")
            if bookmarks.isdigit(): bookmarks = int(bookmarks)
            else: bookmarks = None
        chapters = stats.find("dd", {"class": "chapters"})
        if chapters is not None:
            chapters = chapters.text.split('/')[0].replace(",", "")
            if chapters.isdigit(): chapters = int(chapters)
            else: chapters = None
        expected_chapters = stats.find("dd", {"class": "chapters"})
        if expected_chapters is not None:
            expected_chapters = expected_chapters.text.split('/')[-1].replace(",", "")
            if expected_chapters.isdigit(): expected_chapters = int(expected_chapters)
            else: expected_chapters = None
        hits = stats.find("dd", {"class": "hits"})
        if hits is not None:
            hits = hits.text.replace(",", "")
            if hits.isdigit(): hits = int(hits)
            else: hits = None
        kudos = stats.find("dd", {"class": "kudos"})
        if kudos is not None:
            kudos = kudos.text.replace(",", "")
            if kudos.isdigit(): kudos = int(kudos)
            else: kudos = None
        comments = stats.find("dd", {"class": "comments"})
        if comments is not None:
            comments = comments.text.replace(",", "")
            if comments.isdigit(): comments = int(comments)
            else: comments = None
        restricted = work.find("img", {"title": "Restricted"}) is not None
        if chapters is None:
            complete = None
        else:
            complete = chapters == expected_chapters
    else:
        language = words = bookmarks = chapters = expected_chapters = hits = restricted = complete = None

    date = work.find("p", {"class": "datetime"})
    if date is None:
        date_updated = None
    else:
        date_updated = datetime.datetime.strptime(date.getText(), "%d %b %Y")
        
    date_queried = datetime.datetime.now()

    __setifnotnone(new, "authors", authors)
    __setifnotnone(new, "bookmarks", bookmarks)
    __setifnotnone(new, "categories", categories)
    __setifnotnone(new, "nchapters", chapters)
    __setifnotnone(new, "characters", characters)
    __setifnotnone(new, "complete", complete)
    __setifnotnone(new, "date_updated", date_updated)
    __setifnotnone(new, "expected_chapters", expected_chapters)
    __setifnotnone(new, "fandoms", fandoms)
    __setifnotnone(new, "hits", hits)
    __setifnotnone(new, "comments", comments)
    __setifnotnone(new, "kudos", kudos)
    __setifnotnone(new, "language", language)
    __setifnotnone(new, "rating", rating)
    __setifnotnone(new, "relationships", relationships)
    __setifnotnone(new, "restricted", restricted)
    __setifnotnone(new, "series", series)
    __setifnotnone(new, "summary", summary)
    __setifnotnone(new, "freeforms", freeforms)
    __setifnotnone(new, "title", workname)
    __setifnotnone(new, "warnings", warnings)
    __setifnotnone(new, "words", words)
    __setifnotnone(new, "date_queried", date_queried)
    
    return new

def url_join(base, *args):
    result = base
    for arg in args:
        if len(result) > 0 and not result[-1] == "/":
            result += "/"
        if len(arg) > 0 and arg[0] != "/":
            result += arg
        else:
            result += arg[1:]
    return result