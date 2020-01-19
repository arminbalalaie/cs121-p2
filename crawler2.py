import logging
import operator
import re
from collections import defaultdict
from urllib.parse import urlparse, urldefrag

from lxml import html

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.history = defaultdict(int)
        self.repeat_pattern = re.compile(r'.*?([^\/\&?]{4,})(?:[\/\&\?])(.*?\1){3,}.*')

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

        for trap in sorted(self.history.items(), key=operator.itemgetter(1))[-20:]:
            print("%s: %s" % (trap[0], trap[1]))

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """

        url = url_data["url"] if not url_data["is_redirected"] else url_data["final_url"]
        content = url_data["content"]
        parsed = urlparse(url)
        history_key = parsed.netloc + parsed.path
        self.history[history_key] += 1

        outputLinks = []

        if not content:
            return outputLinks
        try:
            tree = html.fromstring(content)
            tree.make_links_absolute(url)
            for link in tree.iterlinks():
                outputLinks.append(urldefrag(link[2])[0])
        except:
            logger.warn("Error while parsing %s", url_data["url"])

        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        history_key = parsed.netloc + parsed.path
        if self.history[history_key] > 100:
            return False

        if len(url) > 100:
            return False

        if parsed.scheme not in set(["http", "https"]):
            return False

        if re.match(self.repeat_pattern, url): return False

        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False

