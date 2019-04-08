import logging
import re
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class Crawler:

    def __init__(self, frontier):
        self.frontier = frontier

    def start_crawling(self):
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ...", url)
            url_data = self.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    self.frontier.add_url(next_link)

    def fetch_url(self, url):
        """
        This method, using the given url, should find the corresponding file in the corpus, find the encoding of the
        content (hint: you can use chardet library to do this), decode the content and encode it again in UTF-8 (hint:
        you can do this using the str built-in function) and return a dictionary containing the url, content and content
        size
        :param url: the url to be fetched
        :return: a dictionary containing the url, UTF-8 encoded content and the size of the content
        """
        url_data = {
            "url": url,
            "content": "",
            "size": 0
        }
        return url_data

    def extract_next_links(self, url_data):
        """
        This method will be given the url data coming from the fetch_url method. url_data contains the fetched url, the
        url content encoded in UTF-8, and the size of the content. This method should return a list of urls in their
        absolute form (some links in the content are relative and needs to be converted to the absolute form).
        Validation of links is done later via is_valid method.
        It is not required to remove duplicates that have already been downloaded.
        The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = []
        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be downloaded or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
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

