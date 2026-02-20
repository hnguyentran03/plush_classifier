import re
import json
import html
import scrapy
from urllib.parse import quote_plus

from typing import Iterator, List
from scrapy.http import Response
from image_scraper.items import ImageScraperItem


class BingImagesSpider(scrapy.Spider):
    name = "bing_images"
    # Empty so pipeline can download images from any host (Bing only links to them)
    allowed_domains = []

    def __init__(self, queries_json: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # queries_json: list of (animal, query) as JSON string
        self._pairs = []
        if queries_json:
            try:
                self._pairs = json.loads(queries_json)
            except (json.JSONDecodeError, TypeError):
                pass

    def start_requests(self) -> Iterator[scrapy.Request]:
        """
        Generate requests for each (animal, query) pair.

        Yields
        ------
        scrapy.Request
            A request for the Bing image search for the given (animal, query) pair.
        """
        if not self._pairs:
            self.logger.warning("No (animal, query) pairs. Add queries_json spider arg.")
            return
        for animal, query in self._pairs:
            url = "https://www.bing.com/images/search?q=" + quote_plus(query)
            yield scrapy.Request(
                url,
                meta={"animal": animal},
                callback=self.parse,
            )

    def parse(self, response: Response) -> Iterator[ImageScraperItem]:
        """
        Parse the response and yield an ImageScraperItem.

        Arguments
        ----------
        response : scrapy.http.Response
            The response to parse.

        Yields
        ------
        ImageScraperItem
            An item with the image URLs and animal name.
        """
        animal = response.meta.get("animal", "unknown")
        image_urls = self._extract_image_urls(response)
        if not image_urls:
            self.logger.debug("No image URLs found for animal=%s url=%s", animal, response.url)
            return
        yield ImageScraperItem(
            image_urls=image_urls,
            animal=animal,
        )

    def _extract_image_urls(self, response: Response) -> List[str]:
        """
        Extract the image URLs from the response.

        Arguments
        ----------
        response : scrapy.http.Response
            The response to extract image URLs from.
        """
        seen = set()
        urls = []

        # Bing often embeds JSON with murl in script or in data-m
        text = response.text
        for m in re.finditer(r'"murl"\s*:\s*"((?:https?://[^"]+?)(?:\\u0026|[&])[^"]*)"', text):
            url = m.group(1).replace("\\u0026", "&").strip()
            if url.startswith("http") and url not in seen:
                seen.add(url)
                urls.append(url)
        for m in re.finditer(r'"murl"\s*:\s*"(https?://[^"]+)"', text):
            url = m.group(1).replace("\\/", "/").strip()
            if url not in seen:
                seen.add(url)
                urls.append(url)

        # Anchor tags with data-m (HTML-encoded JSON with murl)
        for sel in response.css("a[data-m]"):
            data_m = sel.attrib.get("data-m")
            if not data_m:
                continue
            try:
                decoded = html.unescape(data_m)
                obj = json.loads(decoded)
                murl = obj.get("murl") or obj.get("m")
                if isinstance(murl, str) and murl.startswith("http") and murl not in seen:
                    seen.add(murl)
                    urls.append(murl)
            except (json.JSONDecodeError, TypeError):
                pass

        # img.mimg or img with data-src / src in results
        for img in response.css("img.mimg"):
            url = img.attrib.get("data-src") or img.attrib.get("src")
            if url and url.startswith("http") and url not in seen:
                seen.add(url)
                urls.append(url)
        for img in response.css("img[data-src]"):
            url = img.attrib.get("data-src")
            if url and url.startswith("http") and url not in seen:
                seen.add(url)
                urls.append(url)

        return urls
