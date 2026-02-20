import os
from typing import Any, Iterator, Optional

import scrapy
from itemadapter import ItemAdapter
from scrapy.http import Request, Response
from scrapy.pipelines.images import ImagesPipeline

from image_scraper.items import ImageScraperItem


class ImageScraperPipeline:
    def process_item(self, item: ImageScraperItem, spider: scrapy.Spider) -> ImageScraperItem:
        return item


class AnimalImagesPipeline(ImagesPipeline):
    """Saves images to IMAGES_STORE/{animal}/. Caps at IMAGES_MAX_PER_ANIMAL per animal (default 900)."""

    def get_media_requests(self, item: ImageScraperItem, info: Any) -> Iterator[Request]:
        """
        Get media requests for the given item.

        Arguments
        ----------
        item : ImageScraperItem
            The item to get media requests for.
        info : Any
            Pipeline internal state (SpiderInfo).

        Yields
        ------
        Request
            A request for the image URL.
        """
        adapter = ItemAdapter(item)
        animal = adapter.get("animal", "unknown")
        urls = adapter.get("image_urls", [])

        max_per_animal = self.crawler.settings.getint("IMAGES_MAX_PER_ANIMAL", 900)
        store_path = self.crawler.settings.get("IMAGES_STORE", "images")
        subfolder = animal.replace(" ", "_")
        animal_dir = os.path.join(store_path, subfolder)
        if os.path.isdir(animal_dir):
            current = sum(
                1 for f in os.listdir(animal_dir)
                if os.path.isfile(os.path.join(animal_dir, f)) and f.lower().endswith((".jpg", ".jpeg", ".png"))
            )
        else:
            current = 0

        remaining = max(0, max_per_animal - current)
        for url in urls[:remaining]:
            yield scrapy.Request(url, meta={"animal": animal})

    def file_path(
        self,
        request: Request,
        response: Optional[Response] = None,
        info: Optional[Any] = None,
        *, item: Optional[ImageScraperItem] = None,
    ) -> str:
        """
        Get the file path for the given item.

        Arguments
        ----------
        request : Request
            The request to get the file path for.
        response : Response, optional
            The response to get the file path for.
        info : Any, optional
            Pipeline internal state.
        item : ImageScraperItem, optional
            The item being processed.

        Returns
        -------
        str
            The file path for the given item.
        """
        animal = request.meta.get("animal", "unknown")
        subfolder = animal.replace(" ", "_")
        try:
            path = super().file_path(request, response, info, item=item)
        except TypeError:
            path = super().file_path(request, response, info)
        # path is like "full/<hash>.jpg"
        name = path.split("/")[-1] if "/" in path else path
        return f"{subfolder}/{name}"
