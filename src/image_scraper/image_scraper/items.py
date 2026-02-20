import scrapy

class ImageScraperItem(scrapy.Item):
    image_urls = scrapy.Field()
    animal = scrapy.Field()
