import scrapy, json # , unicodedata, re
from datetime import datetime, date, timezone
from copy import deepcopy
from re import sub
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import SitemapSpider #, CrawlSpider, Rule, XMLFeedSpider
from scrapy.loader import ItemLoader
from urllib.parse import urlsplit, parse_qs
from scrapy_caliber.items import ProductItem
from w3lib.html import replace_escape_chars, remove_tags, replace_entities
from collections import Counter
# from scrapy_splash import SplashRequest

# Current time does NOT include seconds because it potentially creates 
# two different directories in S3 bucket by a difference of only 1 second
# ex. 20231114085252/ ===> containes product images | 20231114085253/ ===> contains jsonline batches

current_time = datetime.now().strftime("%Y%m%d%I%M")

# def distinguish_categories(unknow_category):

class KatespadeSpider(SitemapSpider):
    # start_urls = ["https://www.katespade.com/]

    name = 'katespade'
    allowed_domains = ['katespade.com', 'images.katespade.com']

    # start_urls = [
    sitemap_urls = [
        'https://www.katespade.com/sitemap_0-product.xml',
    ]

    # handle_httpstatus_list = [301]

    sitemap_rules = [
        # ('/en-us/', 'parse_category'),
        ('https://www.katespade.com/api/products/', 'parse_product')
    ]


    # Configurations for HTTP headers and Scrapy's settings (i.e., settings.py)
    headers = {
        # "Host": "c.go-mpulse.net",
        "Origin": "https://www.katespade.com",
        "Referer": "https://www.katespade.com/",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0, no-cache, no-store",
        "Upgrade-Insecure-Requests": "1",
        "Content-Encoding": "gzip",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        # "Accept": "text/html,application/json,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept": "application/json",
        # "Accept": "*/*",
        # "DNT": "1",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9"
    }

    custom_settings = {
        'IMAGES_STORE': 's3://scrapy-caliber/downloads/' + name + "/",
        'FEEDS': {
            's3://scrapy-caliber/downloads/%(name)s/%(current_time)s/batched_jsonlines/%(batch_id)d-%(name)s_%(batch_time)s.jsonl': {
                'format': 'jsonlines', 
                'encoding': 'utf-8',
                'overwrite': False,
                'store_empty': False,
                # 'batch_item_count': 100
            }
        },
        'AUTOTHROTTLE_ENABLED': True,
        'ITEM_PIPELINES': {
            'scrapy_caliber.pipelines.ScrapyCaliberPipeline': 400,
            'scrapy_caliber.pipelines.OrganizeProductImagesPipeline': 300
            # 'scrapy.pipelines.images.ImagesPipeline': 300
        },
        # 'IMAGES_THUMBS': {
        #     'small': (50, 50),
        #     'medium': (370, 370),
        #     'large': (750, 750)
        # },
        # 'USER_AGENT': 'Pinterestbot',
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'DOWNLOAD_TIMEOUT': 60,
        # 'CONCURRENT_REQUESTS': 5,
        # 'DOWNLOAD_DELAY': 2,
        # 'DEFAULT_REQUEST_HEADERS': headers
    }


    def sitemap_filter(self, entries):
        # self.logger.info('\tSITEMAP FILTER - %s', inspect(entries))
        for entry in entries:
            # self.logger.info('\tSITEMAP FILTER - %s', entry)
            entry['loc'] = sub('https://www.katespade.com/products/', 'https://www.katespade.com/api/products/', entry['loc'])
            # self.logger.info('\tSITEMAP FILTER - %s', entry)
            if "-bundle" not in entry['loc']:
                yield entry
            else:
                continue


    # def parse_category(self, response):
    #    get_product_links = response.css(" a::attr(href)").getall()
    #    
    #    for product_link in get_product_links:
    #        yield scrapy.Request(
    #            url=product_link,
    #            callback=self.parse_product,
    #            cb_kwargs={}                
    #        )


    def parse_product(self, response, html_product_page=None): # cb_gender=None, cb_category=None, cb_subcategory=None, cb_category_list=None):
        product_data = json.loads(response.body)['pageData']

        il = ItemLoader(item=ProductItem())

        il.add_value('brand_id', None)
        il.add_value('category_id', None)

        # product_title = next((element for element in cms_content_elements if element.get('type', None) == "PRODUCTTITLE"), None)
        # product_variations = next((element for element in cms_content_elements if isinstance(element, dict) and element.get('type', None) == "PRODUCTVARIATIONS"), None)
        # product_unique = next((element for element in cms_content_elements if isinstance(element, dict) and element.get('type', None) == "PRODUCTUNIQUE"), None)
        # get_breadcrumbs = next((element for element in cms_content_elements if isinstance(element, dict) and element.get('type', None) == "BREADCRUMB"), None)
        # product_declinations = next((element.get('declinations', None) for element in cms_content_elements if isinstance(element, dict) and element.get('type', None) == "PRODUCTDECLINATIONS"), None)
        # product_section_description = next((element for element in cms_content_elements if isinstance(element, dict) and element.get('type', None) == "PRODUCTSECTIONDESCRIPTION"), None)
        # product_section_sizefit = next((element for element in cms_content_elements if isinstance(element, dict) and element.get('type', None) == "PRODUCTSECTIONSIZEFIT"), None)
        # product_medias = next((element for element in cms_content_elements if element.get('type', None) == "PRODUCTMEDIAS"), None)

        il.add_value('name', product_data['name'])

        il.add_value('brand_title', "kate spade")

        get_product_url = "https://www.katespade.com/" + product_data['url']
        il.add_value('product_url', get_product_url)

        il.add_value('product_json_url', response.url)

        il.add_value('upid', product_data['id'])

        il.add_value('retail_price', product_data['prices']['currentPrice'])

        il.add_value('retail_price_cents', product_data['prices']['currentPrice'])

        get_images = [img['src'] + "?$desktopProductZoom$" for img in product_data['media']['full']]
        il.add_value('image_urls', get_images)

        il.add_value('gender', 'women')

        il.add_value('body_part', 'n/a')

        

        ### OPTIONAL - RECOMMENDED ###
        # il.add_value('super_category', product_data[''])
        valid_parent_categories = ['handbags', 'wallets', 'shoes', 'clothing', 'jewelry', 'accessories']
        excluded_categories = ['just dropped', 'gift shop', 'designer home', 'sale', 'social impact', 'preloved', 'view all']

        # Deepcopy breadcrumbs to remove unwanted elements without altering original data
        get_breadcrumbs = deepcopy(product_data.get('breadcrumbs', None))

        # Remove last category, which is just the product's name
        # last_category_using_product_name = next((bc for breadcrumb in get_breadcrumbs if (bc := breadcrumb.get('htmlValue', None)) == product_data['name']), None)
        if get_breadcrumbs is not None and get_breadcrumbs[-1] == product_data['name']:
            get_breadcrumbs.remove(last_category_using_product_name)

        get_category = next((bc for breadcrumb in get_breadcrumbs if (bc := breadcrumb.get('htmlValue', None).lower()) in valid_parent_categories and bc not in excluded_categories), None)
        il.add_value('category', get_category)
        # get_breadcrumbs.remove(get_category)

        get_subcategory = next((bc for breadcrumb in get_breadcrumbs if (bc := breadcrumb.get('htmlValue', None).lower()) not in valid_parent_categories and bc not in excluded_categories), None)
        il.add_value('subcategory', get_subcategory)

        il.add_value('description', product_data.get('shortDescription', None))

        if product_data.get('longDescription', None) is not None and product_data.get('longDescription', None).startswith("<Ul>"):
            sanitized_list_items = product_data.get('longDescription', None).replace("<Ul>", "").replace("</Ul>", "")
            get_details_list = [list_item.replace("<li>", "").replace("</li>", "").strip() for list_item in sanitized_list_items.split("</li><li>") if list_item != "&nbsp"]
            
            il.add_value('detail_list', get_details_list)

            if "MEASUREMENTS" in get_details_list:
                get_dimensions = get_details_list.index("MEASUREMENTS") + 1
                il.add_value('dimensions', get_dimensions)

        il.add_value('detail_list_styled', product_data.get('longDescription', None))

        il.add_value('color', product_data['selectedColor']['text'].lower())



        # il.add_value('subtitle', product_data['subtitle'])

        # il.add_value('sku', product_data['sku'])
        # il.add_value('uuid', product_data['uuid'])

        # il.add_value('composition', product_data['composition'])

        # il.add_value('materials', product_data['materials'])

        # il.add_value('ingredients', product_data['ingredients'])

        # il.add_value('season', product_data['season'])

        # il.add_value('collection', product_data['collection'])

        

        ### OPTIONAL ###
        # il.add_value('category_data', '')

        # il.add_value('is_variant_of', '')
        get_color_variant = product_data.get('selectedColor', None)
        il.add_value('color_variant', get_color_variant)

        il.add_value('variant_colors', product_data.get('colors', None).remove(get_color_variant))

        # il.add_value('color_data', '')
        # il.add_value('pattern', '')
        # il.add_value('size', '')
        # il.add_value('variant_sizes', '')

        # il.add_value('description_styled', product_data.get('longDescription', None))
        # il.add_value('short_description', product_data.get('shortDescription', None))
        # il.add_value('disambiguating_description', '')
        # il.add_value('detail_list_styled', None)
        # il.add_value('slogan', None)

        # il.add_value('alternate_name', None)
        # il.add_value('mpn', None
        # il.add_value('gtin', None)

        # il.add_value('sale_price', None)
        # il.add_value('sale_price_cents', None)
        # il.add_value('price_data', None)
        # il.add_value('offers', product_data['offers'])
        # il.add_value('product_line', product_data['product_line'])
        # il.add_value('manufacturer', None)

        # il.add_value('production_date', None)
        # get_raw_release_date = product_data.get('validFrom', None)
        # if get_raw_release_date is not None and get_raw_release_date != "" and "Z" in get_raw_release_date:
        #     release_date_iso_format = get_raw_release_date[:-1] + '+00:00'
        #     get_release_date = datetime.fromisoformat(release_date_iso_format).astimezone(timezone.utc).strftime("%Y-%m-%d")
        #     self.logger.info('\tSITEMAP FILTER - %s', date.fromisoformat(get_release_date))
        #     il.add_value('release_date', date.fromisoformat(get_release_date))

        # il.add_value('height_value', get_height) 
        # il.add_value('depth_value', get_depth)
        # il.add_value('length_value', get_length)
        # il.add_value('weight_value', get_weight)
        # il.add_value('height_unit', get_height)
        # il.add_value('depth_unit', get_depth)
        # il.add_value('length_unit', get_length)
        # il.add_value('weight_unit', get_weight)

        

        # il.add_value('dimension_data', get_dimension_data) 

        # il.add_value('country_of_origin', None)
        # il.add_value('country_of_assembly', None)
        # # il.add_value('country_of_purchase', None)
        # # il.add_value('country_of_last_processing', None)

        get_reviews_data = product_data.get('reviewsData', None)
        if get_reviews_data is not None:
            get_reviews = [review for review in get_reviews_data['results'][0]['reviews']]
            il.add_value('additional_data', get_reviews)
        # il.add_value('notes', None)
        # il.add_value('tags', None)
        # il.add_value('keywords', None)
        # il.add_value('has_adult_consideration', None)
        # il.add_value('is_family_friendly', None)
        # il.add_value('energy_consumption_details', None)

        yield il.load_item()
