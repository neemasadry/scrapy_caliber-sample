import scrapy, json # , unicodedata, re
from datetime import datetime
from copy import deepcopy
from re import sub, search
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import SitemapSpider #, CrawlSpider, Rule, XMLFeedSpider
from scrapy.loader import ItemLoader
from urllib.parse import urlsplit, parse_qs
from scrapy_caliber.items import ProductItem
from w3lib.html import replace_escape_chars, remove_tags, replace_entities
from collections import Counter
# from scrapy_splash import SplashRequest

current_time = datetime.now().strftime("%Y%m%d%I%M%S")

# scrapy shell 'http://localhost:8050/render.html?url=https://www.fendi.com/on/demandware.store/Sites-US-Site/en_US/Search-UpdateGrid?cgid=us_woman_bags_totebags&start=12&sz=12&timeout=10&wait=0.5'

class FendiSpider(SitemapSpider):
    # start_urls = ["https://www.fendi.com/]

    name = 'fendi'
    allowed_domains = ['fendi.com', 'static.fendi.com']

    # start_urls = [
    sitemap_urls = [
        # 'https://www."fendi.com"/en-us/sitemap-en-us_0-product.xml',
        # 'https://www.fendi.com/sitemap.xml',
        'https://www.fendi.com/us-en/sitemap-pages-1.xml'
        # 'https://www.fendi.com/sitemap-pages-21.xml',
    ]

    # handle_httpstatus_list = [301]

    sitemap_rules = [
        # ('/en-us/[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*/[0-9]*.html', 'parse_product')
        ('/us-en/(woman|man)/[a-z-]*/(|[a-z-]*/)[a-z-]*-(?=.*[0-9])(?=.*[a-zA-Z])([a-zA-Z0-9]+)$', 'convert_html_to_json')
    ]

    headers = {
        # "Host": "c.go-mpulse.net",
        "Origin": "https://www.fendi.com",
        "Referer": "https://www.fendi.com/",
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
        # 'IMAGES_STORE': 'downloads/' + name,
        # 'FEEDS': {
        #     f'downloads/{name}/{current_time}_{name}.json': {
        #         'format': 'json', 
        #         'overwrite': False,
        #         'encoding': 'utf8'
        #     }
        # },
        # 'FEED_URI': f'downloads/{name}/' + f'{current_time}_{name}.json',
        # 'FEED_FORMAT': 'json',
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
        # 'DOWNLOAD_TIMEOUT': 30,
        # 'CONCURRENT_REQUESTS': 5,
        # 'DOWNLOAD_DELAY': 2,
        # 'DEFAULT_REQUEST_HEADERS': headers
    }



    # def parse_category_tiles(self, response, top_category_key=None, mid_category_key=None, sub_category_value=None, category_parent_url=None, more_pages=False):
    #     get_product_links = response.css('div.maincontent div.row ul.product-grid li.c-tiles button.add-to-wishlist::attr(data-url)').getall()
        
    #     for product_link in get_product_links:
    #         self.logger.info('\tPCT - Product Link: %s', product_link)
    #         yield scrapy.Request(
    #             url=product_link,
    #             callback=self.parse_product
    #         )

    #     next_page_url = response.css('div.show-more button.show-more::attr("data-url")').get()

    #     if next_page_url != '' and next_page_url is not None:
    #         yield scrapy.Request(
    #             url=next_page_url,
    #             callback=self.parse_category_tiles
    #         )

    # def sitemap_filter(self, entries):
    #     # self.logger.info('\tSITEMAP FILTER - %s', inspect(entries))
    #     for entry in entries:
    #         self.logger.info('\tSITEMAP FILTER - %s', entry)
    #         if bool(search(r"^((.*[0-9]+)[a-zA-Z]+.*)|((.*[a-zA-Z]+)[0-9]+.*)$", entry['loc'])):
    #             entry['loc'] = sub('https://www.dior.com/', 'https://api.dior.com/', entry['loc'])
    #             self.logger.info('\tSITEMAP FILTER - %s', entry)
    #             yield entry


    def convert_html_to_json(self, response):
        upid_with_color_code = urlsplit(response.url).path.split('/')[-1].split('-')[-1].upper()
        upid = upid_with_color_code[:-5] # Returns the UPID (i.e., all chars until the last five (color code))
        color_code = upid_with_color_code[-5:] # Returns the color code (i.e., last five chars)

        api_request_url = f'https://www.fendi.com/on/demandware.store/Sites-US-Site/en_US/Product-Variation?dwvar_{upid}_color={color_code}&pid={upid}&quantity=1'

        yield scrapy.Request(
            url=api_request_url,
            callback=self.parse_product,
            # headers = self.headers,
            cb_kwargs={
                'html_product_page': response.url,
                'upid_with_color_code': upid_with_color_code
            }
        )


    def parse_product(self, response, html_product_page=None, upid_with_color_code=None):
        product_data = json.loads(response.body).get('product', None)

        il = ItemLoader(item=ProductItem())

        il.add_value('brand_id', None)
        il.add_value('category_id', None)

        il.add_value('name', product_data.get('productName', None))

        il.add_value('brand_title', "fendi")

        if product_data.get('variantGroupID', None) is not None and product_data.get('variantGroupID', None) != '':
            get_upid = product_data.get('variantGroupID', None)
        else:
            get_upid = upid_with_color_code

        il.add_value('upid', get_upid)

        get_uuid = product_data.get('itemUuid', None)
        il.add_value('uuid', get_uuid)

        il.add_value('product_url', html_product_page)
        il.add_value('product_json_url', response.url)

        get_variations_attrs = product_data.get('variationAttributes', None)

        if isinstance(get_variations_attrs, list) == True and len(get_variations_attrs) >= 1:
            get_color_dict = next((variant_type for variant_type in get_variations_attrs if variant_type.get('id', None) == "color" or variant_type.get('displayName', None) == "Color" or variant_type.get('attributeId', None) == "color"), None)

            if isinstance(get_color_dict, dict) == True and get_color_dict != {}:
                get_product_variant = next((color_variant for color_variant in get_color_dict.get('values', None) if color_variant.get('selected', None) == True), None)
                il.add_value('color', get_product_variant.get('displayValue', None))

                get_retail_price = get_product_variant.get('price', None).get('sales', None).get('formatted', None)
                
                get_large_images = [img.get('zoom', None) for img in get_product_variant.get('images', None).get('large', None)]
            

        if get_retail_price == None or get_retail_price == '':
            get_retail_price = product_data.get('price', None).get('sales', None).get('formatted', None)

        il.add_value('retail_price', get_retail_price)

        il.add_value('retail_price_cents', get_retail_price)

        if get_large_images == None or get_large_images == []:
            get_large_images = [img.get('zoom', None) for img in get_product_variant.get('images', None).get('large', None)]

        il.add_value('image_urls', get_large_images)

        # get_upid = urlsplit(response.url).path.split('/')[-1].replace('.html', '')
        # il.add_value('upid', get_upid)

        il.add_value('gender', product_data.get('gender', None))

        il.add_value('body_part', 'n/a')

        html_product_page_path = urlsplit(html_product_page).path
        path_list = html_product_page_path.split("/")
        sanitized_categories = [path_fragment for path_fragment in path_list if path_fragment not in ['', 'us-en', 'man', 'woman'] and not bool(search(r"^((.*[0-9]+)[a-zA-Z]+.*)|((.*[a-zA-Z]+)[0-9]+.*)$", path_fragment))]
        
        if len(sanitized_categories) == 2:
            # il.add_value('super_category', None)
            il.add_value('category', sanitized_categories[0])
            il.add_value('subcategory', sanitized_categories[1])
        elif len(sanitized_categories) == 1:
            # il.add_value('super_category', None)
            il.add_value('category', sanitized_categories[0])
            il.add_value('subcategory', None)
        else:
            # il.add_value('super_category', None)
            il.add_value('category', None)
            il.add_value('subcategory', None)

        # il.add_value('subcategory', )

        il.add_value('description', product_data.get('longDescription', None))

        il.add_value('composition', product_data.get('productComposition', None))

        get_product_dimensions_array = product_data.get('productDimensionArray', None).get('attributeArray', None)
        if isinstance(get_product_dimensions_array, list) and get_product_dimensions_array is not None and len(get_product_dimensions_array) >= 1:
            get_height = next((dimension.get('value', None) for dimension in get_product_dimensions_array if dimension.get('label', None).lower() == "height" and dimension.get('isValid', None) == True), None)
            get_depth = next((dimension.get('value', None) for dimension in get_product_dimensions_array if dimension.get('label', None).lower() == "depth" and dimension.get('isValid', None) == True), None)
            get_length = next((dimension.get('value', None) for dimension in get_product_dimensions_array if dimension.get('label', None).lower() == "length" and dimension.get('isValid', None) == True), None)
            get_weight = next((dimension.get('value', None) for dimension in get_product_dimensions_array if dimension.get('label', None).lower() == "weight" and dimension.get('isValid', None) == True), None)

            # get_height = next((float(sub('[^0-9.]', '', dimension.get('value', None))) for dimension in get_product_dimensions_array if dimension.get('label', None).lower() == "height" and dimension.get('isValid', None) == True), None)
            # get_depth = next((float(sub('[^0-9.]', '', dimension.get('value', None))) for dimension in get_product_dimensions_array if dimension.get('label', None).lower() == "depth" and dimension.get('isValid', None) == True), None)
            # get_length = next((float(sub('[^0-9.]', '', dimension.get('value', None))) for dimension in get_product_dimensions_array if dimension.get('label', None).lower() == "length" and dimension.get('isValid', None) == True), None)
            # get_weight = next((float(sub('[^0-9.]', '', dimension.get('value', None))) for dimension in get_product_dimensions_array if dimension.get('label', None).lower() == "weight" and dimension.get('isValid', None) == True), None)

            # get_height_unit = next((str(sub('[0-9.]', '', dimension.get('value', None))) for dimension in get_product_dimensions_array if dimension.get('label', None).lower() == "height" and dimension.get('isValid', None) == True), None)
            # get_depth_unit = next((str(sub('[0-9.]', '', dimension.get('value', None))) for dimension in get_product_dimensions_array if dimension.get('label', None).lower() == "depth" and dimension.get('isValid', None) == True), None)
            # get_length_unit = next((str(sub('[0-9.]', '', dimension.get('value', None))) for dimension in get_product_dimensions_array if dimension.get('label', None).lower() == "length" and dimension.get('isValid', None) == True), None)
            # get_weight_unit = next((str(sub('[0-9.]', '', dimension.get('value', None))) for dimension in get_product_dimensions_array if dimension.get('label', None).lower() == "weight" and dimension.get('isValid', None) == True), None)

            if get_height is not None and get_height != "":
                # self.logger.info('\n\t\tget_height: %s - TYPE: %s', get_height, type(get_height))
                il.add_value('height_value', get_height) 
                il.add_value('height_unit', get_height)
            if get_depth is not None and get_depth != "": 
                il.add_value('depth_value', get_depth)
                il.add_value('depth_unit', get_depth)
            if get_length is not None and get_length != "": 
                il.add_value('length_value', get_length)
                il.add_value('length_unit', get_length)
            if get_weight is not None and get_weight != "": 
                il.add_value('weight_value', get_weight)
                il.add_value('weight_unit', get_weight)

        # il.add_value('detail_list', )

        # il.add_value('country_of_origin', )

        # il.add_value('country_of_assembly', )

        variant_color_dictionary_list = [color_variant for color_variant in get_color_dict.get('values', None) if color_variant.get('selected', None) == False]

        il.add_value('variant_colors', variant_color_dictionary_list)

        yield il.load_item()
