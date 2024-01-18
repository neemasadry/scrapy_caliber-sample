import scrapy, json # , unicodedata, re
from datetime import datetime
from copy import deepcopy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import SitemapSpider #, CrawlSpider, Rule, XMLFeedSpider
from scrapy.loader import ItemLoader
from urllib.parse import urlsplit, parse_qs
from scrapy_caliber.items import ProductItem
from w3lib.html import remove_tags, replace_escape_chars, replace_entities
# from scrapy_splash import SplashRequest

current_time = datetime.now().strftime("%Y%m%d%I%M%S")

class BalenciagaSpider(SitemapSpider): #XMLFeedSpider, CrawlSpider):
    name = 'balenciaga'
    allowed_domains = ['balenciaga.com']

    # start_urls = [
    sitemap_urls = [
        # Example Sitemap URL
        'https://www.balenciaga.com/en-us/sitemap_0-category.xml'
    ]

    sitemap_rules = [
        ('/en-us/women/ready-to-wear/', 'parse_category'),
        ('/en-us/women/leather-goods/', 'parse_category'),
        ('/en-us/women/shoes/', 'parse_category'),
        ('/en-us/women/accessories/', 'parse_category'),
        ('/en-us/men/ready-to-wear/', 'parse_category'),
        ('/en-us/men/leather-goods/', 'parse_category'),
        ('/en-us/men/shoes/', 'parse_category'),
        ('/en-us/men/accessories/', 'parse_category'),
    ]

    custom_settings = {
        # 'IMAGES_STORE': 'downloads/' + name,
        # 'FEED_URI': f'downloads/{name}/' + f'{current_time}_{name}.json',
        # 'FEED_FORMAT': 'json',
        'IMAGES_STORE': 's3://scrapy-caliber/downloads/' + name + "/",
        'FEEDS': {
            's3://scrapy-caliber/downloads/%(name)s/%(current_time)s/batched_jsonlines/%(batch_id)d-%(name)s_%(batch_time)s.jsonl': {
                'format': 'jsonlines', 
                'encoding': 'utf-8',
                'overwrite': False,
                'store_empty': False
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
        # 'USER_AGENT': '',
        # 'CONCURRENT_REQUESTS': 2,
        # 'DOWNLOAD_DELAY': 2
    }

    headers = {
       # "Host": "c.go-mpulse.net",
       # "Origin": "https://www.balenciaga.com",
       # "Referer": "https://www.balenciaga.com/",
       "Connection": "keep-alive",
       # "Cache-Control": "max-age=0",
       # "Upgrade-Insecure-Requests": "1",
       "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
       "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
       # "Accept": "*/*",
       # "DNT": "1",
       "Accept-Encoding": "gzip, deflate, br",
       "Accept-Language":"en-US,en;q=0.9"
    }

    def parse_category(self, response):
        # self.logger.info('\tParse Category URL: %s', response.url)
        split_url_path = urlsplit(response.url).path.split('/')

        if 'view-all' in split_url_path[-1] or len(split_url_path) <= 3:
            self.logger.info('\tParse Category - Skipped "view-all": %s', split_url_path[-1])
        else:
            omitted_fragments = ['', 'en-us']
            sanitized_path_list = [url_fragment for url_fragment in split_url_path if url_fragment not in omitted_fragments]

            # Standard category assignment
            cb_gender = sanitized_path_list[0]
            cb_category = sanitized_path_list[1]

            if len(sanitized_path_list) >= 3:
                cb_subcategory = sanitized_path_list[2]
            else:
                cb_subcategory = None

            for product_tile in response.css('ul.l-productgrid__inner li.l-productgrid__item article.c-product div.c-product__item a.c-product__focus'):
                
                temp_link = "https://www.balenciaga.com" + product_tile.attrib['href']

                yield scrapy.Request(
                    url=temp_link,
                    callback=self.parse_product,
                    headers=self.headers,
                    # meta={
                    #     'splash': {
                    #         'args': {
                    #             'wait': 5.0,
                    #             'resource_timeout': 10.0,
                    #             'timeout': 50.0
                    #         },
                    #         'endpoint': "render.html"
                    #     }
                    # },
                    cb_kwargs={
                        'cb_gender': cb_gender,
                        'cb_category': cb_category,
                        'cb_subcategory': cb_subcategory,
                        'cb_category_list': sanitized_path_list
                    }
                )

    def parse_product(self, response, cb_gender=None, cb_category=None, cb_subcategory=None, cb_category_list=None):
        self.logger.info('\tParse Product URL: %s', response.url)

        cb_breadcrumb_list = [blink.get() for blink in response.css('ol.c-breadcrumbs li.c-breadcrumbs__item a.c-breadcrumbs__link span::text') if blink.get() != 'Home']

        for product_swatch_item in response.css('div.c-swatches p.c-swatches__item label.c-swatches__itemlabel input'):
            cb_swatch_color_code = product_swatch_item.attrib['data-attr-value']
            swatch_variant_url = product_swatch_item.attrib['data-attr-href']

            yield scrapy.Request(
                url=swatch_variant_url,
                headers=self.headers,
                callback=self.parse_product_variant,
                # meta={
                #     'splash': {
                #         'args': {
                #             'wait': 5.0,
                #             'resource_timeout': 10.0,
                #             'timeout': 50.0
                #         },
                #         'endpoint': "render.html"
                #     }
                # },
                cb_kwargs={
                    'cb_gender': cb_gender,
                    'cb_category': cb_category,
                    'cb_subcategory': cb_subcategory,
                    'cb_category_list': cb_subcategory,
                    'cb_swatch_color_code': cb_swatch_color_code,
                    'cb_breadcrumb_list': cb_breadcrumb_list
                }
            )

    def parse_product_variant(self, response, cb_gender=None, cb_category=None, cb_subcategory=None, cb_category_list=None, cb_swatch_color_code=None, cb_breadcrumb_list=None):
        product_variant_data = json.loads(response.body)

        il = ItemLoader(item=ProductItem())

        # il.add_value('user_id', 16)
        # il.add_value('account_id', 35)
        il.add_value('brand_id', None)

        il.add_value('category_id', None)

        il.add_value('name', product_variant_data['product']['productName'])

        il.add_value('brand_title', 'balenciaga')

        il.add_value('country_of_assembly', product_variant_data['product']['madeIn'])

        il.add_value('upid', product_variant_data['product']['productSMC'])
        il.add_value('sku', product_variant_data['product']['id'])

        il.add_value('product_url', "https://www.balenciaga.com" + product_variant_data['product']['selectedProductUrl'])
        il.add_value('product_json_url', response.url)

        il.add_value('subtitle', product_variant_data['product']['productTitle'])

        il.add_value('description', product_variant_data['product']['longDescription'])
        # il.add_value('description_styled', product_variant_data['product']['longDescription'])

        deepcopy_detail_list = deepcopy(product_variant_data['product']['shortDescription'])
        sanitized_detail_list = replace_entities(replace_escape_chars(remove_tags(deepcopy_detail_list))).split('•')
        final_detail_list = [li.strip() for li in sanitized_detail_list if li != '']
        il.add_value('detail_list', final_detail_list)

        il.add_value('detail_list_styled', product_variant_data['product']['shortDescription'])

        il.add_value('retail_price', product_variant_data['product']['price']['sales']['decimalPrice'])
        il.add_value('retail_price_cents', product_variant_data['product']['price']['sales']['decimalPrice'])

        if product_variant_data['product']['productGender'] != None:
            il.add_value('gender', product_variant_data['product']['productGender'])
        elif cb_gender != None:
            il.add_value('gender', cb_gender)
        else:
            il.add_value('gender', "N/A")

        il.add_value('body_part', 'n/a')

        vattrs = deepcopy(product_variant_data['product']['variationAttributes'])

        product_color = next(vattr for vattr in vattrs if vattr['attributeId'] == 'color')['selectedValue']

        il.add_value('color', product_color)

        color_variant = next(ct for vattr in vattrs if vattr['attributeId'] == 'color' for ct in vattr['values'] if product_color == ct['displayValue'])

        il.add_value('color_variant', color_variant)

        variant_colors = next(vattr for vattr in vattrs if vattr['attributeId'] == 'color')['values'].remove(color_variant)

        il.add_value('variant_colors', variant_colors)

        variant_sizes = next(vattr for vattr in vattrs if vattr['attributeId'] == 'size')['values']

        il.add_value('variant_sizes', variant_sizes)

        il.add_value('additional_data', product_variant_data['exposedData']['gtm']['product'])

        il.add_value('category', cb_category)

        il.add_value('subcategory', cb_subcategory)

        category_data = {
            'gender': cb_gender,
            'category': cb_category,
            'subcategory': cb_subcategory,
            'category_list': cb_category_list,
            'color': product_color,
            'swatch_color_code': cb_swatch_color_code,
            'breadcrumb_list': cb_breadcrumb_list,
            'collection_season': product_variant_data['product']['collection'],
            'primary_category_id': product_variant_data['product']['primaryCategoryID'],
            'categories_list': product_variant_data['product']['categories'],
            'other_styles_list': product_variant_data['product'].get('otherStyles', None),
        }

        il.add_value('category_data', category_data)

        product_variant_images = [img['large'] for img in product_variant_data['product']['akeneoImages']['packshot']]

        il.add_value('image_urls', product_variant_images)

        yield il.load_item()
