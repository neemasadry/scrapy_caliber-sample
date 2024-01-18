import scrapy, json # , unicodedata, re
from datetime import datetime
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

current_time = datetime.now().strftime("%Y%m%d%I%M%S")


class BerlutiSpider(SitemapSpider):
    # start_urls = ['https://www.berluti.com/']

    name = 'berluti'
    allowed_domains = ['berluti.com']

    # start_urls = [
    sitemap_urls = [
        # 'https://www.berluti.com/en-us/sitemap-en-us_index.xml',
        # 'https://www.berluti.com/en-us/sitemap-en-us_0-product.xml',
        'https://www.berluti.com/en-us/sitemap-en-us_2-category.xml',
    ]

    handle_httpstatus_list = [301]

    sitemap_rules = [
        # ('/en-us/[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*/[0-9]*.html', 'parse_product')
        ('https://www.berluti.com/en-us/shoes/oxfords-derbies/', 'parse_category'),
        ('https://www.berluti.com/en-us/shoes/loafers/', 'parse_category'),
        ('https://www.berluti.com/en-us/shoes/buckle-shoes/', 'parse_category'),
        ('https://www.berluti.com/en-us/shoes/boots/', 'parse_category'),
        ('https://www.berluti.com/en-us/shoes/sneakers/', 'parse_category'),
        ('https://www.berluti.com/en-us/sandal-collections/', 'parse_category'),
        ('https://www.berluti.com/en-us/bags/briefcases/', 'parse_category'),
        ('https://www.berluti.com/en-us/bags/backpacks/', 'parse_category'),
        ('https://www.berluti.com/en-us/bags/messenger-bags/', 'parse_category'),
        ('https://www.berluti.com/en-us/bags/travel-bags/', 'parse_category'),
        ('https://www.berluti.com/en-us/bags/clutches/', 'parse_category'),
        ('https://www.berluti.com/en-us/bags/tote-bags/', 'parse_category'),
        ('https://www.berluti.com/en-us/bags/berluti-x-globe-trotter/', 'parse_category'),
        ('https://www.berluti.com/en-us/wallets/wallets/', 'parse_category'),
        ('https://www.berluti.com/en-us/wallets/cardholders/', 'parse_category'),
        ('https://www.berluti.com/en-us/wallets/cases-covers/', 'parse_category'),
        ('https://www.berluti.com/en-us/ready-to-wear/coats-blousons/', 'parse_category'),
        ('https://www.berluti.com/en-us/ready-to-wear/knitwear-sweatshirts/', 'parse_category'),
        ('https://www.berluti.com/en-us/ready-to-wear/polos-tshirts/', 'parse_category'),
        ('https://www.berluti.com/en-us/ready-to-wear/shirts/', 'parse_category'),
        ('https://www.berluti.com/en-us/ready-to-wear/trousers-denim/', 'parse_category'),
        ('https://www.berluti.com/en-us/ready-to-wear/shop-by-look/', 'parse_category'),
        ('https://www.berluti.com/en-us/accessories/belts/', 'parse_category'),
        ('https://www.berluti.com/en-us/accessories/ties-pocket-squares/', 'parse_category'),
        ('https://www.berluti.com/en-us/accessories/hat--scarves-gloves/', 'parse_category'),
        ('https://www.berluti.com/en-us/accessories/homeware--high-tech-lifestyle/', 'parse_category'),
        ('https://www.berluti.com/en-us/accessories/key-holders/', 'parse_category'),
        ('https://www.berluti.com/en-us/accessories/jewels-sunglasses/', 'parse_category'),
        ('https://www.berluti.com/en-us/accessories/socks/', 'parse_category')
    ]

    # def sitemap_filter(self, entries):
    #     for entry in entries:
    #         self.logger.info('SITEMAP_FILTER: %s', entry['loc'])
    #         if (('/new/' in entry['loc']) or ('/iconic/' in entry['loc']) or ('-by-berluti/' in entry['loc']) or ('/shop-by-look/' in entry['loc'])):
    #             continue
    #         elif ((entry['loc'] == 'https://www.berluti.com/en-us/bags/') or (entry['loc'] == 'https://www.berluti.com/en-us/shoes/') or (entry['loc'] == 'https://www.berluti.com/en-us/wallets/')):
    #             continue
    #         elif ((entry['loc'] == 'https://www.berluti.com/en-us/ready-to-wear/') or (entry['loc'] == 'https://www.berluti.com/en-us/accessories-1/') or (entry['loc'] == 'https://www.berluti.com/en-us/accessories-2/')):
    #             continue
    #         elif ((entry['loc'] == 'https://www.berluti.com/en-us/gifts-by-berluti/') or (entry['loc'] == 'https://www.berluti.com/en-us/shop-by-look-1/') or (entry['loc'] == 'https://www.berluti.com/en-us/shop-by-look-2/')):
    #             continue
    #         else:
    #             self.logger.info('SITEMAP_FILTER: %s', entry['loc'])
    #             yield entry

    custom_settings = {
        # 'IMAGES_STORE': 'downloads/' + name,
        'IMAGES_STORE': 's3://scrapy-caliber/downloads/' + name + "/",
        # 'FEEDS': {
        #     f'downloads/{name}/{current_time}_{name}.jsonl': {
        #         'format': 'jsonlines', 
        #         'overwrite': False,
        #         'encoding': 'utf8'
        #     }
        # },
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
        'USER_AGENT': 'Pinterestbot',
        # 'CONCURRENT_REQUESTS': 2,
        # 'DOWNLOAD_DELAY': 2
    }

    # lua_script = """
    #     function main(splash)
    #         local num_scrolls = 10
    #         local scroll_delay = 1.0

    #         local scroll_to = splash:jsfunc("window.scrollTo")
    #         local get_body_height = splash:jsfunc(
    #             "function() {return document.body.scrollHeight;}"
    #         )
    #         assert(splash:go(splash.args.url))
    #         splash:wait(splash.args.wait)

    #         for _ = 1, num_scrolls do
    #             scroll_to(0, get_body_height())
    #             splash:wait(scroll_delay)
    #         end        
    #         return splash:html()
    #     end
    # """

    headers = {
       # "Host": "c.go-mpulse.net",
       # "Origin": "https://www.dior.com",
       # "Referer": "https://www.dior.com/",
       "Connection": "keep-alive",
       # "Cache-Control": "max-age=0",
       # "Upgrade-Insecure-Requests": "1",
       # "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
       "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
       # "Accept": "*/*",
       # "DNT": "1",
       "Accept-Encoding": "gzip, deflate, br",
       "Accept-Language":"en-US,en;q=0.9"
    }

    def parse_category(self, response):
        get_product_links = response.css("div.filter-grid-wrapper div.product-tile div.product-tile-inner a::attr(href)").getall()
        for product_link in get_product_links:
            yield scrapy.Request(
                url=product_link,
                callback=self.parse_product
            )


    def parse_product(self, response):
        il = ItemLoader(item=ProductItem())

        # il.add_value('user_id', 24)
        # il.add_value('account_id', 43)
        il.add_value('brand_id', None)
        il.add_value('category_id', None)

        il.add_value('name', response.css('h1.product-title::text').get())

        il.add_value('brand_title', "berluti")

        il.add_value('product_url', response.url)
        il.add_value('product_json_url', None)

        il.add_value('retail_price', response.css('div.product-price span.price-sales::text').get())

        il.add_value('retail_price_cents', response.css('div.product-price span.price-sales::text').get())

        get_upid = urlsplit(response.url).path.split('/')[-1].replace('.html', '')
        il.add_value('upid', get_upid)

        # json_product_script_tag = next((json_schema for schema in product_schemas if (json_schema := json.loads(remove_tags(schema))).get('@type', "nothing") == 'Product'), None)
        # il.add_value('sku', json_product_script_tag.get('sku', None))

        il.add_value('gender', 'male')

        il.add_value('body_part', 'n/a')

        get_category = replace_escape_chars(response.css("div.pdp-main div.breadcrumb-container ul.breadcrumb li.breadcrumb-root a::text").get())
        il.add_value('category', get_category)

        get_subcategory = replace_escape_chars(response.css("div.pdp-main div.breadcrumb-container ul.breadcrumb li.breadcrumb-item a.current-breadcrumb::text").get())
        il.add_value('subcategory', get_subcategory)

        il.add_value('description', response.css('div.product-description-container p.product-description-text::text').get())

        get_detail_list = response.css("div.tab-contents div.tab-1 ul li::text").getall()
        il.add_value('detail_list', get_detail_list)
        # il.add_value('detail_list_styled', None)

        il.add_value('country_of_origin', 'Italy')

        il.add_value('country_of_assembly', 'Italy')

        # get_detail_list_tab_2 = response.css("div.tab-contents div.tab-2 ul li::text").getall()
        # if 'Dimensions' in get_detail_list_tab_2:
        #     # Remove all elements up to, and including, the string 'Dimensions'
        #     isolate_dimensions = get_detail_list_tab_2[get_detail_list_tab_2.index('Dimensions')+1:]

        #     il.add_value('length', next((s.lower() for s in isolate_dimensions if 'length' in s.lower() or 'lenght' in s.lower()), None).split(':')[-1].strip())
        #     il.add_value('width', next((s.lower() for s in isolate_dimensions if 'width' in s.lower()), None).split(':')[-1].strip())
        #     il.add_value('height', next((s.lower() for s in isolate_dimensions if 'height' in s.lower()), None).split(':')[-1].strip())

        # Returns list of images where width=640
        get_small_images = response.css("div.product-image-container ul.pdp-img-carousel li.pdp-image button img.productthumbnail::attr(src)").getall()
        # Returns list of images where width=2000
        get_large_images = [img.replace("?sw=640&sfrm=jpg", "?sw=2000&sfrm=jpg") for img in get_small_images]
        il.add_value('image_urls', get_large_images)

        get_color = response.css("div.product-info-container div.product-detail div.product-variations span.product-color-name::text").get().lower()
        il.add_value('color', get_color)

        # raw_query_color = next((query_color for query_color in url_queries if search('^dwvar_[a-zA-Z0-9\-]*_color=', query_color)), None)
        # query_color = sub('dwvar_[a-zA-Z0-9-_]*_color=', '', query_color)

        get_color_variations = response.css("div.product-info-container div.product-detail div.product-variations div.variant-dropdown div.pdp-redesign-img-variations-wrapper div.pdp-redesign-img-wrapper")

        if len(get_color_variations) >= 2:
            color_link_tuple_list = [(get_color_variation.attrib['data-attrvalue'].lower(), get_color_variation.attrib['data-value']) for get_color_variation in get_color_variations]

            variant_color_dictionary = {color_variant_name: color_variant_link for color_variant_name, color_variant_link in color_link_tuple_list}
            variant_color_dictionary.pop(get_color)
        else:
            variant_color_dictionary = None

        il.add_value('variant_colors', variant_color_dictionary)

        yield il.load_item()
