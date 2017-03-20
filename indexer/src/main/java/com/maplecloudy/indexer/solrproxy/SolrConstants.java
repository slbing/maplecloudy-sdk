package com.maplecloudy.indexer.solrproxy;

public interface SolrConstants {
	public static final String CORE_NAME_LOCAL = "core-local";
	public static final String CORE_NAME_QA = "core-qa";
	public static final String CORE_NAME_CATEGORY = "core-categories";
	public static final String CORE_NAME_EDU = "core-edu";
	public static final String CORE_NAME_EDU_ARTICLE = "core-edu-articles";
	public static final String CORE_NAME_BRAND = "core-brands";
	public static final String CORE_NAME_SERP_CRAWLER = "core-serpcrawler";
	public static final String CORE_NAME_ARTICLE = "core-article";
	public static final String CORE_NAME_RECIPE = "core-recipes";
	public static final String CORE_NAME_AUTO = "core-auto";
	public static final String CORE_NAME_JOB = "core-jobs";
	public static final String CORE_NAME_GEOIP = "core-geoip";
	public static final String CORE_NAME_REVIEW = "core-review";
	public static final String CORE_NAME_KEYWORD = "core-keyword";
	public static final String CORE_NAME_SHOP = "core-shop";
	public static final String CORE_NAME_SINK = "core-sink";
	// local search parameters
	public static final String PARAM_NAME_LOCATION = "location";
	public static final String PARAM_NAME_LATITUDE = "lat";
	public static final String PARAM_NAME_LONGITUDE = "long";
	public static final String PARAM_NAME_RADIUS = "radius";

	// gnosis search parameters
	public static final String PARAM_NAME_MARKETING_TAG = "marketing_tag";
	public static final String PARAM_NAME_USER_KEYWORDS = "user_kws";
	public static final String PARAM_NAME_PRECISION_LEVEL = "precision_level";

	// Solr response field names
	public static final String SOLR_NAME_RESPONSE_HEADER = "responseHeader";
	public static final String SOLR_NAME_RESPONSE_PARAMS = "params";
	public static final String SOLR_NAME_RESPONSE_RESULT = "response";
	public static final String SOLR_NAME_RESPONSE_DOCS = "docs";

	// index field names
	public static final String FIELD_NAME_SCORE = "score";

	// index fields for article
	public static final String FIELD_ARTICLE_ID = "article_id";
	public static final String FIELD_ARTICLE_TYPE = "article_type";
	public static final String FIELD_ARTICLE_TITLE = "article_title";
	public static final String FIELD_ARTICLE_BODY = "article_body";
	public static final String FIELD_ARTICLE_SITE = "article_site";
	public static final String FIELD_ARTICLE_USER_ID = "user_id";
	public static final String FIELD_ARTICLE_CAT1 = "cat1";
	public static final String FIELD_ARTICLE_CAT2 = "cat2";
	public static final String FIELD_ARTICLE_CAT3 = "cat3";
	public static final String FIELD_ARTICLE_CAT4 = "cat4";
	public static final String FIELD_ARTICLE_MARKETING_TAGS = "marketing_tag";
	public static final String FIELD_ARTICLE_ADMIN_TAG = "admin_tag";
	public static final String FIELD_ARTICLE_USER_TAG = "user_tag";
	public static final String FIELD_ARTICLE_SUB_TYPE = "article_sub_type";
	public static final String FIELD_ARTICLE_SUB_SUB_TYPE = "article_sub_sub_type";
	public static final String FIELD_ARTICLE_TYPE_FACET = "article_type_facet";
	public static final String FIELD_ARTICLE_SUB_TYPE_FACET = "article_sub_type_facet";
	public static final String FIELD_ARTICLE_SUB_SUB_TYPE_FACET = "article_sub_sub_type_facet";
	public static final String FIELD_ARTICLE_LATITUDE = "lat";
	public static final String FIELD_ARTICLE_LONGITUDE = "lng";

	// index fields for categorizer
	public static final String FIELD_CATEGORIZER_CATEGORY = "category";
	public static final String FIELD_CATEGORIZER_LEVEL = "level";
	public static final String FIELD_CATEGORIZER_PARENTS = "parents";

	// index fields for local
	public static final String FIELD_LOCAL_ID = "pid";
	public static final String FIELD_LOCAL_ADDRESS = "address";
	public static final String FIELD_LOCAL_ATTRIBUTES = "attributes";
	public static final String FIELD_LOCAL_BUSINESS_NAME = "businessname";
	public static final String FIELD_LOCAL_CATEGORY = "category";
	public static final String FIELD_LOCAL_CHAIN_ID = "chainid";
	public static final String FIELD_LOCAL_CITY = "city";
	public static final String FIELD_LOCAL_CONFIDENCE_SCORE = "confidence_score";
	public static final String FIELD_LOCAL_DESCRIPTION = "description";
	public static final String FIELD_LOCAL_GROUP = "group";
	public static final String FIELD_LOCAL_LATITUDE = "lat";
	public static final String FIELD_LOCAL_LONGITUDE = "lng";
	public static final String FIELD_LOCAL_PHONE = "phone";
	public static final String FIELD_LOCAL_PUB_DATE = "pubdate";
	public static final String FIELD_LOCAL_NAICS_DESCRIPTION = "naics_description";
	public static final String FIELD_LOCAL_SIC_DESCRIPTION = "sic_description";
	public static final String FIELD_LOCAL_SIC_RELEVANCY = "sic_relevancy";
	public static final String FIELD_LOCAL_STATE = "state";
	public static final String FIELD_LOCAL_SUB_DEPARTMENT = "subdepartment";
	public static final String FIELD_LOCAL_NORMALIZED_HEADING = "normalized_heading";
	public static final String FIELD_LOCAL_HEADING_RELEVANCY = "heading_relevancy";
	public static final String FIELD_LOCAL_UNSTRUCTURED_ATTRIBUTES = "unstructured_attributes";
	public static final String FIELD_LOCAL_VAL_DATE = "valdate";
	public static final String FIELD_LOCAL_ZIP = "zip";
	public static final String FIELD_LOCAL_STANDARD_HOURS = "stdhours";
	public static final String FIELD_LOCAL_HOURS_OPEN = "hoursopen";
	public static final String FIELD_LOCAL_TAGLINE = "tagline";
	public static final String FIELD_LOCAL_CUSTOM_ATTRIBUTE = "custom_attribute";
	public static final String FIELD_LOCAL_CUSTOM_ATTRIBUTE_TYPE = "custom_attribute_type";
	public static final String FIELD_LOCAL_CUSTOM_DESCRIPTION = "custom_description";
	public static final String FIELD_LOCAL_CUSTOM_RELEVANCY = "custom_relevancy";
	public static final String FIELD_LOCAL_PAYMENT_TYPE = "payment_type";
	public static final String FIELD_LOCAL_LANGUAGE = "language";

	public static final String FIELD_LOCAL_CHAINNAME = "chainname";
	public static final String FIELD_LOCAL_STANDARDNAME = "standardname";
	public static final String FIELD_LOCAL_Z4TYPE = "z4type";
	public static final String FIELD_LOCAL_CATEGORY_ATTRIBUTE = "category_attribute";
	public static final String FIELD_LOCAL_CATEGORY_HEADING = "category_heading";
	public static final String FIELD_LOCAL_URL = "url";
	public static final String FIELD_LOCAL_EMAIL = "email";
	public static final String FIELD_LOCAL_LOGO = "logo";


	// index fields for Q&A
	public static final String FIELD_QA_QUESTION_PK = "question_pk";
	public static final String FIELD_QA_QUESTION_ID = "question_id";
	public static final String FIELD_QA_QUESTION = "question";
	public static final String FIELD_QA_DATE_TIME = "question_datetime";
	public static final String FIELD_QA_TITLE = "question_title";
	public static final String FIELD_QA_URL = "question_url";
	public static final String FIELD_QA_KEYWORD = "question_keyword";
	public static final String FIELD_QA_SITE = "question_site";
	public static final String FIELD_QA_QUESTION_STATUS = "question_status";
	public static final String FIELD_QA_USER_ID = "user_id";
	public static final String FIELD_QA_CAT1 = "cat1";
	public static final String FIELD_QA_CAT2 = "cat2";
	public static final String FIELD_QA_CAT3 = "cat3";
	public static final String FIELD_QA_CAT4 = "cat4";
	public static final String FIELD_QA_TAGS = "question_tags";
	public static final String FIELD_QA_ANSWERS = "answers";
	public static final String FIELD_QA_ANSWER_COUNT = "answer_count";
	public static final String FIELD_QA_ANSWER_ID = "answer_id";
	public static final String FIELD_QA_ANSWER_STATUS = "answer_status";
	public static final String FIELD_QA_LATITUDE = "lat";
	public static final String FIELD_QA_LONGITUDE = "lng";
	public static final String FIELD_QA_IS_PREMIUM = "is_premium";
	public static final String FIELD_QA_IS_EXPERT = "is_expert";
	public static final String FIELD_QA_RATING_COUNT = "rating_count";
	public static final String FIELD_QA_RATING = "rating";	
	public static final String FIELD_QA_ENTITIES = "entities";
	public static final String FIELD_QA_MODIFIER_ENTITIES = "modifier_entities";
	public static final String FIELD_QA_ENTITY_AMBIGUOUS = "entity_ambiguous";
	public static final String FIELD_QA_BRANDS = "brands";
	public static final String FIELD_QA_DEMOGRAPHICS = "demographics";
	public static final String FIELD_QA_PRICES = "prices";
	public static final String FIELD_QA_BUSINESS_TRANSACTIONS = "business_transactions";	

	// index fields for recipe
	public static final String FIELD_RECIPE_ID = "article_id";
	public static final String FIELD_RECIPE_CATEGORY = "recipe_category";
	public static final String FIELD_RECIPE_TITLE = "recipe_title";
	public static final String FIELD_RECIPE_BODY = "recipe_body";
	public static final String FIELD_RECIPE_SITE = "article_site";
	public static final String FIELD_RECIPE_TYPE = "article_type";
	public static final String FIELD_RECIPE_USER_ID = "user_id";

	// index fields for jobs
	public static final String FIELD_JOB_GUID = "guid";
	public static final String FIELD_JOB_DESCRIPTION = "description";
	public static final String FIELD_JOB_EMPLOYER = "employer";
	public static final String FIELD_JOB_EMPLOYER_FACET = "employer_facet";
	public static final String FIELD_JOB_EXPIRATION_DATE = "expiration_date";
	public static final String FIELD_JOB_IMG_LINK = "img_link";
	public static final String FIELD_JOB_INDUSTRY = "industry";
	public static final String FIELD_JOB_INDUSTRY_FACET = "industry_facet";
	public static final String FIELD_JOB_LATITUDE = "lat";
	public static final String FIELD_JOB_LINK = "link";
	public static final String FIELD_JOB_LOCATION = "location";
	public static final String FIELD_JOB_LONGITUDE = "lng";
	public static final String FIELD_JOB_PUBLISH_DATE = "publish_date";
	public static final String FIELD_JOB_SALARY_MAX = "salary_max";
	public static final String FIELD_JOB_SALARY_MIN = "salary_min";
	public static final String FIELD_JOB_TITLE = "title";
	public static final String FIELD_JOB_TITLE_FACET = "title_facet";

	// index fields for geoip
	public static final String FIELD_GEOIP_IP_RANGE_ID = "ip_range_id";
	public static final String FIELD_GEOIP_SOURCE = "source";
	public static final String FIELD_GEOIP_START_IP_NUM = "start_ip_num";
	public static final String FIELD_GEOIP_END_IP_NUM = "end_ip_num";
	public static final String FIELD_GEOIP_IP_SPAN = "ip_span";
	public static final String FIELD_GEOIP_CITY = "city";
	public static final String FIELD_GEOIP_STATE = "state";
	public static final String FIELD_GEOIP_COUNTRY = "country";
	public static final String FIELD_GEOIP_LATITUDE = "latitude";
	public static final String FIELD_GEOIP_LONGITUDE = "longitude";
	public static final String FIELD_GEOIP_ASN = "asn";
	public static final String FIELD_GEOIP_ASN_OWNER = "asn_owner";
	public static final String FIELD_GEOIP_DOC_TYPE = "doc_type";
	public static final String FIELD_GEOIP_SUBNET_CLASS = "subnet_class";
	public static final String FIELD_GEOIP_VERSION = "version";
	public static final String FIELD_GEOIP_WEIGHT = "weight";
	public static final String FIELD_GEOIP_ZIP = "zip";

	// index fields for review
	public static final String FIELD_REVIEW_PK = "pk";
	public static final String FIELD_REVIEW_ID = "id";
	public static final String FIELD_REVIEW_BODY = "body";
	public static final String FIELD_REVIEW_CREATE_DATETIME = "create_datetime";
	public static final String FIELD_REVIEW_DECODED_MERCHANT_ID = "decoded_merchant_id";
	public static final String FIELD_REVIEW_DISLIKES = "dislikes";
	public static final String FIELD_REVIEW_DOCUMENT_TYPE = "document_type";
	public static final String FIELD_REVIEW_HEADLINE = "headline";
	public static final String FIELD_REVIEW_KEYWORD = "keyword";
	public static final String FIELD_REVIEW_LATITUDE = "lat";
	public static final String FIELD_REVIEW_LIKES = "likes";
	public static final String FIELD_REVIEW_LONGITUDE = "lng";
	public static final String FIELD_REVIEW_MERCHANT_ID = "merchant_id";
	public static final String FIELD_REVIEW_MERCHANT_NAME = "merchant_name";
	public static final String FIELD_REVIEW_OLD_ID_TEMP = "old_id_temp";
	public static final String FIELD_REVIEW_RATING = "rating";
	public static final String FIELD_REVIEW_SITE_CODE = "site_code";
	public static final String FIELD_REVIEW_USER_FIRST_NAME = "user_first_name";
	public static final String FIELD_REVIEW_USER_LAST_NAME = "user_last_name";
	public static final String FIELD_REVIEW_USER_ID = "user_id";

	// index fields for comments
	public static final String FIELD_COMMENT_PK = "pk";
	public static final String FIELD_COMMENT_ID = "id";
	public static final String FIELD_COMMENT_BODY = "body";
	public static final String FIELD_COMMENT_COMMENT_TYPE = "comment_type";
	public static final String FIELD_COMMENT_COMMENT_TYPE_ID = "comment_type_id";
	public static final String FIELD_COMMENT_CREATE_DATETIME = "create_datetime";
	public static final String FIELD_COMMENT_DISLIKES = "dislikes";
	public static final String FIELD_COMMENT_DOCUMENT_TYPE = "document_type";
	public static final String FIELD_COMMENT_ITEM_ID = "item_id";
	public static final String FIELD_COMMENT_LIKES = "likes";
	public static final String FIELD_COMMENT_SITE_CODE = "site_code";
	public static final String FIELD_COMMENT_USER_FIRST_NAME = "user_first_name";
	public static final String FIELD_COMMENT_USER_LAST_NAME = "user_last_name";
	public static final String FIELD_COMMENT_USER_ID = "user_id";

	// index fields for keyword
	public static final String FIELD_KEYWORD_PK = "keyword";
	public static final String FIELD_KEYWORD_LIST_NAME = "list_name";
	public static final String FIELD_KEYWORD_CAT1 = "cat1";
	public static final String FIELD_KEYWORD_CAT2 = "cat2";
	public static final String FIELD_KEYWORD_CAT3 = "cat3";
	public static final String FIELD_KEYWORD_CAT4 = "cat4";
	public static final String FIELD_KEYWORD_CAT1_FACET = "cat1_facet";
	public static final String FIELD_KEYWORD_CAT2_FACET = "cat2_facet";
	public static final String FIELD_KEYWORD_CAT3_FACET = "cat3_facet";
	public static final String FIELD_KEYWORD_CAT4_FACET = "cat4_facet";
	public static final String FIELD_KEYWORD_COVERAGE_SITECODE = "sites";
	public static final String FIELD_KEYWORD_COVERAGE_FEEDID = "feed_id";
	public static final String FIELD_KEYWORD_CONTENT_COVERAGE_STATUS = "content_coverage_status";
	public static final String FIELD_KEYWORD_COVERAGE_MATCHED_CATEGORY_LEVEL = "matched_category_level";
	public static final String FIELD_KEYWORD_COVERAGE_CREATIVE_ID = "creative_id";
	public static final String FIELD_KEYWORD_JSON_OBJECT = "keyword_json_object";
	public static final String FIELD_KEYWORD_SITE_LEVEL_CONTENT_COVERAGE_STATUS_PREFIX = "content_coverage_";

	public static final String FIELD_KEYWORD_CATEGORIZER_CONFIDENCE = "categorizer_confidence";
	public static final String FIELD_KEYWORD_YELD_RESULT = "yield_result";
	public static final String FIELD_KEYWORD_SITES = "sites";
	public static final String FIELD_KEYWORD_ACTIVE_SITES = "active_sites";
	public static final String FIELD_KEYWORD_ACTIVE_SITES_SOURCES = "active_site_sources";
	public static final String FIELD_KEYWORD_INACTIVE_SITES = "inactive_sites";
	public static final String FIELD_KEYWORD_INACTIVE_SITES_SOURCES = "inactive_site_sources";
	public static final String FIELD_KEYWORD_SEM_RUSH_SCORE_ADCENTER = "adcenter_score";
	public static final String FIELD_KEYWORD_SEM_RUSH_SCORE_ADWC = "adwc_score";
	public static final String FIELD_KEYWORD_SEM_RUSH_SCORE_ADWORDS = "adwords_score";
	/*add by yanhui.ma*/
	public static final String FIELD_KEYWORD_FLAG_CATEGORIZER = "categorizer_flag";
	public static final String FIELD_KEYWORD_FLAG_LOCATION = "location_flag";
	public static final String FIELD_KEYWORD_STEMMER_CANONICAL_PHRASE = "canonical_phrase";

	// index fields for shop
	public static final String FIELD_SHOP_MAP = "map";
	public static final String FIELD_SHOP_ATTRIBUTES_NAMES = "attribute_names";
	public static final String FIELD_SHOP_ATTRIBUTES_VALUES = "attribute_values";
	public static final String FIELD_SHOP_TITLE = "title";
	public static final String FIELD_SHOP_SUPPLIER_ID = "supplier_id";
	public static final String FIELD_SHOP_DROP_SHIP_FEE = "drop_ship_fee";
	public static final String FIELD_SHOP_SUPPLIER_NAME = "supplier_name";
	public static final String FIELD_SHOP_PRODUCT_SKU = "product_sku";
	public static final String FIELD_SHOP_WARRANTY = "warranty";
	public static final String FIELD_SHOP_DESCRIPTION = "description";
	public static final String FIELD_SHOP_CONDITION = "condition";
	public static final String FIELD_SHOP_DETAILS = "details";
	public static final String FIELD_SHOP_MANUFACTURER = "manufacturer";
	public static final String FIELD_SHOP_BRAND_NAME = "brand_name";
	public static final String FIELD_SHOP_CASE_PACK_QUANTITY = "case_pack_quantity";
	public static final String FIELD_SHOP_COUNTRY_OF_ORIGIN = "country_of_origin";
	public static final String FIELD_SHOP_PRODUCT_LAST_UPDATE = "product_last_update";
	public static final String FIELD_SHOP_ITEM_SKU = "item_sku";
	public static final String FIELD_SHOP_MPN = "mpn";
	public static final String FIELD_SHOP_UPC = "upc";
	public static final String FIELD_SHOP_ITEM_NAME = "item_name";
	public static final String FIELD_SHOP_ITEM_WEIGHT = "item_weight";
	public static final String FIELD_SHOP_SHIP_ALONE = "ship_alone";
	public static final String FIELD_SHOP_SHIP_FREIGHT = "ship_freight";
	public static final String FIELD_SHOP_SHIP_WEIGHT = "ship_weight";
	public static final String FIELD_SHOP_SHIP_COST = "ship_cost";
	public static final String FIELD_SHOP_MAX_SHIP_SINGLE_BOX = "max_ship_single_box";
	public static final String FIELD_SHOP_PRICE = "price";
	public static final String FIELD_SHOP_CUSTOM_PRICE = "custom_price";
	public static final String FIELD_SHOP_PREPAY_PRICE = "prepay_price";
	public static final String FIELD_SHOP_STREET_PRICE = "street_price";
	public static final String FIELD_SHOP_MSRP = "msrp";
	public static final String FIELD_SHOP_QTY_AVAIL = "qty_avail";
	public static final String FIELD_SHOP_STOCK = "stock";
	public static final String FIELD_SHOP_EST_AVAIL = "est_avail";
	public static final String FIELD_SHOP_QTY_ON_ORDER = "qty_on_order";
	public static final String FIELD_SHOP_ITEM_LAST_UPDATE = "item_last_update";
	public static final String FIELD_SHOP_ITEM_DISCONTINUED_DATE = "item_discontinued_date";
	public static final String FIELD_SHOP_CATEGORIES = "categories";
	public static final String FIELD_SHOP_IMAGE_FILE = "image_file";
	public static final String FIELD_SHOP_IMAGE_WIDTH = "image_width";
	public static final String FIELD_SHOP_IMAGE_HEIGHT = "image_height";
	public static final String FIELD_SHOP_ADDITIONAL_IMAGES = "additional_images";
	public static final String FIELD_SHOP_FOLDER_PATHS = "folder_paths";
	public static final String FIELD_SHOP_IS_CUSTOMIZED = "is_customized";
	public static final String FIELD_SHOP_PRODUCT_ID = "product_id";
	public static final String FIELD_SHOP_ITEM_ID = "item_id";

	// index fields for sink
	public static final String FIELD_SINK_DOCUMENT_ID = "document_id";
	public static final String FIELD_SINK_MARKETING_TAG = "marketing_tag";
	public static final String FIELD_SINK_TITLE = "title";
	public static final String FIELD_SINK_DESCRIPTION = "description";
	public static final String FIELD_SINK_SEARCHABLE = "searchable";
	public static final String FIELD_SINK_SORTABLE = "sortable";
	public static final String FIELD_SINK_CONTENT_TYPE = "content_type";
	public static final String FIELD_SINK_SITE_CODE = "site_code";
	public static final String FIELD_SINK_LATITUDE = "lat";
	public static final String FIELD_SINK_LONGITUDE = "lng";
	// field used for hadoop
	// the idea is that the value of this key will be used to name the directory
	// where the SOLR index is written
	public static final String FIELD_MAP_TASK_ID = "map_task_id";
}
