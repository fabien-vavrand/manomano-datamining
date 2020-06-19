import os
import logging
import progressbar
import pandas as pd
from aikit.tools.helper_functions import load_pkl, save_pkl
from mano.data.utils import load_data_from_dirty_json_file, chunks


class DataManager:

    JSON_FIELDS_TO_MAP = [
        {
            'keys': ['delivery_offers', 'min_fee', 'as_float'],
            'key': 'delivery_offers_min_fee'
        },
        {
            'keys': ['prices', 'per_item', 'unit'],
            'key': 'prices_per_item_unit'
        }
    ]

    ## Fields tested and removed because lots of nan or high correlations with other fields
    JSON_FIELDS_TO_MAP_REMOVED = [
        {
            'keys': ['delivery_offers', 'min_time_fee', 'as_float'],
            'key': 'delivery_offers_min_time_fee'
        },
        {
            'keys': ['prices', 'main_price'],
            'key': 'prices_main_price'
        },
        {
            'keys': ['prices', 'secondary_price'],
            'key': 'prices_secondary_price'
        },
        {
            'keys': ['prices', 'per_item', 'actual_price', 'with_vat', 'as_float'],
            'key': 'prices_per_item_actual_price_with_vat'
        },
        {
            'keys': ['prices', 'per_item', 'actual_price', 'without_vat', 'as_float'],
            'key': 'prices_per_item_actual_price_without_vat'
        },
        {
            'keys': ['prices', 'per_item', 'retail_price', 'with_vat', 'as_float'],
            'key': 'prices_per_item_retail_price_with_vat'
        },
        {
            'keys': ['prices', 'per_item', 'retail_price', 'without_vat', 'as_float'],
            'key': 'prices_per_item_retail_price_without_vat'
        }
    ]

    JSON_FIELDS_TO_DELETE = [
        'detail_price', 'is_seller_b2b', 'is_mmf', 'has_3x_payment', 'market', 'model_markets', 'is_sample', 'has_sample',
        'image_fullpath', 'brand_image_fullpath', 'url', 'default_title', 'legacy_unit',
        'attribute_facet', 'top_attributes', 'catalog_attribute', 'reranking_positions', 'reranking_positions.alternate',
        'categories.l3', 'categories.l3.id', 'categories.last_id', 'category_slug', 'banner.alternate',
        'experiences', 'score', 'me_id', 'energy_efficiency',
        'seller_id', 'brand_id', 'categories.l0.id', 'categories.l1.id', 'categories.l2.id', 'categories.last.id'
    ]

    COLUMNS_TO_DELETE = [
        'image_path', 'brand_image_path', 'thumbnails', 'catalog_attribute_facet', 'banner.categories',
        'banner.default'
    ]

    COLUMNS = [
        'objectID', 'model_id', 'article_id', 'title',
        'price', 'vat_rate', 'ecopart', 'discount', 'delivery_offers_min_fee', 'ranking_score_v1',
        'seller_name', 'seller_country_id', 'brand_name', 'rating', 'rating_count',
        'unit_type', 'unit_price', 'min_quantity', 'models_count',
        'categories.l0', 'categories.l1', 'categories.l2', 'categories.last',
        'n_categories.l0', 'n_categories.l1', 'n_categories.l2', 'n_thumbnails', 'n_attributes', 'n_topsales',
        'has_brand_image','has_free_delivery', 'has_relay_delivery', 'has_1day_delivery', 'on_sale', 'indexable'
    ]

    def __init__(self, path):
        self.path = path
        self.data_path = os.path.join(path, 'data')
        self.processed_path = os.path.join(path, 'processed')
        self.cache_file = os.path.join(path, 'cache.pkl')
        self.files = sorted(os.listdir(self.data_path))
        self.mapping = {}

        if not os.path.exists(self.processed_path):
            os.mkdir(self.processed_path)

    def load(self):
        """ Process and load the scraped data to a pandas dataframe """
        if os.path.exists(self.cache_file):
            return load_pkl(self.cache_file)

        self._process_files_chunks()
        results = self._concat_chunks()
        save_pkl(results, self.cache_file)
        return results

    def _process_files_chunks(self):
        """ Process files by chunks of ~1000 to allow fast recovery """
        files_chunks = chunks(self.files, 1000)

        for i, files in enumerate(files_chunks):
            logging.info('---- Chunk {}'.format(i))
            processed_file = os.path.join(self.processed_path, 'chunk_{}.pkl'.format(i))
            if os.path.exists(processed_file):
                continue

            results = []
            for file in progressbar.progressbar(files):
                r = self._process_file(file)
                if r is not None:
                    results.append(r)

            results = pd.concat(results, sort=False)
            # Drop duplicates objects
            results = results.drop_duplicates('objectID')
            save_pkl(results, processed_file)

    def _concat_chunks(self):
        """ Concatenate the processed chunks into one dataset"""
        results = [load_pkl(os.path.join(self.processed_path, chunk)) for chunk in os.listdir(self.processed_path)]
        results = pd.concat(results, sort=False)
        results = results[self.COLUMNS]
        results = results.drop_duplicates('objectID')
        results = results.utils.to_categoricals()
        return results

    def _process_file(self, file):
        """ Process a single file """
        file_path = os.path.join(self.data_path, file)
        with open(file_path, 'r', encoding='utf-8') as file_stream:
            data = file_stream.read()

        data = load_data_from_dirty_json_file(data)
        if data is None:
            return None
        data = data[0]['hits']
        data = self._preprocess_json(data)
        data = pd.json_normalize(data)
        data = self._reduce_memory_size(data)
        data = data[[c for c in self.COLUMNS if c in data.columns]]
        return data

    def _preprocess_json(self, data):
        """ Remove useless info from json data to facilitate dataframe conversion """
        for d in data:
            # We map the catalog_attribute_facet to a list of label / value for it to fit in a single dataframe column
            if 'catalog_attribute_facet' in d:
                d['catalog_attribute_facet'] = [{'label': k, 'value': v} for k, v in d['catalog_attribute_facet'].items()]

            for mapping in self.JSON_FIELDS_TO_MAP:
                self._map_json(d, mapping['keys'], mapping['key'])

            # delete useless fields after having performed mapping
            for key_to_delete in set([m['keys'][0] for m in self.JSON_FIELDS_TO_MAP]):
                if key_to_delete in d:
                    del d[key_to_delete]

            # delete useless fields
            for field in self.JSON_FIELDS_TO_DELETE:
                if field in d:
                    del d[field]
        return data

    def _reduce_memory_size(self, data):
        # Strip some text
        data['title'] = data['title'].apply(lambda s: s.strip())

        # We compress some features we don't want to use as is, like arrays
        #data['has_image'] = data['image_path'].apply(lambda x: ~pd.isnull(x) and len(x) > 0)
        data['has_brand_image'] = data['brand_image_path'].apply(lambda x: ~pd.isnull(x) and len(x) > 0)

        def get_length(x):
            try:
                return len(x)
            except Exception as e:
                return 0

        def get_unique_length(x):
            try:
                return len(set(x))
            except Exception as e:
                return 0

        def get_topsales_length(x):
            try:
                return len([ts for ts in x if 'topSales' in ts])
            except Exception:
                return 0

        data['n_thumbnails'] = data['thumbnails'].apply(get_length)
        if 'catalog_attribute_facet' in data.columns:
            data['n_attributes'] = data['catalog_attribute_facet'].apply(get_length)
        else:
            data['n_attributes'] = 0
        if 'banner.categories' in data.columns:
            data['n_topsales'] = data['banner.categories'].apply(get_topsales_length)
        else:
            data['n_topsales'] = 0

        data['n_categories.l0'] = data['categories.l0'].apply(get_unique_length)
        data['n_categories.l1'] = data['categories.l1'].apply(get_unique_length)
        data['n_categories.l2'] = data['categories.l2'].apply(get_unique_length)

        # We assign to main categories, and map categories to categories.id
        data['categories.l0'] = data['categories.l0'].apply(lambda a: a[0].strip())
        data['categories.l1'] = data['categories.l1'].apply(lambda a: a[0].strip())
        data['categories.l2'] = data['categories.l2'].apply(lambda a: a[0].strip())
        data['categories.last'] = data['categories.last'].apply(lambda a: a[0].strip())

        to_delete = list(set(self.COLUMNS_TO_DELETE).intersection(data.columns))
        data = data.drop(columns=to_delete)
        # We downcast int types to save some memory
        data = data.utils.downcast_int_columns()
        return data

    @staticmethod
    def _map_json(data, initial_keys, final_key):
        """ Simplify the json by pushing interesting data to top level """
        current_node = data
        for key in initial_keys:
            if key in current_node:
                current_node = current_node[key]
            else:
                current_node = None
            if current_node is None:
                break
        # map value to top level
        if current_node is not None:
            data[final_key] = current_node


