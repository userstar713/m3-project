#!/usr/bin/env python
from application import create_app
from application.db_extension.models import db, DomainAttribute

ATTRIBUTES = [
 (1, 'sku', False, False, False, False, False, False, 'sku', '', '[]', 'text', None, 0, False, None, False),
 (1, 'designation', False, False, False, False, True, False, 'designation', '', '[]', 'text', None, 0, False, None, False),
 (1,'price', False, False, False, True, True, False, 'price', '$#,##0.00', '["price range", "cost", "how much"]', 'currency', None, 15, False, None, False),
 (1,'MSRP', False, False, False, False, True, False, 'msrp', '', '[]', 'currency', None, 0, False, None, False),
 (1, 'characteristic', True, False, True, False, True, True, 'characteristics', '', '["flavors", "taste", "taste like", "tastes", "aromas", "smell", "smells", "flavor", "note", "notes", "tones", "tone", "bouquet", "nose", "hints", "aftertaste", "after taste"]', 'text', None, 0, False, None, False),
 (1, 'fortified', False, False, False, False, True, True, 'fortified', '', '[]', 'boolean', None, 0, False, None, False),
 (1, 'description', False, False, False, False, True, False, 'description', '', '[]', 'text', None, 0, False, None, False),
 (1, 'image', False, False, True, False, False, False, 'image', '', '[]', 'text', None, 0, False, None, False),
 (1, 'date added', False, False, False, False, True, False, 'date_added', '', '[]', 'datetime', None, 0, False, None, False),
 (1, 'single product url', False, False, False, False, False, False, 'single_product_url', '', '[]', 'text', None, 0, False, None, False),
 (1, 'is vintage wine', False, False, False, False, True, False, 'is_vintage_wine', '', '[]', 'boolean', None, 0, False, None, False),
 (1, 'drink from', False, False, False, False, False, True, 'drink_from', '', '[]', 'integer', None, 0, False, None, False),
 (1, 'drink to', False, False, False, False, False, True, 'drink_to', '', '[]', 'integer', None, 0, False, None, False),
 (1, 'blend', False, False, False, False, True, True, 'is_blend', '', '[]', 'boolean', None, 0, False, None, False),
 (1,'name', False, False, False, True, False, False, 'name', '', '[]', 'text', None, 0, False, None, False),
 (1, 'acidity', True, True, False, False, True, True, 'acidity', ' ', '["acid level", "acid", "acidic"]', 'float', None, 0, False, None, False),
 (1, 'body', True, True, False, False, True, True, 'body', '', '["type of body", "body type"]', 'float', None, 0, False, None, False),
 (1, 'tannin', True, True, False, False, True, True, 'tannin', '', '["tannins", "mouthfeel", "texture"]', 'float', None, 0, False, None, False),
 (1, 'alcohol', False, False, False, False, True, True, 'alcohol_pct', '##.#%', '["abv", "alc"]', 'float', None, 0, False, None, False),
 (1, 'sweetness', True, True, False, False, True, False, 'sweetness', '', '["sugar", "residual sugar", "RS"]', 'float', None, 0, False, None, False),
 (1,'varietal', True, False, True, False, True, False, 'varietals', '', '["grapes", "grape types", "type of grape", "grape", "made from"]', 'text', 0.25, 0, False, None, False),
 (1, 'popularity', False, False, False, True, False, False, 'popularity', '', '[]', 'float', None, 0, False, None, False),
 (1, 'purpose', True, False, True, False, True, False, 'purpose', '', '[]', 'text', None, 0, False, None, False),
 (1, 'flaw', True, False, False, False, True, False, 'flaw', '', '["bad flavors", "off flavors"]', 'text', None, 0, False, None, False),
 (1,'vintage', True, False, False, False, True, False, 'vintage', '####', '["year", "years"]', 'text', None, 0, False, None, False),
 (1, 'qpr', True, False, False, False, True, False, 'qpr', '', '["value", "deal", "quality to price", "price to quality"]', 'float', None, 0, False, None, False),
 (1, 'style', True, False, True, False, True, False, 'styles', '', '[]', 'text', None, 0, True, None, False),
 (1, 'bottle size', True, False, False, True, True, False, 'bottle_size', '', '["size", "bottle type"]', 'text', None, 0, False, None, False),
 (1, 'highlights', True, False, True, False, True, False, 'highlights', '', '[]', 'text', None, 0, False, None, False),
 (1, 'color', True, True, False, False, True, False, 'color_intensity', '', '[]', 'float', None, 0, False, None, False),
 (1, 'discount', False, False, False, False, True, False, 'discount_pct', '#0%', '["discount", "discounts", "off", "discount_pct", "percent discount", "percent off", "save", "savings", "saving", "discounted", "bargains", "bargain", "best buys", "deals"]', 'float', None, 0, False, None, False),
 (1,'food', True, False, True, False, True, False, 'foods', '', '["dinner", "type of food"]', 'text', None, 0, False, None, False),
 (1, 'critic score', False, False, False, False, True, False, 'rating', '# points', '["press", "score", "rating", "quality", "points", "point", "rated", "scoring", "award", "awards", "pt", "pts"]', 'float', None, 90, False, None, False),
 (1, 'product_id', False, False, False, False, False, False, 'product_id', '', '[]', 'text', None, 0, False, None, False),
 (1, 'qoh', False, False, False, False, True, False, 'qoh', '', '["inventory", "stock"]', 'integer', None, 0, False, None, False),
 (1,'wine type', True, False, False, True, True, False, 'wine_type', '', '["categories of wine", "category of wine", "type", "type of wine", "types of wine", "wine types"]', 'text', None, 0, False, None, False),
 (1,'region', True, False, False, True, True, False, 'region', '', '["where", "regions", "country", "countries", "area", "AVA", "AOC", "DOCG", "come from", "comes from"]', 'text', 0.5, 0, False, None, False),
 (1,'winery', True, False, False, True, True, False, 'brand', '', '["brand", "winery", "wineries", "producer"]', 'text', 0.25, 0, False, None, False),
 (1, 'prototype', True, False, True, False, True, False, 'prototype', '', '[]', 'node_id', None, 0, False, None, False),
]

KEYS = ('category_id',
        'name',
        'has_taxonomy',
        'taxonomy_is_scored',
        'is_multivalue',
        'is_required',
        'is_runtime',
        'should_extract_values',
        'code',
        'display_format',
        'aliases',
        'datatype',
        'popularity_weight',
        'default_value',
        'should_extract_from_name',
        'extract_content_support',
        'taxonomy_is_binary')

if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        for values in ATTRIBUTES:
            da = DomainAttribute(**dict(zip(KEYS, values)))
            db.session.add(da)
        db.session.commit()