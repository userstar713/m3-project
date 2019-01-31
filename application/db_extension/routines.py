from .models import db, DomainCategory
from typing import List, Optional
from flask import current_app
from application.caching import cache
import re
from funcy import log_durations


def validate_pipeline_run(sequence_id: int, source_id: int):
    return True  # FIXME always return true
    q = """SELECT check_type, status 
           FROM public.validate_pipeline_run(:category_id, 
                                             :source_id, 
                                             :sequence_id)"""
    rows = db.session.execute(q,
                              {'category_id': get_default_category_id(),
                               'source_id': source_id,
                               'sequence_id': sequence_id})
    if rows is None or len(rows) < 2:
        return False
    for row in rows:
        if not row['status']:
            return False
    return True


def filter_tsquery(s):
    return s.replace("'", " ").replace("!", '').replace("(", '').replace(")",
                                                                         '')


@cache.memoize(timeout=60 * 60 * 24 * 7)
def attribute_lookup(sentence,
                     brand_treatment='exclude',
                     attribute_code=False):
    """
    function attribute_lookup2(p_category_id bigint,
                               p_search_str text,
                               p_predicate_str text,
                               p_brand_treatment character
                                  varying DEFAULT 'include'::character varying,
                               p_should_extract_values
                                  boolean DEFAULT NULL::boolean,
                               p_should_restrict_nodes boolean DEFAULT true)

    • category_id: category
    • p_search_str: string we searching for attributes in
    • p_predicate_str: string that contains an optional attribute predicate
      (e.g., "goes with" is a predicate for food, "comes from" is a predicate
      for region). This was used to help to disambiguate attribute values
      ("tastes like cherry" vs. "goes with cherry pie"). I think we can remove
      for M3 initially, although we may have to look at adding something back
      in the future (and would have to dig out predicate detection code from M2
      Python code).
    • p_brand_treatment: should we allow brands to be returned in the results.
      When we know we're not looking for a brand (e.g., we know we're looking
      for a particular attribute), it's safer and faster to exclude brands.
    • p_should_extract_values: Whether we want to the original user-specified
      version of attribute value to be returned in result set as 'original'
       (necessary for human interactions, but not M3)
    • p_should_restrict_nodes: This restricts brand selection to only those
     that we have from any retailer (that is, if the brand exists in
     domain_taxonomy_nodes then we will include it). Again, this is useful for
      human interactions (after we've run the pipeline for all sources) to
       avoid too many false-positives on brands based on human input. But it
       doesn't make sense in M3.

    Later, we can remove p_predicate_str, p_should_extract_values and
     p_should_restrict_nodes. In the meantime, set p_predicate_str to NULL
      and the others to False.
        :param sentence:
    :param index:
    :param brand_treatment:
    :param should_restrict_nodes:
    :param predicate:
    :return:
    """
    # Remove potentially problematic chars
    sentence = re.sub('[^A-Za-z0-9$]+', ' ', sentence).lstrip()

    q = """SELECT *
           FROM public.attribute_lookup2 (:category_id,
                                          :sentence,
                                          :brand_treatment);
        """
    if attribute_code:
        q = q.replace(':brand_treatment', ':brand_treatment, :attribute_code')
    # Note 'exclude' because we don't extract brands and False because we don't
    # want to constrain to current store (which we are populating!)
    rows = db.session.execute(q, {
        'category_id': get_default_category_id(),
        'sentence': filter_tsquery(sentence),
        'brand_treatment': brand_treatment,
        'attribute_code': attribute_code,
    })
    attributes = []
    for row in rows:
        atts = row[0].get('attributes')
        if atts:
            attributes.extend(atts)
    return attributes


def domain_attribute_lookup(sentence):
    result = attribute_lookup(sentence)
    return {'attributes': result, 'extra_words': []}


#

def _domain_attribute_lookup(sentence):
    print('sentence')
    index = 0
    result = attribute_lookup(sentence)
    final_result = []
    for rs in result:
        frs = {
            'conjunction': rs['conjunction'],
            'predicate_attribute_code': rs['predicate_attribute_code'],
            'remaining_words': rs['remaining_words']
        }

        if rs['attributes']:
            frs['attributes'] = []
            for atb in rs['attributes']:

                # Because postgres call can return two different attributes with same value, we want to
                # only allow one of these. So disallow 2nd+
                found_attrs = list(filter(
                    lambda a: (a.get('value', None) == atb['value']
                               and a.get('code', None) != atb['code']),
                    frs['attributes']))

                if not found_attrs:
                    obj = {
                        'code': atb['code'],
                        'value': atb['value'],
                        'original': atb.get('original', atb['value']),
                        'start': atb['start'] + index,
                        'end': atb['end'] + index,
                    }

                    # To deal with nlp limitations in the explainer,
                    # for the original (alias) values, we only want to show them if they have the same POS as the
                    # base value. e.g., POS(salmon) == POS(fish). However, POS(fine tannin) != POS(smooth), so in
                    # the latter case, let's just use the base value instead of original.
                    # We'll only look at the last word for now (revert is POS not the same)
                    # Also, just look at the first two chars in POS (e.g., so VBN == VBD)

                    # attr = list(filter(lambda a: a.get('code', None) == atb['code'], all_attributes))[0]
                    # if get_pos_for_word(obj['value'].split()[-1])[:2] != get_pos_for_word(obj['original'].split()[-1])[:2]:
                    # if attr.get('pos', None)[:2] != get_pos_for_word(obj['original'].split()[-1])[:2]:
                    #    obj['original'] = obj['value']

                    # Add the node expression text if it's there
                    derived_definition = atb.get('derived_definition', None)

                    # Substitute {value} with alias if it exists
                    if derived_definition:
                        pos = derived_definition.find('{value}')
                        if pos > -1:
                            word = obj[
                                'original'].capitalize() if pos == 0 else obj[
                                'original']
                            obj[
                                'derived_definition'] = derived_definition.replace(
                                '{value}', word)
                        else:
                            obj['derived_definition'] = derived_definition

                    frs['attributes'].append(obj)

            frs['attributes'] = sorted(frs['attributes'],
                                       key=lambda attribute: attribute[
                                           'start'])
        else:
            frs['attributes'] = []

        final_result.append(frs)

    return {'attributes': final_result, 'extra_words': []}


def get_description_contents_data(sequence_id: int, source_id: int):
    q = """SELECT master_product_id,value_text 
           FROM pipeline_attribute_values pav, domain_attributes da
           WHERE pav.source_id=:source_id
           AND pav.sequence_id=:sequence_id
           AND pav.attribute_id=da.id 
           AND da.code = 'description'
           """
    rows = db.session.execute(q,
                              {'source_id': source_id,
                               'sequence_id': sequence_id}
                              )
    if rows:
        result = [(row.master_product_id, row.value_text) for row in rows]
    else:
        result = []
    return result


def configure_default_category_id():
    category = db.session.query(DomainCategory.id).first()
    if not category:
        category = DomainCategory(id=1, name='wine')
        db.session.add(category)
        db.session.commit()
    current_app.config['DEFAULT_CATEGORY_ID'] = category.id


def get_default_category_id():
    return current_app.config['DEFAULT_CATEGORY_ID']


def source_into_pipeline_copy(sequence_id: int, source_id: int) -> None:
    q = """SELECT * 
           FROM public.source_into_pipeline_copy_func(:sequence_id, 
                                                      :category_id, 
                                                      :source_id);"""
    db.session.execute(q,
                       {
                           'sequence_id': sequence_id,
                           'category_id': get_default_category_id(),
                           'source_id': source_id
                       })
    db.session.commit()


def seeding_products_func(sequence_id: int, source_id: int) -> None:
    q = """SELECT * 
           FROM public.seeding_products_func(:sequence_id, 
                                             :category_id,  
                                             :source_id); 
        """
    db.session.execute(q,
                       {
                           'sequence_id': sequence_id,
                           'category_id': get_default_category_id(),
                           'source_id': source_id
                       })
    db.session.commit()


def pipe_aggregate(source_id: int, sequence_id: int) -> List:
    q = """SELECT * 
           FROM public.pipe_aggregate(:category_id, 
                                      :source_id, 
                                      :sequence_id, 
                                      :calculate_aggregates);"""
    result = db.session.execute(q,
                                {
                                    'sequence_id': sequence_id,
                                    'category_id': get_default_category_id(),
                                    'source_id': source_id,
                                    'calculate_aggregates': False
                                }).first()
    db.session.commit()
    return result


def update_price_qoh(m_product_id: int, price: float, qoh: int):
    q = ('SELECT * '
         'FROM public.update_price_m3('
         '    :m_product_id,'
         '    :price,'
         '    :qoh)'
         )
    db.session.execute(
        q,
        {'m_product_id': m_product_id,
         'price': price,
         'qoh': qoh}
    ).first()
    db.session.commit()


def assign_to_products(
        f_name: str,
        source_id: int,
        sequence_id: int,
        category_id: Optional[int] = None,
        is_partial: bool = False):
    q = ('SELECT * '
         f'FROM public.{f_name}('
         '    :category_id,'
         '    :source_id,'
         '    :sequence_id,'
         '    :is_partial)'
         )
    if category_id is None:
        category_id = get_default_category_id()
    db.session.execute(
        q,
        {'category_id': category_id,
         'source_id': source_id,
         'sequence_id': sequence_id,
         'is_partial': is_partial}
    ).first()
    db.session.commit()


def assign_prototypes_to_products(
        source_id: int,
        sequence_id: int,
        category_id: Optional[int] = None,
        is_partial: bool = False):
    assign_to_products('assign_prototypes_to_products',
                       source_id,
                       sequence_id,
                       category_id,
                       is_partial)


def assign_themes_to_products(
        source_id: int,
        sequence_id: int,
        category_id: Optional[int] = None,
        is_partial: bool = False):
    assign_to_products('assign_themes_to_products',
                       source_id,
                       sequence_id,
                       category_id,
                       is_partial)
