from application.db_repo.models import db
from application.db_repo.models.domain_attribute import DomainAttribute
from application.db_repo.models.source import Source
from application.db_repo.models.domain_taxonomy_node import DomainTaxonomyNode
from application.db_repo.models import master_product
from application.db_repo.models import source_product
from application.db_repo.models import source_attribute_value
from application.db_repo.models.domain_reviewer import DomainReviewer
from application.db_repo.models import pipeline_sequence
from application.db_repo.models.pipeline_attribute_value import PipelineAttributeValue
from application.db_repo.models.pipeline_review_content import PipelineReviewContent
from application.db_repo.models import source_review
from application.db_repo.models.domain_category import DomainCategory
from application.db_repo.models.source_location import SourceLocation

from application.db_repo.models import source_location
from application.db_repo.models import source_location_product

from sqlalchemy.sql import ClauseElement
from sqlalchemy.dialects import postgresql

from typing import Iterable


class GetIdOrCreateMixin:
    #@classmethod
    #def get_or_create(cls, defaults=None, **kwargs) -> (ClauseElement, bool):
    #    instance = db.session.query(cls).filter_by(**kwargs).first()
    #    if instance:
    #        return instance, False
    #    else:
    #        params = dict((k, v) for k, v in kwargs.items() if
    #                      not isinstance(v, ClauseElement))
    #        params.update(defaults or {})
    #        instance = cls(**params)
    #        db.session.add(instance)
    #        return instance, True
    @classmethod
    def get_by(cls, **kwargs):
        return cls.query.filter_by(**kwargs).first()

    @classmethod
    def get_or_create(cls, **kwargs):
        r = cls.get_by(**kwargs)
        if not r:
            r = cls.create(**kwargs)
            return r, True
        else:
            return r, False

    @classmethod
    def create(cls, **kwargs):
        r = cls(**kwargs)
        db.session.add(r)
        return r



class BulkInsertDoNothingMixin:

    @classmethod
    def bulk_insert_do_nothing(cls, items: Iterable[dict]) -> None:
        if items:
            db.session.bulk_insert_mappings(
                cls, items
            )
            db.session.commit()

class SourceLocationProductProxy(source_location_product.SourceLocationProduct, GetIdOrCreateMixin):
    pass

class MasterProductProxy(master_product.MasterProduct, GetIdOrCreateMixin):
    pass


class SourceProductProxy(source_product.SourceProduct, GetIdOrCreateMixin):
    pass


class SourceAttributeValue(source_attribute_value.SourceAttributeValue,
                           BulkInsertDoNothingMixin):
    pass


class SourceReview(source_review.SourceReview,
                           BulkInsertDoNothingMixin):
    pass

class PipelineSequence(pipeline_sequence.PipelineSequence):
    def get_latest_sequence_id(self):
        pass