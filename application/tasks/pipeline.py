from logging import getLogger

from datetime import datetime

from typing import Optional
from application.db_extension.routines import (validate_pipeline_run,
                                               source_into_pipeline_copy,
                                               seeding_products_func,
                                               assign_prototypes_to_products,
                                               assign_themes_to_products,
                                               pipe_aggregate,
                                               update_price_qoh)
from application.db_extension.models import db
from application.db_extension.models import PipelineSequence
from .information_extraction import PipelineExtractor

QUEUED = "queued"
EXECUTING = "executing"
AGGREGATING = 'aggregating'
READY = "ready"
ERROR = "error"

logger = getLogger(__name__)


def execute_pipeline_inc(products: list, source_id: int,
                         sequence_id: Optional[int] = None):
    for product in products:
        update_price_qoh()


def execute_pipeline_full(source_id: int,
                     sequence_id: Optional[int] = None,
                     debug_product_search_str: Optional[str] = None
                     ) -> dict:
    """
    Body:
        {category_id:1, source_id:6, sequence_id:123 [optional])

        Process:
        Check to see if pipeline_sequence.sequence_id already exists. If it does not exist (or is null), insert a new
        row into pipeline_sequence and set values for category_id and source_id.
        Set value of pipeline_sequence.start_time = now(), status = "executing".
        Call the postgres function: "SELECT * FROM source_into_pipeline_copy_func(sequence_id, category_id, source_id);"
        Call the postgres function: "SELECT * FROM seeding_products_func(sequence_id, category_id, source_id);"
        Execute the Python information extraction function (pipeline_information_extraction).
        Execute "SELECT * FROM public.pipe_aggregate(category_id, source_id, sequence_id, False);"
        Set value of pipeline_sequence.end_time = now(), status = "ready"
        Return category_id (the new id created when the row was created or, if the row already existed, the sequence_id
        called with).

        If any of the above functions returns an error, then set pipeline_sequence.status="error" and return early.
    """
    logger.info("execute_pipeline: starts, sequence_id=" + str(sequence_id))
    start = datetime.now()

    if sequence_id:
        sequence = db.session.query(PipelineSequence).get(sequence_id)
    else:
        sequence = None

    if not sequence:
        sequence = PipelineSequence(
            source_id=source_id,
            comments='',
            start_time=datetime.now(),
            is_active=True,
            state=EXECUTING
        )
        db.session.add(sequence)
        db.session.commit()
        sequence_id = sequence.id
    else:
        sequence.start_time = datetime.now()
        sequence.state = EXECUTING
        db.session.commit()

    try:
        logger.info("execute_pipeline: source_into_pipeline_copy_func starts.")
        source_into_pipeline_copy(sequence_id=sequence_id, source_id=source_id)
        logger.info("execute_pipeline: seeding_products_func starts.")
        seeding_products_func(sequence_id=sequence_id, source_id=source_id)
        logger.info(
            "execute_pipeline: pipeline_information_extraction starts.")
        extractor = PipelineExtractor(source_id=source_id,
                                      sequence_id=sequence_id,
                                      debug_product_search_str=debug_product_search_str)
        extractor.process_all()

        logger.info("execute_pipeline: pipe_aggregate starts")
        sequence.state = AGGREGATING

        result = pipe_aggregate(source_id=source_id, sequence_id=sequence_id)

        status_msg = result[0] if result else 'Empty'
        logger.info(f'execute_pipeline: pipe_aggregate: '
                    f'Status Msg: {status_msg}')
        if not status_msg.startswith("pipe_aggregate"):
            raise Exception(f'pipe_aggregate is not successful, '
                            f'status msg: {status_msg}')

        logger.info("execute_pipeline: assign_prototypes_to_products starts")
        assign_prototypes_to_products(source_id=source_id, sequence_id=sequence_id)
        logger.info("execute_pipeline: assign_themes_to_products starts")
        assign_themes_to_products(source_id=source_id, sequence_id=sequence_id)

    except BaseException as e:
        db.session.rollback()
        set_completion_status(sequence=sequence,
                              source_id=source_id,
                              exception=e)
        db.session.commit()
        # print(traceback.format_exception(None, e, e.__traceback__))
        logger.error(
            f'execute_pipeline: Exception occurred: error:{e.args}')
        logger.exception('execute_pipeline: got exception')
        raise e

    set_completion_status(sequence=sequence, source_id=source_id)
    db.session.commit()

    print(sequence.__dict__)
    logger.info(
        f'pipeline_execute: pipeline processing is completed,'
        f' timetaken={datetime.now() - start}')

    return {'sequence_id': sequence_id}


#
#  This method is used to set the pipeline sequence status at the end of each run
# 1) First it validates the pipeline run by calling validate_pipeline_run.sql
# 2) If status is true , it will set the sequence status to READY
# otherwise, it will set state to ERROR and send email to Admin
# 3) Set sequence end time to current time
#
def set_completion_status(sequence, source_id,
                          exception=None):
    # Validate the pipeline input and output
    logger.info("execute_pipeline: validate pipeline execution")
    if exception is None:
        status = validate_pipeline_run(sequence.id, source_id)
    else:
        status = False

    if status:
        logger.info("pipeline_execute: setting state to READY")
        sequence.state = READY
    else:
        logger.error("pipeline_execute: setting state to ERROR")
        sequence.state = ERROR
    sequence.end_time = sequence.updated = datetime.now()
