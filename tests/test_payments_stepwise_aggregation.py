
import os
import pytest
import time

import pandas as pd

from .fixtures import spark, payments_container, payments_aggr_container

from aggregators.payment_step_counts import aggregate_payments



def test_single_payment_insertion(spark, payments_container, payments_aggr_container):


    payload = { 'id': '0',
                'step': 1,
                'type': 'PAYMENT',
                'amount': 9839.64,
                'nameOrig': 'C1231006815',
                'oldbalanceOrg': 170136.0,
                'newbalanceOrig': 160296.36,
                'nameDest': 'M1979787155',
                'oldbalanceDest': 0.0,
                'newbalanceDest': 0.0,
                'isFraud': 0,
                'isFlaggedFraud': 0}

    payments_container.create_item(body=payload)

    payments_aggregator = aggregate_payments(spark)
    payments_aggregator.start()

    time.sleep(20)

    items = list(payments_aggr_container.query_items(
        query="SELECT * FROM c",
        enable_cross_partition_query=True
    ))

    assert len(items) == 1

    aggr_row = {key:v for key, v in items[0].items() if key in ('step','type','count')}

    assert aggr_row == {
        'step': 1,
        'type': 'PAYMENT',
        'count': 1
    }



def test_multiple_payment_insertion(spark, payments_container, payments_aggr_container):


    payload = {'id': '0',
                'step': 1,
                'type': 'PAYMENT',
                'amount': 9839.64,
                'nameOrig': 'C1231006815',
                'oldbalanceOrg': 170136.0,
                'newbalanceOrig': 160296.36,
                'nameDest': 'M1979787155',
                'oldbalanceDest': 0.0,
                'newbalanceDest': 0.0,
                'isFraud': 0,
                'isFlaggedFraud': 0}

    for i in range(0, 3):
        payload['id']=str(i)
        payments_container.create_item(body=payload)

    payments_aggregator = aggregate_payments(spark)
    payments_aggregator.start()

    time.sleep(20)

    items = list(payments_aggr_container.query_items(
        query="SELECT * FROM c",
        enable_cross_partition_query=True
    ))

    assert len(items) == 1

    aggr_row = {key:v for key, v in items[0].items() if key in ('step','type','count')}

    assert aggr_row == {
        'step': 1,
        'type': 'PAYMENT',
        'count': 3
    }


