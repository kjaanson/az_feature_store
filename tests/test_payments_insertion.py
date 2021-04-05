

import pytest

import pandas as pd

from .fixtures import payments_container

def test_single_payment_insertion(payments_container):


    payload = { 'id': '0',
                'step': '1',
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


    items = list(payments_container.query_items(
        query="SELECT * FROM c",
        enable_cross_partition_query=True
    ))

    assert len(items) == 1

def test_multiple_payment_insertion(payments_container):


    payload = {'id': '0',
                'step': '1',
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

    items = list(payments_container.query_items(
        query="SELECT * FROM c",
        enable_cross_partition_query=True
    ))

    assert len(items) == 3
