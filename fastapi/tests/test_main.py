import pytest

from fastapi.testclient import TestClient
from app import main
from app.main import app
from urllib.parse import quote_plus


def mocked_read_voucher_frequent_most_used(country_code: str, segment: str) -> int:
    voucher_amount = None
    if country_code == 'Peru':
        if segment == '0-4':
            voucher_amount = None
        elif segment == '5-13':
            voucher_amount = 2640
        elif segment == '13-37':
            voucher_amount = 2640
    return voucher_amount


@pytest.fixture()
def client():
    with TestClient(app) as test_client:
        yield test_client


def test_read_voucher_most_used_frequent_exist(client, monkeypatch):
    monkeypatch.setattr(main, "__read_voucher_frequent_most_used", mocked_read_voucher_frequent_most_used)
    param_str = """{"customer_id": 123,"country_code": "Peru","last_order_ts": "2022-03-01 00:00:00","first_order_ts": "2017-05-03 00:00:00","total_orders": 15,"segment_name": "frequent_segment"}"""
    param_encoded = quote_plus(param_str)
    response = client.get("/voucher/most-used?customer={}".format(param_encoded))
    
    assert response.status_code == 200
    assert response.json() == {'voucher_amount': 2640}


def test_read_voucher_most_used_frequent_not_exist(client, monkeypatch):
    monkeypatch.setattr(main, "__read_voucher_frequent_most_used", mocked_read_voucher_frequent_most_used)
    param_str = """{"customer_id": 123,"country_code": "Peru","last_order_ts": "2022-03-01 00:00:00","first_order_ts": "2017-05-03 00:00:00","total_orders": 2,"segment_name": "frequent_segment"}"""
    param_encoded = quote_plus(param_str)
    response = client.get("/voucher/most-used?customer={}".format(param_encoded))

    assert response.status_code == 200
    assert response.json() == {'voucher_amount': None}
