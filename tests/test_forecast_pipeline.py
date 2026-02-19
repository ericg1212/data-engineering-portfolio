"""Tests for forecast_pipeline DAG: transform and load functions."""

import json
import os
import pytest

from forecast_pipeline.forecast_pipeline import transform_forecast, load_to_s3

# ── Sample data ───────────────────────────────────────────────────────────────

RAW_FORECAST = {
    'city': {'name': 'Brooklyn'},
    'list': [
        {
            'dt_txt': '2026-02-19 06:00:00',
            'main': {'temp': 38.5, 'feels_like': 32.1, 'humidity': 88},
            'weather': [{'description': 'light snow'}],
            'wind': {'speed': 8.2},
        },
        {
            'dt_txt': '2026-02-19 09:00:00',
            'main': {'temp': 40.1, 'feels_like': 34.0, 'humidity': 82},
            'weather': [{'description': 'overcast clouds'}],
            'wind': {'speed': 6.5},
        },
    ],
}


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def raw_forecast_file():
    """Write RAW_FORECAST to /tmp/forecast_raw.json; clean up after."""
    os.makedirs('/tmp', exist_ok=True)
    with open('/tmp/forecast_raw.json', 'w') as f:
        json.dump(RAW_FORECAST, f)
    yield
    for path in ['/tmp/forecast_raw.json', '/tmp/forecast_transformed.json']:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


@pytest.fixture
def transformed_forecast_file():
    """Write NDJSON forecast records to /tmp/forecast_transformed.json."""
    os.makedirs('/tmp', exist_ok=True)
    records = [
        {
            'city': 'Brooklyn',
            'forecast_time': '2026-02-19 06:00:00',
            'temperature': 38.5,
            'feels_like': 32.1,
            'humidity': 88,
            'description': 'light snow',
            'wind_speed': 8.2,
            'timestamp': '2026-02-19T06:00:00',
        }
    ]
    with open('/tmp/forecast_transformed.json', 'w') as f:
        f.write('\n'.join(json.dumps(r) for r in records))
    yield
    try:
        os.remove('/tmp/forecast_transformed.json')
    except FileNotFoundError:
        pass


# ── Transform tests ───────────────────────────────────────────────────────────

class TestTransformForecast:
    """Tests for transform_forecast()."""

    def test_returns_count_of_periods(self, raw_forecast_file):
        count = transform_forecast()
        assert count == 2

    def test_output_written_as_ndjson(self, raw_forecast_file):
        transform_forecast()

        with open('/tmp/forecast_transformed.json') as f:
            lines = [line for line in f.read().strip().split('\n') if line]
        assert len(lines) == 2
        for line in lines:
            record = json.loads(line)
            assert 'city' in record
            assert 'forecast_time' in record
            assert 'temperature' in record

    def test_extracts_all_required_fields(self, raw_forecast_file):
        transform_forecast()

        with open('/tmp/forecast_transformed.json') as f:
            record = json.loads(f.readline())

        assert record['city'] == 'Brooklyn'
        assert record['forecast_time'] == '2026-02-19 06:00:00'
        assert record['temperature'] == 38.5
        assert record['feels_like'] == 32.1
        assert record['humidity'] == 88
        assert record['description'] == 'light snow'
        assert record['wind_speed'] == 8.2
        assert 'timestamp' in record

    def test_raises_file_not_found_without_raw_data(self):
        try:
            os.remove('/tmp/forecast_raw.json')
        except FileNotFoundError:
            pass

        with pytest.raises(FileNotFoundError):
            transform_forecast()


# ── Load tests ────────────────────────────────────────────────────────────────

class TestLoadForecastToS3:
    """Tests for load_to_s3() in the forecast pipeline."""

    def test_uploads_to_forecast_prefix(self, s3_client, transformed_forecast_file):
        result = load_to_s3()

        assert result.startswith('s3://test-bucket/forecast/')

    def test_object_exists_in_s3_after_upload(self, s3_client, transformed_forecast_file):
        load_to_s3()

        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='forecast/')
        assert objects['KeyCount'] == 1

    def test_uploaded_content_contains_forecast_records(self, s3_client, transformed_forecast_file):
        load_to_s3()

        objects = s3_client.list_objects_v2(Bucket='test-bucket', Prefix='forecast/')
        key = objects['Contents'][0]['Key']
        body = s3_client.get_object(Bucket='test-bucket', Key=key)['Body'].read().decode()
        record = json.loads(body.strip().split('\n')[0])

        assert record['city'] == 'Brooklyn'
        assert 'forecast_time' in record
        assert 'temperature' in record
