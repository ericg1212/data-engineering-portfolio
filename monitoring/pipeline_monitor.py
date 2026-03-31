import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402
from datetime import datetime, timedelta  # noqa: E402
import io  # noqa: E402
import logging  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from utils import _s3_client, get_date_str, log_failure  # noqa: E402

logger = logging.getLogger(__name__)


default_args = {
    'owner': 'eric',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 2),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': log_failure,
}

dag = DAG(
    'pipeline_monitor',
    default_args=default_args,
    description='Monitor health of 4 active pipelines',
    schedule_interval='0 18 * * *',
    catchup=False,
)


def _read_parquet_from_s3(s3, bucket, key):
    """Download a Parquet file from S3 and return as list of dicts."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    table = pq.read_table(io.BytesIO(obj['Body'].read()))
    return table.to_pylist()


def _is_trading_day(dt=None):
    """Return True if the given date falls on a weekday (Mon–Fri)."""
    if dt is None:
        dt = datetime.now()
    return dt.weekday() < 5


def check_stock_pipeline():
    """Check if stock data was loaded today (skips weekends)."""
    if not _is_trading_day():
        day_name = datetime.now().strftime('%A')
        logger.info(f"Stock pipeline: {day_name} is not a trading day — skipping check")
        return {'status': 'OK', 'message': f'{day_name} — non-trading day'}

    try:
        s3, bucket = _s3_client()
        today = get_date_str()
        prefix = f"stocks/date={today}/"

        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if 'Contents' not in response or len(response['Contents']) == 0:
            logger.warning(f"No stock data found for {today}")
            return {'status': 'WARNING', 'message': f'No data for {today}'}

        latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'])[-1]
        data = _read_parquet_from_s3(s3, bucket, latest_file['Key'])

        if len(data) == 10:
            symbols = [s['symbol'] for s in data]
            prices = {s['symbol']: s['price'] for s in data}
            logger.info(f"Stock pipeline healthy: {symbols}")
            return {
                'status': 'OK',
                'stocks': symbols,
                'prices': prices,
                'last_update': str(latest_file['LastModified']),
            }
        else:
            logger.error(f"Expected 10 stocks, got {len(data)}")
            return {'status': 'ERROR', 'message': f'Expected 10 stocks, got {len(data)}'}

    except Exception as e:
        logger.error(f"Stock pipeline check failed: {str(e)}")
        return {'status': 'ERROR', 'message': str(e)}


def check_edgar_pipeline():
    """Check if EDGAR fundamentals data was loaded within the last 120 days (quarterly cadence)."""
    try:
        s3, bucket = _s3_client()
        response = s3.list_objects_v2(Bucket=bucket, Prefix='fundamentals/')

        if 'Contents' not in response or len(response['Contents']) == 0:
            logger.warning("No EDGAR fundamentals data found")
            return {'status': 'WARNING', 'message': 'No fundamentals data found'}

        latest = max(response['Contents'], key=lambda x: x['LastModified'])
        days_old = (datetime.now(latest['LastModified'].tzinfo) - latest['LastModified']).days

        if days_old > 120:
            logger.warning(f"EDGAR data is {days_old} days old — expected <= 120")
            return {'status': 'WARNING', 'message': f'Data is {days_old} days old (expected <=120)'}

        partitions = len(response['Contents'])
        logger.info(f"EDGAR pipeline healthy: {partitions} partition files, latest {days_old}d ago")
        return {
            'status': 'OK',
            'partitions': partitions,
            'days_since_update': days_old,
            'last_update': str(latest['LastModified']),
        }

    except Exception as e:
        logger.error(f"EDGAR pipeline check failed: {str(e)}")
        return {'status': 'ERROR', 'message': str(e)}


def check_fred_pipeline():
    """Check if FRED macro data was loaded within the last 35 days (monthly + 2-week lag buffer)."""
    try:
        s3, bucket = _s3_client()
        response = s3.list_objects_v2(Bucket=bucket, Prefix='macro_indicators/')

        if 'Contents' not in response or len(response['Contents']) == 0:
            logger.warning("No FRED macro data found")
            return {'status': 'WARNING', 'message': 'No macro_indicators data found'}

        latest = max(response['Contents'], key=lambda x: x['LastModified'])
        days_old = (datetime.now(latest['LastModified'].tzinfo) - latest['LastModified']).days

        if days_old > 35:
            logger.warning(f"FRED data is {days_old} days old — expected <= 35")
            return {'status': 'WARNING', 'message': f'Data is {days_old} days old (expected <=35)'}

        partitions = len(response['Contents'])
        logger.info(f"FRED pipeline healthy: {partitions} partition files, latest {days_old}d ago")
        return {
            'status': 'OK',
            'partitions': partitions,
            'days_since_update': days_old,
            'last_update': str(latest['LastModified']),
        }

    except Exception as e:
        logger.error(f"FRED pipeline check failed: {str(e)}")
        return {'status': 'ERROR', 'message': str(e)}


def generate_health_report(**context):
    """Generate overall health report for the 4 active pipelines."""
    ti = context['ti']

    stock_status = ti.xcom_pull(task_ids='check_stock_pipeline')
    edgar_status = ti.xcom_pull(task_ids='check_edgar_pipeline')
    fred_status = ti.xcom_pull(task_ids='check_fred_pipeline')

    logger.info("=" * 50)
    logger.info("PIPELINE HEALTH REPORT")
    logger.info("=" * 50)

    for name, status in [('Stock', stock_status), ('EDGAR', edgar_status), ('FRED', fred_status)]:
        logger.info(f"{name} Pipeline: {status.get('status', 'UNKNOWN')}")
        if status.get('status') != 'OK':
            logger.info(f"  |- Issue: {status.get('message')}")
        elif name == 'Stock':
            logger.info(f"  |- Prices: {status.get('prices')}")
        else:
            logger.info(f"  |- Partitions: {status.get('partitions')} | "
                        f"Days since update: {status.get('days_since_update')}")

    logger.info("=" * 50)

    all_ok = all(s.get('status') == 'OK' for s in [stock_status, edgar_status, fred_status])
    if all_ok:
        logger.info("All pipelines healthy!")
        return 'HEALTHY'
    else:
        logger.warning("Some pipelines need attention")
        return 'NEEDS_ATTENTION'


check_stock_task = PythonOperator(
    task_id='check_stock_pipeline',
    python_callable=check_stock_pipeline,
    dag=dag,
)

check_edgar_task = PythonOperator(
    task_id='check_edgar_pipeline',
    python_callable=check_edgar_pipeline,
    dag=dag,
)

check_fred_task = PythonOperator(
    task_id='check_fred_pipeline',
    python_callable=check_fred_pipeline,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_health_report',
    python_callable=generate_health_report,
    provide_context=True,
    dag=dag,
)

[check_stock_task, check_edgar_task, check_fred_task] >> report_task
