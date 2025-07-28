from etl.spark_job import run_etl

def test_etl_result():
    df = run_etl()
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]['name'] == 'Alice'
