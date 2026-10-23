import sys, os
if sys.platform == 'win32':
    import concurrent.futures.thread, concurrent.futures.process
    os.register_at_fork = getattr(os, 'register_at_fork', lambda **k: None)

sys.path.insert(0, os.path.abspath('include'))
sys.path.insert(0, os.path.abspath('dags'))
import nammamart_etl_ajay as etl
print("ETL Tasks:", list(etl.nammamart_etl_dag.task_dict.keys()))
