# tests/test_nammamart_pipelines.py
# NammaMart Pipeline Unit & Integrity Tests

import os
import sys
import unittest

# Windows patch for airflow on Python 3.13
if sys.platform == 'win32':
    import concurrent.futures.thread, concurrent.futures.process
    os.register_at_fork = getattr(os, 'register_at_fork', lambda **k: None)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'include')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dags')))

import nammamart_helpers as helpers


class TestNammaMartPipelines(unittest.TestCase):

    def test_helper_email_masking(self):
        self.assertEqual(helpers.mask_email("sanjay.kumar@gmail.com"), "sa***@gmail.com")
        self.assertEqual(helpers.mask_email("a@b.com"), "a***@b.com")
        self.assertEqual(helpers.mask_email(None), "")
        self.assertEqual(helpers.mask_email("invalid-email"), "***@masked.invalid")

    def test_helper_phone_masking(self):
        self.assertEqual(helpers.mask_phone("9876543210"), "98******10")
        self.assertEqual(helpers.mask_phone("+91-9876543210"), "91******10")
        self.assertEqual(helpers.mask_phone("123"), "******")
        self.assertEqual(helpers.mask_phone(None), "")

    def test_helper_standardization(self):
        self.assertEqual(helpers.standardize_loyalty_tier("gold"), "Gold")
        self.assertEqual(helpers.standardize_loyalty_tier("PLATINUM"), "Platinum")
        self.assertEqual(helpers.standardize_loyalty_tier("unknown_tier"), "Bronze")

        self.assertEqual(helpers.standardize_payment_mode("upi"), "UPI")
        self.assertEqual(helpers.standardize_payment_mode("cod"), "Cash on Delivery")
        self.assertEqual(helpers.standardize_payment_mode("card"), "Card")

    def test_etl_dag_structure(self):
        import nammamart_etl_ajay as etl
        dag = etl.nammamart_etl_dag
        self.assertEqual(dag.dag_id, 'nammamart_etl_ajay')
        self.assertGreaterEqual(dag.default_args.get('retries'), 2)
        self.assertIn('fde', dag.tags)
        self.assertIn('nammamart', dag.tags)
        self.assertIn('ajay', dag.tags)
        self.assertTrue('0 9 * * 1-6' in str(dag.timetable))

        expected_tasks = {
            'extract_orders', 'extract_payments', 'extract_customers',
            'extract_products', 'extract_stores', 'validate_and_quarantine',
            'transform', 'load_star_schema', 'reconcile_payments',
            'generate_reports', 'audit_pipeline_run'
        }
        self.assertTrue(expected_tasks.issubset(set(dag.task_dict.keys())))

    def test_elt_dag_structure(self):
        import nammamart_elt_ajay as elt
        dag = elt.nammamart_elt_dag
        self.assertEqual(dag.dag_id, 'nammamart_elt_ajay')
        self.assertGreaterEqual(dag.default_args.get('retries'), 2)
        self.assertIn('fde', dag.tags)
        self.assertIn('nammamart', dag.tags)
        self.assertIn('ajay', dag.tags)
        self.assertTrue('30 9 * * 1-6' in str(dag.timetable))

        expected_tasks = {
            'raw_ingestion', 'staging_transformations', 'incremental_merge',
            'build_marts', 'verify_revenue_consistency', 'audit_pipeline_run'
        }
        self.assertTrue(expected_tasks.issubset(set(dag.task_dict.keys())))

    def test_duckdb_marts_and_governance(self):
        con = helpers.get_db_connection()
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]

        self.assertIn('stg_orders', tables)
        self.assertIn('mart_daily_zone_revenue', tables)
        self.assertIn('mart_category_margin', tables)
        self.assertIn('mart_delivery_sla', tables)
        self.assertIn('mart_reconciliation_summary', tables)
        self.assertIn('mart_top_customers', tables)
        self.assertIn('dq_results', tables)
        self.assertIn('pipeline_runs', tables)

        # Check non-negative revenue
        rev = con.execute("SELECT MIN(daily_net_revenue) FROM mart_daily_zone_revenue").fetchone()[0]
        self.assertGreaterEqual(rev, 0.0)

        # Check SLA percentage sum
        sla_check = con.execute("""
            SELECT ROUND(pct_under_15_min + pct_15_to_20_min + pct_over_20_min, 1)
            FROM mart_delivery_sla
            LIMIT 1
        """).fetchone()[0]
        self.assertLessEqual(abs(sla_check - 100.0), 0.2)

        con.close()


if __name__ == '__main__':
    unittest.main()

