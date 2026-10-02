import copy
import json
import os
import sqlite3
import unittest
from unittest.mock import Mock, patch

import cleaner_db
import collector_client
import interior_sync as sync
import mainrieltor_client as http
import pipeline
import renovation_scorer


def evaluation(number=1, **changes):
    return dict(dict(evaluation_id=number, listing_id=str(number), photo_set_hash=None,
        evaluator_version=renovation_scorer.EVALUATOR_VERSION, status='no_photos',
        score=None, coverage=None, evaluation_key=None, content_set_key=None,
        evaluated_at='2026-10-03T00:00:00+00:00'), **changes)


class ClientTests(unittest.TestCase):
    def test_pages_over_500_restart_at_zero_and_ack(self):
        session = Mock()
        def request(method, url, **kw):
            self.assertFalse(kw['allow_redirects'])
            if method == 'POST':
                n = len(kw['json']['evaluation_ids'])
                body = dict(requested=n, acknowledged=n, already_acknowledged=0, missing=0, not_ready=0)
            else:
                start = kw['params']['after_id']
                end = min(start + 200, 601)
                body = dict(results=[evaluation(i) for i in range(start+1, end+1)],
                            count=end-start, has_more=end < 601, next_after_id=end)
            return Mock(status_code=200, json=lambda: body)
        session.request.side_effect = request
        client = http.Client('http://example.test', 'SECRET', session)
        self.assertEqual(len(client.fetch()), 601)
        self.assertEqual(len(client.fetch()), 601)
        starts = [c.kwargs['params']['after_id'] for c in session.request.call_args_list]
        self.assertEqual(starts, [0, 200, 400, 600] * 2)
        client.ack([1, 2], 'build')

    def test_errors_and_bad_pages_do_not_ack_or_leak_secret(self):
        bad_pages = [None, {}, dict(results=[], count=0, has_more=True, next_after_id=0),
            dict(results=[evaluation(), evaluation()], count=2, has_more=False, next_after_id=1),
            dict(results=[evaluation(score=5)], count=1, has_more=False, next_after_id=1)]
        responses = [Mock(status_code=s) for s in (401, 403, 500)]
        responses += [Mock(status_code=200, json=Mock(return_value=p)) for p in bad_pages]
        responses += [Mock(status_code=200, json=Mock(side_effect=ValueError('SECRET')))]
        for response in responses:
            session = Mock()
            session.request.return_value = response
            with self.subTest(response=response):
                with self.assertRaises(http.ExchangeError) as exc:
                    http.Client('http://example.test', 'SECRET', session).fetch()
                self.assertNotIn('SECRET', str(exc.exception))
                self.assertTrue(all(c.args[0] == 'GET' for c in session.request.call_args_list))
        session.request.side_effect = TimeoutError('SECRET')
        with self.assertRaisesRegex(http.ExchangeError, 'network'):
            http.Client('http://example.test', 'SECRET', session).fetch()


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        cleaner_db._create_schema(self.conn)
        self.rows = [dict(id='1', photo_urls=[], price=100)]

    def tearDown(self):
        self.conn.close()

    def test_migration_replay_disappearance_price_and_versions(self):
        item = evaluation()
        fields = {'interior_' + k: item[k] for k in (
            'status', 'score', 'coverage', 'evaluator_version', 'evaluation_key',
            'content_set_key', 'evaluated_at')}
        cleaner_db.replace_clean_baseline(self.conn, [dict(self.rows[0], **fields)], 'v1')
        sync.migrate(self.conn)
        sync.save(self.conn, [evaluation(secret='SECRET'), item])
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM interior_evaluations').fetchone()[0], 1)
        self.assertNotIn('SECRET', self.conn.execute('SELECT payload_json FROM interior_evaluations').fetchone()[0])
        cleaner_db.replace_clean_baseline(self.conn, [dict(id='other')], 'v2')
        restored = sync.enrich(self.conn, [dict(self.rows[0], price=90)])[0]
        self.assertEqual(restored['interior_status'], 'no_photos')
        self.assertIsNone(restored['interior_score'])
        with patch.object(renovation_scorer, 'EVALUATOR_VERSION', 'new'):
            self.assertIsNone(sync.enrich(self.conn, self.rows)[0]['interior_status'])
        self.assertIsNone(sync.enrich(self.conn, [dict(self.rows[0], photo_urls=['url'])])[0]['interior_status'])

    def test_validation_nulls_and_decimals(self):
        for status, coverage in [('ready', 'insufficient'), ('ready_with_download_gaps', 'insufficient'),
                                 ('no_photos', None), ('photos_unavailable', None)]:
            self.assertIsNone(sync.validate(evaluation(status=status, coverage=coverage))['score'])
        for score in [True, float('nan'), float('inf'), 0, 10.1, 6.55, None]:
            with self.assertRaises(ValueError):
                sync.validate(evaluation(status='ready', coverage='partial', score=score))

    def test_ack_selection_and_newer_hash_retained(self):
        items = [evaluation(i) for i in range(1, 6)]
        items[4]['photo_set_hash'] = 'a' * 64
        sync.migrate(self.conn)
        sync.save(self.conn, items)
        rows = [dict(id=str(i), photo_urls=[]) for i in (1, 2, 3, 5)]
        cleaner_db.upsert_verdict(self.conn, '3', collector_client.content_hash(rows[2]),
                                 dict(usable=False), 'v')
        cleaner_db.replace_clean_baseline(self.conn, sync.enrich(self.conn, rows[:1]), 'v')
        self.conn.commit()
        client = Mock()
        result = sync.acknowledge_published(self.conn, client, items, rows, dict(published=1))
        self.assertEqual(client.ack.call_args.args[0], [1, 3, 4, 5])
        self.assertEqual(result['acknowledged'], 4)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM interior_evaluations').fetchone()[0], 5)
        client.reset_mock()
        for skip in ('empty_result', 'relabel_in_progress'):
            sync.acknowledge_published(self.conn, client, items, rows, dict(publish_skipped=skip))
        client.ack.assert_not_called()

    def run_cycle(self, client):
        from contextlib import ExitStack, contextmanager
        @contextmanager
        def connection():
            yield self.conn
        with ExitStack() as stack:
            stack.enter_context(patch.object(cleaner_db, 'connect', connection))
            stack.enter_context(patch.object(collector_client, 'fetch_all_rows', return_value=('v', self.rows)))
            stack.enter_context(patch.object(http, 'configured', return_value=client))
            stack.enter_context(patch.object(pipeline, 'ingest_completed_batches', return_value=0))
            stack.enter_context(patch.object(pipeline, 'apply_rules_to_known', return_value=(0, 0)))
            stack.enter_context(patch.object(pipeline, 'apply_heuristics', return_value=[]))
            stack.enter_context(patch.object(pipeline, 'submit_new_batches', return_value=([], 0)))
            stack.enter_context(patch.dict(os.environ, {'CLEANER_RELABEL_GEN': ''}))
            return pipeline.run_cycle()

    def test_pipeline_ack_failure_replay_and_publication_failure(self):
        cleaner_db.upsert_verdict(self.conn, '1', collector_client.content_hash(self.rows[0]), dict(usable=True), 'v')
        self.conn.commit()
        client = Mock()
        client.fetch.return_value = [evaluation()]
        client.ack.side_effect = http.ExchangeError('test failure')
        result = self.run_cycle(client)
        self.assertIn('error', result['interior_sync'])
        self.assertEqual(next(cleaner_db.iter_clean_baseline(self.conn))['interior_status'], 'no_photos')
        client.ack.side_effect = None
        self.assertEqual(self.run_cycle(client)['interior_sync']['acknowledged'], 1)
        client.reset_mock()
        with patch.object(cleaner_db, 'replace_clean_baseline', side_effect=RuntimeError('write failed')):
            with self.assertRaises(RuntimeError):
                self.run_cycle(client)
        client.ack.assert_not_called()

    def test_failed_commit_no_ack(self):
        cleaner_db.upsert_verdict(self.conn, '1', collector_client.content_hash(self.rows[0]), dict(usable=True), 'v')
        self.conn.commit()
        real_conn = self.conn
        class FailCommit:
            def __getattr__(self, name):
                return getattr(real_conn, name)
            def commit(self):
                raise sqlite3.OperationalError('commit failed')
        self.conn = FailCommit()
        client = Mock()
        client.fetch.return_value = [evaluation()]
        try:
            with self.assertRaises(sqlite3.OperationalError):
                self.run_cycle(client)
            client.ack.assert_not_called()
        finally:
            self.conn = real_conn
            self.conn.rollback()

    def test_waiting_and_sync_failure_no_ack(self):
        client = Mock()
        client.fetch.return_value = [evaluation()]
        self.assertEqual(self.run_cycle(client)['publish_skipped'], 'empty_result')
        client.ack.assert_not_called()
        cleaner_db.upsert_verdict(self.conn, '1', collector_client.content_hash(self.rows[0]), dict(usable=True), 'v')
        self.conn.commit()
        client.fetch.side_effect = http.ExchangeError('network failure')
        result = self.run_cycle(client)
        self.assertEqual(result['published'], 1)
        client.ack.assert_not_called()


if __name__ == '__main__':
    unittest.main()
