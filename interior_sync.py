"""Durable compact evaluations and post-publication delivery acknowledgement."""
import math
import re
from collections import Counter

import cleaner_db
import collector_client
import interior_baseline as baseline
import renovation_scorer


def validate(item, delivery=True):
    if not isinstance(item, dict):
        raise ValueError('invalid evaluation')
    if delivery and (type(item.get('evaluation_id')) is not int or item['evaluation_id'] <= 0):
        raise ValueError('invalid evaluation ID')
    for key in ('listing_id', 'evaluator_version'):
        if not isinstance(item.get(key), str) or not item[key].strip():
            raise ValueError('missing evaluation identity')
    photo_hash = item.get('photo_set_hash')
    if photo_hash is not None and (not isinstance(photo_hash, str) or not re.fullmatch('[0-9a-f]{64}', photo_hash)):
        raise ValueError('invalid photo hash')
    status, score, coverage = (item.get(k) for k in ('status', 'score', 'coverage'))
    if status not in baseline.TERMINAL_STATUSES:
        raise ValueError('invalid status')
    if status in {'no_photos', 'photos_unavailable'}:
        if score is not None or coverage is not None:
            raise ValueError('unavailable photos require null score and coverage')
    elif coverage == 'insufficient':
        if score is not None:
            raise ValueError('insufficient requires null score')
    elif coverage in {'partial', 'sufficient'}:
        if type(score) not in (float, int) or not math.isfinite(score) or not 1 <= score <= 10 or abs(score * 10 - round(score * 10)) > 1e-8:
            raise ValueError('invalid score')
    else:
        raise ValueError('invalid coverage')
    for key in ('evaluation_key', 'content_set_key', 'evaluated_at'):
        if item.get(key) is not None and not isinstance(item[key], str):
            raise ValueError('invalid evaluation metadata')
    return item


def from_baseline(row):
    return dict(listing_id=str(row['id']), photo_set_hash=row.get('photo_set_hash'),
                **{key: row.get('interior_' + key) for key in (
                    'status', 'score', 'coverage', 'evaluator_version',
                    'evaluation_key', 'content_set_key', 'evaluated_at')})


def save(conn, items):
    items = [validate(item, delivery=False) for item in items]
    for item in items:
        item = {k: item.get(k) for k in ('listing_id', 'photo_set_hash', 'evaluator_version',
                'status', 'score', 'coverage', 'evaluation_key', 'content_set_key', 'evaluated_at')}
        conn.execute('''INSERT INTO interior_evaluations
            (listing_id, photo_hash_key, evaluator_version, payload_json, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(listing_id, photo_hash_key, evaluator_version) DO NOTHING''',
            (item['listing_id'], item.get('photo_set_hash') or '', item['evaluator_version'],
             cleaner_db.json.dumps(item, ensure_ascii=False), cleaner_db.utcnow_iso()))


def migrate(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS interior_evaluations (
        listing_id TEXT NOT NULL, photo_hash_key TEXT NOT NULL,
        evaluator_version TEXT NOT NULL, payload_json TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (listing_id, photo_hash_key, evaluator_version))''')
    if cleaner_db.get_meta(conn, 'interior_migrated_v1')[0]:
        return
    for row in cleaner_db.iter_clean_baseline(conn):
        if row.get('interior_status') in baseline.TERMINAL_STATUSES:
            item = from_baseline(row)
            if not item['evaluator_version'] and item['status'] in {'no_photos', 'photos_unavailable'}:
                item['evaluator_version'] = renovation_scorer.EVALUATOR_VERSION
            save(conn, [item])
    cleaner_db.set_meta(conn, 'interior_migrated_v1', '1')


def enrich(conn, rows):
    migrate(conn)
    result = []
    for source in rows:
        row = dict(source)
        try:
            key = baseline.photo_set_hash(row.get('photo_urls'), row.get('photo_set_hash'))
        except (TypeError, ValueError):
            row.update(baseline.empty_interior(None))
            result.append(row)
            continue
        row.update(baseline.empty_interior(key))
        stored = conn.execute('''SELECT payload_json FROM interior_evaluations
            WHERE listing_id=? AND photo_hash_key=? AND evaluator_version=?''',
            (str(row['id']), key or '', renovation_scorer.EVALUATOR_VERSION)).fetchone()
        if stored:
            item = cleaner_db.json.loads(stored[0])
            row.update({'interior_' + k: item.get(k) for k in (
                'status', 'score', 'coverage', 'evaluator_version',
                'evaluation_key', 'content_set_key', 'evaluated_at')})
        result.append(row)
    return result


def acknowledge_published(conn, client, items, rows, published):
    if not items or published.get('publish_skipped') or not published.get('published'):
        return {'acknowledged': 0}
    if conn.in_transaction:
        raise RuntimeError('ACK requires committed publication')
    current = {str(row['id']): row for row in rows}
    verdicts = cleaner_db.verdict_state_map(conn)
    visible = {str(row['id']): row for row in cleaner_db.iter_clean_baseline(conn)}
    ids, reasons = [], Counter()
    for item in items:
        row = current.get(item['listing_id'])
        reason = None
        if row is None:
            reason = 'absent_but_retained'
        else:
            try:
                key = baseline.photo_set_hash(row.get('photo_urls'), row.get('photo_set_hash'))
            except (ValueError, TypeError):
                continue
            if key != item.get('photo_set_hash') or item['evaluator_version'] != renovation_scorer.EVALUATOR_VERSION:
                reason = 'different_key_retained'
            else:
                state = verdicts.get(row['id'])
                if state and state[1] == collector_client.content_hash(row) and state[0] == 0:
                    reason = 'filtered_out'
                elif item['listing_id'] in visible:
                    stored = visible[item['listing_id']]
                    if stored.get('photo_set_hash') == key and stored.get('interior_evaluator_version') == item['evaluator_version'] and stored.get('interior_status') in baseline.TERMINAL_STATUSES:
                        reason = 'published'
        if reason:
            ids.append(item['evaluation_id'])
            reasons[reason] += 1
    info = cleaner_db.clean_baseline_info(conn)
    for offset in range(0, len(ids), 500):
        client.ack(ids[offset:offset + 500], str(info[1]) + ':' + info[2])
    return {'acknowledged': len(ids), 'dispositions': dict(reasons)}
