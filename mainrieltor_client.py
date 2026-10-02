"""Bounded synchronous Mainrieltor exchange; errors never contain secrets."""
import os
from urllib.parse import urlsplit

import requests
from interior_sync import validate


class ExchangeError(RuntimeError):
    pass


class Client:
    def __init__(self, url, key, session=None, max_pages=1000):
        parsed = urlsplit(url)
        if parsed.scheme not in {'http', 'https'} or not parsed.netloc or parsed.username or parsed.query or parsed.fragment or not key:
            raise ExchangeError('invalid exchange configuration')
        self.url, self.key = url.rstrip('/'), key
        self.session = session or requests.Session()
        self.max_pages = max_pages

    def request(self, method, path, **kwargs):
        try:
            response = self.session.request(method, self.url + path,
                headers={'X-API-Key': self.key}, timeout=(10, 60), allow_redirects=False, **kwargs)
            if response.status_code != 200:
                raise ExchangeError('exchange HTTP ' + str(response.status_code))
            return response.json()
        except ExchangeError:
            raise
        except Exception:
            raise ExchangeError('exchange network or JSON error') from None

    def fetch(self):
        after, items, seen = 0, [], set()
        for _ in range(self.max_pages):
            page = self.request('GET', '/interior/evaluations', params={'after_id': after, 'limit': 200})
            try:
                rows = page['results']
                if not isinstance(rows, list) or len(rows) > 200 or type(page['count']) is not int or page['count'] != len(rows) or type(page['has_more']) is not bool:
                    raise ValueError()
                for item in rows:
                    validate(item)
                    number = item['evaluation_id']
                    if number <= after or number in seen:
                        raise ValueError()
                    seen.add(number)
                next_id = page['next_after_id']
                if type(next_id) is not int or next_id != (max(r['evaluation_id'] for r in rows) if rows else after):
                    raise ValueError()
                if page['has_more'] and next_id <= after:
                    raise ValueError()
            except (ValueError, KeyError, TypeError):
                raise ExchangeError('invalid exchange page') from None
            items.extend(rows)
            if not page['has_more']:
                return items
            after = next_id
        raise ExchangeError('exchange page limit exceeded')

    def ack(self, ids, version):
        response = self.request('POST', '/interior/evaluations/ack', json={'evaluation_ids': ids, 'baseline_version': version})
        keys = ('requested', 'acknowledged', 'already_acknowledged', 'missing', 'not_ready')
        if not isinstance(response, dict) or any(type(response.get(k)) is not int or response[k] < 0 for k in keys):
            raise ExchangeError('invalid ACK response')
        if response['requested'] != len(ids) or response['missing'] or response['not_ready'] or response['acknowledged'] + response['already_acknowledged'] != len(ids):
            raise ExchangeError('incomplete ACK')


def configured():
    if os.environ.get('MAINRIELTOR_INTERIOR_SYNC_ENABLED', '0').lower() not in {'1', 'true', 'yes', 'on'}:
        return None
    return Client(os.environ.get('MAINRIELTOR_URL', ''), os.environ.get('MAINRIELTOR_INTERIOR_API_KEY', ''))
