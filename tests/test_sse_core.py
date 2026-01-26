import json
import time
import requests
import pytest
from playwright.sync_api import sync_playwright


@pytest.mark.slow
def test_end_of_stream(base_url):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(f"{base_url}/testpage?stream=end")

        # wait for _sse_ended flag
        page.wait_for_function('() => window._sse_ended === true', timeout=5000)
        ended = page.evaluate('() => window._sse_ended')
        assert ended is True
        browser.close()


@pytest.mark.slow
def test_reconnect_behavior(base_url):
    # The /sse/reconnect endpoint sends one message then closes; subsequent connections should resume
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(f"{base_url}/testpage?stream=reconnect")
        page.wait_for_function('() => window._sse_msgs && window._sse_msgs.length >= 1', timeout=5000)
        msgs1 = page.evaluate('() => window._sse_msgs')
        assert len(msgs1) == 1
        browser.close()

    # reconnect: open again and expect next message
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(f"{base_url}/testpage?stream=reconnect")
        page.wait_for_function('() => window._sse_msgs && window._sse_msgs.length >= 1', timeout=5000)
        msgs2 = page.evaluate('() => window._sse_msgs')
        assert len(msgs2) == 1
        # ensure rowid progressed
        m1 = json.loads(msgs1[0]) if isinstance(msgs1[0], str) else msgs1[0]
        m2 = json.loads(msgs2[0]) if isinstance(msgs2[0], str) else msgs2[0]
        assert m2['rowid'] == m1['rowid'] + 1
        browser.close()


@pytest.mark.slow
def test_high_throughput_does_not_block(base_url):
    # This is a heuristic: open two clients, one that reads slowly and one that reads normally.
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page_fast = browser.new_page()
        page_slow = browser.new_page()
        page_fast.goto(f"{base_url}/testpage?stream=high")
        page_slow.goto(f"{base_url}/testpage?stream=high")

        # fast client should receive many messages quickly
        page_fast.wait_for_function('() => window._sse_msgs && window._sse_msgs.length >= 5', timeout=3000)
        fast_count = page_fast.evaluate('() => window._sse_msgs.length')

        # slow client simulate by sleeping before checking
        time.sleep(1.0)
        slow_count = page_slow.evaluate('() => window._sse_msgs.length')

        assert fast_count >= 5
        # slow client might get fewer messages, but server should not be blocked (we assert fast client got messages)
        browser.close()


@pytest.mark.slow
def test_malformed_payload_handling(base_url):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(f"{base_url}/testpage?stream=malformed")
        page.wait_for_function('() => window._sse_msgs && window._sse_msgs.length >= 2', timeout=5000)
        msgs = page.evaluate('() => window._sse_msgs')
        parse_errors = page.evaluate('() => window._sse_parse_errors')
        assert len(msgs) >= 2
        # first was malformed (string), second parsed object
        assert isinstance(msgs[0], str)
        assert len(parse_errors) >= 1
        browser.close()


def test_auth_protection(base_url):
    # unauthenticated request should get 401
    r = requests.get(f"{base_url}/sse/auth", stream=True)
    assert r.status_code == 401

    # authenticated via query param
    r2 = requests.get(f"{base_url}/sse/auth?token=secret-token", stream=True)
    assert r2.status_code == 200

    # authenticated via header
    r3 = requests.get(f"{base_url}/sse/auth", headers={"Authorization": "Bearer secret-token"}, stream=True)
    assert r3.status_code == 200


@pytest.mark.slow
def test_resume_and_retention(base_url):
    # request resume with acceptable last_rowid
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(f"{base_url}/testpage?stream=resume&last_rowid=5")
        page.wait_for_function('() => window._sse_msgs && window._sse_msgs.length >= 1', timeout=5000)
        msgs = page.evaluate('() => window._sse_msgs')
        m = msgs[0]
        if isinstance(m, str):
            m = json.loads(m)
        assert m['rowid'] > 5
        browser.close()

    # request resume with too-old last_rowid to trigger retention
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(f"{base_url}/testpage?stream=resume&last_rowid=1")
        # wait for retention info
        page.wait_for_function('() => window._sse_retention !== undefined', timeout=5000)
        retention = page.evaluate('() => window._sse_retention')
        assert retention['error'] == 'retention_expired'
        browser.close()

