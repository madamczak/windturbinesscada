import json
import pytest
from playwright.sync_api import sync_playwright


@pytest.mark.slow
def test_sse_receives_three_messages(base_url):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(f"{base_url}/testpage?stream=test")

        page.wait_for_function('() => window._sse_msgs && window._sse_msgs.length >= 1', timeout=10000)
        message_list = page.evaluate('() => window._sse_msgs')

        assert isinstance(message_list, list)
        assert len(message_list) >= 1
        objs = [json.loads(m) if isinstance(m, str) else m for m in message_list]
        assert objs[0]['rowid'] == 1
        assert objs[0]['record']['value'] == 'msg1'
        browser.close()
