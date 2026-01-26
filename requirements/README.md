Purpose

This directory contains human-readable requirements and behavior specifications for the project's HTTP/SSE API. Use these files to:

- Describe API behavior in concrete Given/When/Then scenarios (Gherkin-style).
- Capture expected inputs, outputs, side-effects, and error cases.
- Drive test creation (unit, integration, and e2e) and manual QA.

How to use

- Add a new `.feature` file for each endpoint or behavior group (e.g. `sse_api.feature`).
- Keep each scenario small and focused (one main behavior per scenario).
- Reference these scenarios when writing tests (Playwright, pytest, or component tests).

Conventions

- "Given" sets initial state (server running, DB seeded, client connected, auth present).
- "When" performs an action (client opens EventSource, server sends a record, DB updated).
- "Then" asserts outcome (client receives event with JSON shape, server returns status, client reconnects).

Example mapping

- Scenario in `sse_api.feature` -> Playwright e2e test in `tests/` that exercises the browser EventSource.
- Edge-case scenario -> pytest async test that exercises the SSE generator directly.

Notes for implementers

- Keep scenarios implementation-agnostic: describe observable behavior and constraints, not implementation details.
- Include timeouts where behavior depends on timing (e.g. reconnection backoff, message delivery within X seconds).
- Mark scenarios that require external resources (DBs, large files) so CI can skip them or run them with special tags.

Contact

If unclear, open an issue or create a PR with proposed clarification. This file aims to make requirements discoverable to both developers and automated tests.
