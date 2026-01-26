Feature: Server-Sent Events API for next-record

  Background:
    Given the server is running and serving the SSE endpoint at /sse/next-record

  Scenario: Client successfully receives the next record as SSE
    Given a client has established an EventSource connection to /sse/next-record
    And the server has at least one new record ready to send
    When the server emits the next-record event
    Then the client receives an SSE message with JSON containing keys: rowid, table, record
    And the JSON field record includes the fields expected for that table

  Scenario: Client receives end-of-stream event
    Given a long-running import finished and no more records are available
    When the server emits an event named "end"
    Then the client closes the EventSource and acknowledges completion

  Scenario: Client reconnects after transient network error
    Given a client is connected via EventSource
    When the connection experiences a transient error
    Then the client automatically attempts to reconnect within an exponential backoff capped at 30s
    And after reconnect the client resumes receiving new records without duplication (server may track last-sent rowid)

  Scenario: Server handles slow clients without blocking
    Given multiple clients (N) subscribe to /sse/next-record
    When the server emits frequent events (high throughput)
    Then slow or stalled clients should not block the server's ability to push to other clients
    And the server should drop or buffer per-client messages up to a documented limit

  Scenario: Invalid payload handling
    Given the server attempts to send a malformed JSON payload
    When the client receives it
    Then the client logs a parse error and continues listening for future valid messages

  Scenario: Authentication protected SSE
    Given the SSE endpoint requires an API token via Authorization header or query param
    When a client connects without valid credentials
    Then the server should return HTTP 401 and not upgrade to an EventSource stream

  Scenario: Backpressure and missed records policy
    Given a client falls behind and misses records while disconnected
    When the client reconnects and provides the last received rowid
    Then the server should resume sending records with rowid > last_received if available
    Or, if retention window expired, the server should respond with a retention error and advise full resync

