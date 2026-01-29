"""
Test script to check if multiple SSE connections can be opened simultaneously.
Tests both kelmarsh_data and kelmarsh_status endpoints at the same time.
"""

import asyncio
import aiohttp
import time


async def test_sse_connection(session: aiohttp.ClientSession, url: str, name: str, max_messages: int = 3):
    """Connect to an SSE endpoint and read a few messages."""
    print(f"[{name}] Connecting to {url}...")
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as response:
            print(f"[{name}] Status: {response.status}")
            print(f"[{name}] Headers: {dict(response.headers)}")

            if response.status != 200:
                print(f"[{name}] ERROR: Non-200 status code")
                return False

            count = 0
            async for line in response.content:
                decoded = line.decode('utf-8').strip()
                if decoded.startswith('data:'):
                    count += 1
                    # Truncate long messages
                    msg = decoded[:100] + '...' if len(decoded) > 100 else decoded
                    print(f"[{name}] Message {count}: {msg}")
                    if count >= max_messages:
                        print(f"[{name}] Got {max_messages} messages, closing connection.")
                        return True

            print(f"[{name}] Stream ended after {count} messages")
            return count > 0

    except asyncio.TimeoutError:
        print(f"[{name}] Timeout - no response within 15 seconds")
        return False
    except aiohttp.ClientError as e:
        print(f"[{name}] Connection error: {e}")
        return False
    except Exception as e:
        print(f"[{name}] Unexpected error: {type(e).__name__}: {e}")
        return False


async def test_concurrent_connections():
    """Test opening multiple SSE connections concurrently."""
    base_url = "http://127.0.0.1:8000"

    # Define endpoints to test
    endpoints = [
        (f"{base_url}/sse/by-turbine/kelmarsh_data/1?wait_seconds=1", "kelmarsh_data_1"),
        (f"{base_url}/sse/by-turbine/kelmarsh_status/1?wait_seconds=1", "kelmarsh_status_1"),
    ]

    print("=" * 60)
    print("Testing concurrent SSE connections")
    print("=" * 60)

    # Use a single session with connection pooling
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Run both connections concurrently
        tasks = [
            test_sse_connection(session, url, name)
            for url, name in endpoints
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        print("\n" + "=" * 60)
        print("Results:")
        print("=" * 60)
        for (url, name), result in zip(endpoints, results):
            if isinstance(result, Exception):
                print(f"  {name}: EXCEPTION - {result}")
            elif result:
                print(f"  {name}: SUCCESS")
            else:
                print(f"  {name}: FAILED")

        success_count = sum(1 for r in results if r is True)
        print(f"\nTotal: {success_count}/{len(endpoints)} connections successful")

        return success_count == len(endpoints)


async def test_sequential_connections():
    """Test opening SSE connections one at a time."""
    base_url = "http://127.0.0.1:8000"

    endpoints = [
        (f"{base_url}/sse/by-turbine/kelmarsh_data/1?wait_seconds=1", "kelmarsh_data_1"),
        (f"{base_url}/sse/by-turbine/kelmarsh_status/1?wait_seconds=1", "kelmarsh_status_1"),
    ]

    print("\n" + "=" * 60)
    print("Testing sequential SSE connections")
    print("=" * 60)

    connector = aiohttp.TCPConnector(limit=10, limit_per_host=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        results = []
        for url, name in endpoints:
            result = await test_sse_connection(session, url, name)
            results.append((name, result))
            await asyncio.sleep(0.5)  # Small delay between connections

        print("\n" + "=" * 60)
        print("Results:")
        print("=" * 60)
        for name, result in results:
            print(f"  {name}: {'SUCCESS' if result else 'FAILED'}")

        return all(r for _, r in results)


if __name__ == "__main__":
    print("SSE Multi-Connection Test")
    print("Make sure the API server is running on http://127.0.0.1:8000")
    print()

    # Test combined endpoint
    print("\n" + "=" * 60)
    print("Testing combined endpoint")
    print("=" * 60)

    async def test_combined():
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            url = "http://127.0.0.1:8000/sse/kelmarsh-combined/1?wait_seconds=1"
            result = await test_sse_connection(session, url, "combined_1", max_messages=3)
            print(f"Combined endpoint: {'SUCCESS' if result else 'FAILED'}")
            return result

    combined_ok = asyncio.run(test_combined())

    # Run sequential test first
    seq_ok = asyncio.run(test_sequential_connections())

    # Then run concurrent test
    conc_ok = asyncio.run(test_concurrent_connections())

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Combined endpoint: {'PASS' if combined_ok else 'FAIL'}")
    print(f"Sequential connections: {'PASS' if seq_ok else 'FAIL'}")
    print(f"Concurrent connections: {'PASS' if conc_ok else 'FAIL'}")

