import time
import random
from clickhouse_driver import Client
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List
import statistics


@dataclass
class QueryResult:
    query_name: str
    execution_time: float
    rows_returned: int
    success: bool
    error: str = None


class ClickHouseLoadTest:
    def __init__(self, host='localhost', port=9000):
        self.host = host
        self.port = port

    def get_client(self):
        """Создать новый клиент для каждого запроса"""
        try:
            return Client(
                host=self.host,
                port=self.port,
                connect_timeout=5,
                send_receive_timeout=30
            )
        except Exception as e:
            print(f"  ⚠️  Connection failed: {e}")
            return None

    def test_queries(self):
        """Набор тестовых запросов"""
        return {
            'top_categories': """
                SELECT 
                    category_id, 
                    sum(product_count) as product_count
                FROM catalog_by_category_mv 
                GROUP BY category_id
                ORDER BY product_count DESC 
                LIMIT 20
            """,
            'top_brands': """
                SELECT 
                    vendor, 
                    sum(product_count) as product_count
                FROM catalog_by_brand_mv 
                GROUP BY vendor
                ORDER BY product_count DESC 
                LIMIT 30
            """,
            'device_activity': """
                SELECT 
                    DeviceTypeName, 
                    sum(event_count) as total
                FROM events_by_device_mv
                GROUP BY DeviceTypeName
            """,
            'coverage_analysis': """
                SELECT
                    o.category_id,
                    COUNT(DISTINCT o.offer_id) as total,
                    COUNT(DISTINCT e.ContentUnitID) as covered
                FROM ecom_offers o
                LEFT JOIN raw_events e ON o.offer_id = e.ContentUnitID
                GROUP BY o.category_id
                LIMIT 50
            """,
            'uncovered_offers': """
                SELECT o.offer_id, o.category_id, o.vendor
                FROM ecom_offers o
                LEFT JOIN raw_events e ON o.offer_id = e.ContentUnitID
                WHERE e.ContentUnitID IS NULL
                LIMIT 1000
            """
        }

    def execute_query(self, query_name: str, query: str) -> QueryResult:
        """Выполнить один запрос"""
        client = self.get_client()
        if not client:
            return QueryResult(
                query_name=query_name,
                execution_time=0,
                rows_returned=0,
                success=False,
                error="Failed to connect"
            )

        try:
            start_time = time.time()
            result = client.execute(query)
            execution_time = time.time() - start_time

            return QueryResult(
                query_name=query_name,
                execution_time=execution_time,
                rows_returned=len(result),
                success=True
            )
        except Exception as e:
            return QueryResult(
                query_name=query_name,
                execution_time=0,
                rows_returned=0,
                success=False,
                error=str(e)
            )
        finally:
            try:
                client.disconnect()
            except:
                pass

    def run_load_test_incremental(self, start_workers=5, max_workers=50, step=5, queries_per_test=50):
        """
        Постепенно увеличивать нагрузку до падения
        """
        print("\n" + "=" * 60)
        print("🔥 INCREMENTAL LOAD TEST - Finding breaking point")
        print("=" * 60)

        results_by_workers = {}

        for workers in range(start_workers, max_workers + 1, step):
            print(f"\n{'=' * 60}")
            print(f"Testing with {workers} workers...")
            print(f"{'=' * 60}")

            results = []
            queries = self.test_queries()
            failed_connections = 0

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = []

                for i in range(queries_per_test):
                    query_name = random.choice(list(queries.keys()))
                    query = queries[query_name]
                    future = executor.submit(self.execute_query, query_name, query)
                    futures.append(future)

                completed = 0
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
                    completed += 1

                    if not result.success:
                        if "connect" in result.error.lower() or "reset" in result.error.lower():
                            failed_connections += 1

                    if completed % 10 == 0:
                        success_rate = len([r for r in results if r.success]) / len(results) * 100
                        print(f"  Progress: {completed}/{queries_per_test} | Success rate: {success_rate:.1f}%")

            # Анализ результатов
            successful = [r for r in results if r.success]
            success_rate = len(successful) / len(results) * 100

            results_by_workers[workers] = {
                'total': len(results),
                'successful': len(successful),
                'failed': len(results) - len(successful),
                'success_rate': success_rate,
                'failed_connections': failed_connections,
                'avg_time': statistics.mean([r.execution_time for r in successful]) if successful else 0
            }

            print(f"\n📊 Results for {workers} workers:")
            print(f"  Success rate: {success_rate:.1f}%")
            print(f"  Failed connections: {failed_connections}")
            print(f"  Avg query time: {results_by_workers[workers]['avg_time']:.3f}s")

            # Критерий остановки: >30% failure rate или >10 connection failures
            if success_rate < 70 or failed_connections > 10:
                print(f"\n🔴 BREAKING POINT REACHED at {workers} workers!")
                print(f"   ClickHouse couldn't handle this load.")
                print(f"   Maximum stable load: ~{workers - step} workers")
                break

            # Небольшая пауза между тестами
            time.sleep(2)

        # Итоговый отчет
        self.print_incremental_report(results_by_workers)

        return results_by_workers

    def print_incremental_report(self, results_by_workers):
        """Финальный отчет по нагрузке"""
        print("\n" + "=" * 60)
        print("📈 LOAD TEST SUMMARY")
        print("=" * 60)
        print(f"{'Workers':<10} {'Success Rate':<15} {'Avg Time':<15} {'Failed Conn':<15}")
        print("-" * 60)

        for workers, stats in sorted(results_by_workers.items()):
            print(f"{workers:<10} {stats['success_rate']:>6.1f}% {' ' * 8} "
                  f"{stats['avg_time']:>6.3f}s {' ' * 8} {stats['failed_connections']:<15}")

        # Определить максимальную стабильную нагрузку
        stable_workers = []
        for workers, stats in results_by_workers.items():
            if stats['success_rate'] >= 95 and stats['failed_connections'] < 5:
                stable_workers.append(workers)

        if stable_workers:
            max_stable = max(stable_workers)
            print(f"\n✅ Maximum stable load: {max_stable} concurrent workers")
            print(f"   (95%+ success rate, <5 failed connections)")

        print("=" * 60)


def main():
    load_test = ClickHouseLoadTest(host='localhost', port=9000)

    # Incremental load test
    results = load_test.run_load_test_incremental(
        start_workers=5,
        max_workers=50,
        step=5,
        queries_per_test=50
    )


if __name__ == "__main__":
    main()