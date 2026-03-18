import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase
import time
import json
import statistics

pg_conn = psycopg2.connect(dbname="e_commerce", user="user", password="password", host="localhost", port="5432")
mongo_client = MongoClient("mongodb://user:password@localhost:27017/")
mongo_db = mongo_client["e_commerce_db"]
neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

def run_postgres(query_file):
    with open(query_file, 'r') as f:
        query = f.read()
    times = []
    result = None
    with pg_conn.cursor() as cur:
        for i in range(5):
            start = time.perf_counter()
            cur.execute(query)
            if i == 0: result = cur.fetchall()
            times.append((time.perf_counter() - start) * 1000)
    return result, statistics.mean(times), statistics.stdev(times) if len(times) > 1 else 0

def run_mongo(query_file, collection_name):
    with open(query_file, 'r') as f:
        pipeline = json.load(f)
    times = []
    result = None
    collection = mongo_db[collection_name]
    for i in range(5):
        start = time.perf_counter()
        res = list(collection.aggregate(pipeline))
        if i == 0: result = res
        times.append((time.perf_counter() - start) * 1000)
    return result, statistics.mean(times), statistics.stdev(times) if len(times) > 1 else 0

def run_neo4j(query_file):
    with open(query_file, 'r') as f:
        query = f.read()
    times = []
    result = None
    with neo4j_driver.session() as session:
        for i in range(5):
            start = time.perf_counter()
            res = session.run(query).data()
            if i == 0: result = res
            times.append((time.perf_counter() - start) * 1000)
    return result, statistics.mean(times), statistics.stdev(times) if len(times) > 1 else 0

if __name__ == "__main__":
    print("=== DATA ANALYSIS & BENCHMARKING ===")
    
    # ------------------ Q1 ------------------
    print("\n--- Q1: Campaign Analysis ---")
    pg_res, pg_mean, pg_stdev = run_postgres("scripts/q1.sql")
    print(f"Postgres Output: {pg_res[:2]}... | Avg: {pg_mean:.2f} ms | StdDev: {pg_stdev:.2f} ms")

    mg_res, mg_mean, mg_stdev = run_mongo("scripts/q1.js", "messages")
    print(f"MongoDB Output: {mg_res[:2]}... | Avg: {mg_mean:.2f} ms | StdDev: {mg_stdev:.2f} ms")
    
    nj_res, nj_mean, nj_stdev = run_neo4j("scripts/q1.cypherl")
    print(f"Neo4j Output: {nj_res[:2]}... | Avg: {nj_mean:.2f} ms | StdDev: {nj_stdev:.2f} ms")

    # ------------------ Q2 ------------------
    print("\n--- Q2: Top Recommendations ---")
    pg_res2, pg_mean2, pg_stdev2 = run_postgres("scripts/q2.sql")
    print(f"Postgres Output: {pg_res2[:2]}... | Avg: {pg_mean2:.2f} ms | StdDev: {pg_stdev2:.2f} ms")
    
    mg_res2, mg_mean2, mg_stdev2 = run_mongo("scripts/q2.js", "events")
    print(f"MongoDB Output: {mg_res2[:2]}... | Avg: {mg_mean2:.2f} ms | StdDev: {mg_stdev2:.2f} ms")
    
    nj_res2, nj_mean2, nj_stdev2 = run_neo4j("scripts/q2.cypherl")
    print(f"Neo4j Output: {nj_res2[:2]}... | Avg: {nj_mean2:.2f} ms | StdDev: {nj_stdev2:.2f} ms")

    # ------------------ Q3 ------------------
    print("\n--- Q3: Full Text Search ---")
    pg_res3, pg_mean3, pg_stdev3 = run_postgres("scripts/q3.sql")
    print(f"Postgres Output: {pg_res3[:2]}... | Avg: {pg_mean3:.2f} ms | StdDev: {pg_stdev3:.2f} ms")
    
    mg_res3, mg_mean3, mg_stdev3 = run_mongo("scripts/q3.js", "events")
    print(f"MongoDB Output: {mg_res3[:2]}... | Avg: {mg_mean3:.2f} ms | StdDev: {mg_stdev3:.2f} ms")

    pg_conn.close()
    neo4j_driver.close()