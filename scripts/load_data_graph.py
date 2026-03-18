from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

def run_import():
    with driver.session() as session:
        print("Importing Friends...")
        try:
            session.run("CREATE CONSTRAINT u_id FOR (u:User) REQUIRE u.id IS UNIQUE")
        except:
            pass
            
        session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///data/friends_graph.csv' AS row
        MERGE (u1:User {id: row.friend1})
        MERGE (u2:User {id: row.friend2})
        MERGE (u1)-[:FRIEND]->(u2)
        """)
        
        print("Importing Views...")
        try:
            session.run("CREATE CONSTRAINT p_id FOR (p:Product) REQUIRE p.id IS UNIQUE")
        except:
            pass
            
        session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///data/events_graph.csv' AS row
        MERGE (u:User {id: row.user_id})
        MERGE (p:Product {id: row.product_id})
        MERGE (u)-[:VIEWED]->(p)
        """)

if __name__ == "__main__":
    run_import()
    print("Graph data import finished!")