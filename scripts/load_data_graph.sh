#!/bin/sh
echo "Importing graph data via cypher-shell..."
cat scripts/import_graph.cypher | docker exec -i bd_neo4j cypher-shell -u neo4j -p password
echo "Import finished!"