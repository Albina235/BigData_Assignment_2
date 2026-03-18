#!/bin/sh

# Execute the SQL script inside the running PostgreSQL Docker container
# The -i flag keeps STDIN open, allowing us to pipe the SQL file into the container
docker exec -i bd_postgres psql -U user -d e_commerce < scripts/load_data_psql.sql

echo "PostgreSQL data import finished successfully!"