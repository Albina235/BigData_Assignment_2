#!/bin/sh

docker exec -i bd_postgres psql -U user -d e_commerce < scripts/load_data_psql.sql

echo "PostgreSQL data import finished successfully!"