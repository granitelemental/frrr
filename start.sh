#!/bin/sh

sudo docker run -e POSTGRES_PASSWORD=mmCEEz -p 5432:5432 -v data:/var/lib/postgresql/data  -d  postgres
