#!/bin/bash

docker compose down

docker compose up -d postgres

npm run test -- --watch
