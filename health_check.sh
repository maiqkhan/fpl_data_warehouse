# healthcheck.sh

#!/bin/bash

# Run pg_isready with the environment variables, or defaults if they are not set
pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"