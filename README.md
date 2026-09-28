# Square Era PostgreSQL Database Manager Node 2 (`sesydb2`)

Secondary failover PostgreSQL database management runner for **Square Era**. This repository coordinates replica relational storage, high-availability monitoring, and redundant data synchronization with `yasamarium/sedb`.

## Architecture & Responsibilities
- **Node Role**: Secondary Failover PostgreSQL Database Manager (Node 2).
- **Automation Workflow (`.github/workflows/postgres-runner.yml`)**:
  - Automatically runs every 5 hours via staggered cron schedule (`30 */5 * * *`).
  - Staggered by 30 minutes from Node 1 (`yasamarium/sesydb`) to prevent simultaneous restart windows.
  - Spins up a dedicated PostgreSQL 15 service container.
  - Applies database schema (`schema/init.sql`) and seed data from `yasamarium/sedb`.
  - Runs the Express database management server (`manager.js`) on port 8080.
  - Exposes an encrypted public Cloudflare tunnel for external health inspection and admin queries.
  - Runs continuous health checks for 285 minutes (~4.75 hours).
  - Triggers the next workflow run before completion to maintain zero-downtime failover redundancy.

## REST API Endpoints
- `GET /`: Service information, failover operational role, and uptime metrics.
- `GET /health`: PostgreSQL connection status, pool metrics, and database timestamp.
- `GET /status`: Database storage size, table record counts (users, profiles, rooms, world blocks), and memory usage.
- `POST /sync`: Pulls the latest JSON snapshots from `yasamarium/sedb` and syncs them into PostgreSQL.
- `POST /query`: Authorized SQL query interface for administrative tasks.
