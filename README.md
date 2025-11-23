## CIT444 Final Project

This repo hosts the full Postgres data-loading pipeline plus the JavaFX desktop UI for browsing hotel ratings. Follow the steps below to build the containerized backend and launch the UI with JDK 21.

### Prerequisites

- Docker + Docker Compose v2
- Java 21 (Temurin, Oracle, etc.)
- Maven 3.9+
- (Optional) `xvfb` if you need to run the JavaFX app headlessly on Linux

### Load/Postgres Data via Docker Compose

1. Build all images:
	```bash
	docker compose build
	```
2. Seed the database (this runs the schema + data loaders and leaves Postgres running):
	```bash
	docker compose up postgres cit444final
	```
	Wait until the `cit444final` service prints `Ratings summary` twice and exits with code 0.
3. (Optional) Leave Postgres running in the background:
	```bash
	docker compose up -d postgres
	```

Environment variables set for both containers (and expected by the UI) are:

```
POSTGRES_HOST=postgres (or localhost when running the UI natively)
POSTGRES_PORT=5432
POSTGRES_DB=cit444
POSTGRES_USER=cit444
POSTGRES_PASSWORD=cit444
```

### Run the JavaFX UI with Maven

1. Ensure Postgres is running locally (either through Compose or your own instance) and reachable on the host/port above.
2. From the repo root run:
	```bash
	cd gui
	mvn clean javafx:run
	```
	The project already depends on the JavaFX Maven plugin (0.0.8) and will launch `Application.GUIApp`. If you need to override database settings, export the `POSTGRES_*` env vars before running Maven.

When finished, stop Postgres with `docker compose down`.
