# ⚽ Football Platform — Final Definitive Project Roadmap

> **Platform Scope:** World Cup Archive (2002–2026+) · WC World Maps · Messi Career Tab · Ronaldo Career Tab
> **Field:** Data Engineering · **Stack:** End-to-end modern DE pipeline to production frontend

---

## 📋 Table of Contents

| # | Phase | Core Focus |
|---|---|---|
| 0 | [Foundation & System Design](#phase-0) | Architecture, environment, domain study |
| 1 | [Data Source Strategy](#phase-1) | Where every piece of data comes from |
| 2 | [Data Ingestion Layer](#phase-2) | API fetching + Transfermarkt scraping |
| 3 | [Real-Time Stream Processing](#phase-3) | Kafka + Flink for live WC matches |
| 4 | [Batch Processing & Orchestration](#phase-4) | Airflow DAGs for all historical data |
| 5 | [Data Modeling](#phase-5) | PostgreSQL schema design |
| 6 | [Data Transformation](#phase-6) | dbt models between raw and analytics |
| 7 | [Analytical Storage](#phase-7) | ClickHouse for fast aggregations |
| 8 | [Data Quality](#phase-8) | Great Expectations validation |
| 9 | [Caching & Live State](#phase-9) | Redis for real-time match state |
| 10 | [Object Storage & CDN](#phase-10) | Cloudflare R2 for animation + assets |
| 11 | [Tracking Algorithm Engine](#phase-11) | Ball physics + player interpolation |
| 12 | [Backend API Layer](#phase-12) | FastAPI REST + WebSocket server |
| 13 | [Frontend Development](#phase-13) | Pitch tracker, maps, legends, analytics |
| 14 | [DevOps & Monitoring](#phase-14) | Docker, Kubernetes, Prometheus, Grafana |

---

## 🗺️ Full System Architecture Overview

```
DATA SOURCES
────────────────────────────────────────────────────────────
Sportradar API        StatsBomb Open Data       Transfermarkt
(live WC events)      (WC 2018/2022 free)        (career scrape)
YouTube Data API      Mapbox / Wikidata           Manual curation
(clip timestamps)     (stadium coords)            (video timestamps)

INGESTION LAYER
────────────────────────────────────────────────────────────
Python Fetchers → REST API calls, rate-limited, retry logic
Python Scrapers → Transfermarkt career data (BeautifulSoup/Playwright)
YouTube API     → Search clips per match, store video_id + timestamps

TRANSPORT LAYER
────────────────────────────────────────────────────────────
Apache Kafka    → All live events flow through topics
Apache Airflow  → All batch/historical jobs orchestrated here

PROCESSING LAYER
────────────────────────────────────────────────────────────
Apache Flink    → Real-time stream processing (live WC)
dbt             → Batch transformation (raw → clean → analytics)
Great Expectations → Data quality checks at every stage

STORAGE LAYER
────────────────────────────────────────────────────────────
PostgreSQL      → Primary relational store (all match + career data)
ClickHouse      → Analytical columnar store (stats, aggregations)
Redis           → Live match state cache (real-time only)
Cloudflare R2   → Animation files, heatmap images, static assets

API LAYER
────────────────────────────────────────────────────────────
FastAPI         → All REST endpoints
Django Channels → WebSocket server for live match broadcasting

FRONTEND LAYER
────────────────────────────────────────────────────────────
Next.js 14      → Full-stack React framework
PixiJS          → GPU-accelerated pitch tracker canvas
Mapbox GL JS    → Stadium maps + World Football Map
Recharts        → Analytics charts
Framer Motion   → UI transitions + animations
Tailwind CSS    → Styling
```

---

## 🏆 What This Project Covers in Data Engineering

> This is not a dashboard project. This is a full end-to-end data engineering platform.

| DE Concept | Tool Used | Why It Appears in Job Descriptions |
|---|---|---|
| **Data Ingestion** | Python + Sportradar + Transfermarkt scraper | Every DE pipeline starts here |
| **Stream Processing** | Apache Kafka + Apache Flink | Real-time pipelines, core DE skill |
| **Batch Processing** | Apache Airflow | Most-used orchestrator in industry |
| **Data Modeling** | PostgreSQL schema design | Foundational DE skill |
| **Data Transformation** | dbt | Most in-demand DE tool after Kafka/Spark |
| **Analytical Storage** | ClickHouse | OLAP columnar DB, replaces Spark for many use cases |
| **Data Quality** | Great Expectations | Data reliability, non-negotiable in production |
| **Caching Layer** | Redis | Operational data patterns |
| **CDN & Storage** | Cloudflare R2 | Cost-efficient object storage |
| **Orchestration** | Docker + Kubernetes | Deployment standard everywhere |
| **Monitoring** | Prometheus + Grafana | Observability, SRE basics |

### The One Line That Gets Interviews

> **Kafka → Flink → PostgreSQL → dbt → ClickHouse → FastAPI → Next.js**

Adding **dbt** between PostgreSQL and ClickHouse is what separates this from a student project. dbt is currently the single most in-demand tool in data engineering after Kafka and Spark. It transforms raw event data into clean, tested, documented analytical models — and every modern data team uses it.

```
Raw events stored in PostgreSQL
        ↓
dbt runs transformation models
        ↓
Clean, tested, documented tables in ClickHouse
        ↓
Frontend queries analytics instantly
```

---

<a name="phase-0"></a>
## 🔵 PHASE 0 — Foundation & System Design

> **Goal:** Understand the full domain, finalize what you are building, design the entire system on paper, set up your development environment.

**Duration:** 2–3 weeks
**Phase Tech Skills:** System design, domain research, Git, Docker basics, environment setup, data modeling concepts

---

### Step 0.1 — Deep Domain Study

**What to do:**

Before touching any tool, spend a full week understanding football data. You need to know what a "football event" actually is, what tracking data looks like, and what the difference between event data and tracking data means for your architecture.

**Key concepts to learn:**

- What is an **event** in football data — a pass, shot, carry, tackle, pressure, ball receipt. Each has a timestamp, player, team, pitch coordinates, and outcome.
- What is a **tracking frame** — a snapshot of every player's x/y position on the pitch taken 25 times per second. This is how smooth movement is possible.
- What is the **pitch coordinate system** — most providers use a 105m × 68m pitch mapped to normalized 0–1 values or a 120 × 80 unit grid.
- What is **xG (Expected Goals)** — a probability value (0 to 1) that a shot results in a goal based on position, angle, body part, and situation.
- What is the difference between **event data** (what happened) and **tracking data** (where everyone was at all times) — your architecture handles both.

**📚 Where to read:**

- StatsBomb Open Data GitHub: `github.com/statsbomb/open-data` — read the README and explore the JSON files to see what real football data looks like
- StatsBomb Data Spec: `github.com/statsbomb/statsbomb-data-spec` — the most important single document for understanding event schemas
- Friends of Tracking YouTube: search "Friends of Tracking" on YouTube — entire free playlist covering every football data concept
- Laurie Shaw's tracking tutorials: `github.com/Friends-of-Tracking-Data-FoTDA/LaurieOnTracking`
- "Soccermatics" by David Sumpter — non-technical introduction to football analytics, read in parallel

**Non-negotiable:** Do not skip this step. Every architectural decision in later phases depends on understanding what football data looks like.

---

### Step 0.2 — Define Exactly What You Are Building

**What to do:**

Write a single document that defines every section of the platform explicitly. No ambiguity. This becomes your specification.

**The three sections and what each contains:**

**Section 1 — World Cup Archive**
- All World Cup tournaments from 2002 to 2026 and future ones
- Each tournament: full fixture list from Group Stage MD1 to Final
- Each match: top-down pitch tracker replay, match stats, highlight clips
- Each tournament: stadium map of host country with all venue locations pinned
- World Cup World Map — different for each tournament showing all participating nations colored by their result in that year

**Section 2 — Messi Career Tab**
- Every official FIFA-recognized match from debut (Oct 2004) to retirement
- Career match numbered sequentially by date
- Each match: stats card (goals, assists, minutes, rating), pitch tracker, highlight clip (goals/assists only, curated YouTube timestamp)
- Stats counted from your own event data, not external sources

**Section 3 — Ronaldo Career Tab**
- Same as Messi tab, from debut (Aug 2002 Sporting CP) to retirement
- Same structure, same self-counted stats methodology

**📚 Where to read:**

- Product specification writing: "Shape Up" by Basecamp (free at `basecamp.com/shapeup`) — teaches scoping before building
- Transfermarkt for career data reference: `transfermarkt.com` — use to verify career match counts

---

### Step 0.3 — Design All Data Models on Paper

**What to do:**

Before creating any database, draw every entity and relationship on paper or a whiteboard. Every table you will eventually create should be planned here.

**Core entities to design:**

- **Tournament** — id, name, year, host country, host country coordinates
- **Stadium** — id, tournament_id, name, city, country, latitude, longitude, capacity
- **Team** — id, name, country code, home/away/third kit colors (hex), logo
- **Player** — id, team_id, name, jersey number, position, nationality, date of birth
- **Match** — id, tournament_id, home/away team, kickoff datetime, status, round name, matchday, venue, score, kit colors used, formation, animation file URL, replay available flag
- **MatchEvent** — id, match_id, timestamp (ms from kickoff), period, minute, second, event type, player_id, team_id, x/y coordinates, outcome, metadata
- **PlayerAppearance** — id, player_id, match_id, career_match_number, goals, assists, minutes played, yellow cards, rating
- **HighlightClip** — id, match_id, player_id, event_id, youtube_video_id, start_second, end_second, clip_type (goal/assist/skill)
- **Commentary** — id, match_id, event_id, timestamp_ms, language code, text
- **WorldMapData** — id, tournament_id, country_code, best_result (group stage / R16 / QF / SF / Final / Winner)

**📚 Where to read:**

- Entity-Relationship modeling: "Database Design for Mere Mortals" by Michael Hernandez — best beginner-friendly database design book
- PostgreSQL data types: `postgresql.org/docs/current/datatype.html`
- Normalization concepts: `use-the-index-luke.com` — explains database design with performance in mind

---

### Step 0.4 — Set Up Development Environment

**What to do:**

Set up your entire local development environment before writing any data pipeline code. Everything runs in Docker locally so your machine doesn't need any individual installs beyond Docker Desktop.

**Tools to install:**

- Docker Desktop — runs all services (PostgreSQL, Redis, Kafka, ClickHouse, Airflow) locally
- Python 3.11+ — for all backend and pipeline work
- Node.js 20+ — for frontend
- Git — version control
- VS Code — with Python, Docker, and dbt extensions
- DBeaver or TablePlus — database GUI for inspecting PostgreSQL and ClickHouse

**Project folder structure to create:**

```
football-platform/
├── apps/
│   ├── frontend/          Next.js app
│   ├── backend/           FastAPI REST API
│   └── ws-server/         Django Channels WebSocket
├── services/
│   ├── ingestion/         API fetchers + scrapers
│   ├── processing/        Flink jobs + event normalizers
│   └── analytics/         dbt project
├── infrastructure/
│   ├── docker/            All Docker and compose files
│   ├── airflow/           DAG definitions
│   ├── terraform/         Cloud provisioning
│   └── monitoring/        Prometheus + Grafana configs
├── data/
│   ├── raw/               Raw fetched data (never edited)
│   └── processed/         Cleaned data outputs
├── notebooks/             Jupyter for data exploration
└── docs/                  Architecture diagrams + decisions
```

**Local Docker services to run:**

- PostgreSQL 16
- Redis 7.2
- Apache Kafka + Zookeeper
- ClickHouse 23.11
- Apache Airflow 2.8
- Prometheus + Grafana

**📚 Where to read:**

- Docker official docs: `docs.docker.com`
- Docker Compose reference: `docs.docker.com/compose`
- Setting up Airflow with Docker: `airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html`

---

### Phase 0 End Result

✅ Full understanding of football event and tracking data formats
✅ Platform specification document written — all 3 sections defined
✅ All data models drawn on paper with every entity and relationship
✅ Local dev environment running — all Docker services healthy
✅ Project folder structure created and committed to Git

---

<a name="phase-1"></a>
## 🟠 PHASE 1 — Data Source Strategy

> **Goal:** Decide exactly where every single piece of data comes from — live events, historical matches, career data, stadium coordinates, kit colors, YouTube clips. No surprises later.

**Duration:** 1 week (research and access only, no building)
**Phase Tech Skills:** API documentation reading, data contract design, cost analysis, web research

---

### Step 1.1 — Live World Cup Data Source

**What to do:**

For live WC 2026 match events (goals, passes, shots, player positions), you need a commercial data provider. There is no free option for real-time professional match data.

**Ranked options:**

| Provider | Coverage | What You Get | Cost Tier |
|---|---|---|---|
| **Sportradar** | Official FIFA WC partner | Events + some tracking | $$$$ |
| **Stats Perform / Opta** | Premium tier | Events + tracking | $$$ |
| **API-Football (RapidAPI)** | Good WC coverage | Events only | $ |
| **Football-Data.org** | Basic | Scores + lineups | Free / $ |

**Recommendation:** Start with **API-Football** for development (affordable, good enough). Plan for **Sportradar** for production if budget allows — they are the official FIFA data rights holder.

**📚 Where to read:**

- API-Football docs: `www.api-football.com/documentation-v3`
- Sportradar developer portal: `developer.sportradar.com`
- Football-Data.org: `www.football-data.org/documentation/quickstart`

---

### Step 1.2 — Historical World Cup Data Source

**What to do:**

For WC 2002 through WC 2026 historical match data, use a combination of free and paid sources.

**Source map:**

| Tournament | Best Free Source | Paid Alternative |
|---|---|---|
| WC 2022 Qatar | StatsBomb Open Data (full events) | Stats Perform |
| WC 2018 Russia | StatsBomb Open Data (full events) | Stats Perform |
| WC 2014 Brazil | StatsBomb (partial) + OpenFootball | Opta |
| WC 2010 South Africa | OpenFootball (scores only) | Opta |
| WC 2006 Germany | OpenFootball (scores only) | Opta |
| WC 2002 Korea/Japan | OpenFootball (scores only) | Opta |

**Strategy:** Use StatsBomb free data for 2018 and 2022 (richest data). Use OpenFootball for older tournaments (scores, lineups, events at lower resolution). The tracker replay quality will naturally be better for newer tournaments — that is acceptable and honest.

**📚 Where to read:**

- StatsBomb open data: `github.com/statsbomb/open-data`
- statsbombpy Python library: `github.com/statsbomb/statsbombpy`
- OpenFootball: `github.com/openfootball/world-cup`

---

### Step 1.3 — Career Data Source (Messi & Ronaldo)

**What to do:**

Transfermarkt is the definitive source for complete career appearance data for both players. It lists every official match, date, competition, opponent, result, goals, assists, and minutes played.

**What to collect from Transfermarkt:**

- Full appearance history from debut to present
- Each row = one match with: date, competition, team, opponent, result, goals, assists, minutes, cards
- Career match number = sequential number ordered by date (you calculate this yourself)

**Clip curation strategy for YouTube:**

- Run a YouTube Data API search per match where goals or assists occurred
- Store the video ID and manually add start/end timestamps later
- Only curate clips where the player scored or assisted (roughly 400 matches each, not 1,000+)
- One-time effort: approximately 15–20 hours of manual timestamp work per player

**📚 Where to read:**

- Transfermarkt Messi page: `transfermarkt.com/lionel-messi/leistungsdaten/spieler/28003`
- Transfermarkt Ronaldo page: `transfermarkt.com/cristiano-ronaldo/leistungsdaten/spieler/8198`
- YouTube Data API v3: `developers.google.com/youtube/v3`
- YouTube embed timestamp docs: `developers.google.com/youtube/player_parameters`

---

### Step 1.4 — Stadium & Map Data Source

**What to do:**

For the stadium maps per World Cup tournament, you need latitude/longitude coordinates and basic metadata for every official WC venue from 2002 to 2026.

**Sources:**

- **Wikipedia** — every World Cup article lists all official stadiums with city and capacity. Wikipedia also has the coordinates in each stadium's own article.
- **Wikidata** — structured version of Wikipedia data, queryable via SPARQL API
- **Google Maps / OpenStreetMap** — verify coordinates visually before storing
- **Mapbox Tilesets** — for the actual map rendering on the frontend

**Data to collect per stadium:**

- Name, city, country, capacity, latitude, longitude, tournament year, matches played there, photo URL

**📚 Where to read:**

- Wikidata SPARQL endpoint: `query.wikidata.org`
- Mapbox developer docs: `docs.mapbox.com`
- OpenStreetMap Nominatim (free geocoding): `nominatim.openstreetmap.org`

---

### Step 1.5 — Kit Colors Data Source

**What to do:**

Kit colors for all 32 World Cup teams (per tournament) are needed to color player circles correctly on the pitch tracker. These are not available from any single API — they require manual curation.

**Sources:**

- Wikipedia "FIFA World Cup squads" pages — list team kits per tournament year
- Official team federation websites — have hex codes or Pantone references
- Football Kit Archive: `footballkitarchive.com` — visual reference for every kit ever worn
- Wikipedia "Kit" sections of national team articles

**What to store per team per tournament:**

- Home kit primary color (hex), home kit secondary color (hex)
- Away kit primary color (hex), away kit secondary color (hex)
- Logic: when home vs away colors are too similar (low contrast ratio), use the away team's alternate kit

**📚 Where to read:**

- Football Kit Archive: `footballkitarchive.com`
- WCAG color contrast formula: `www.w3.org/TR/WCAG20/#contrast-ratiodef`

---

### Phase 1 End Result

✅ Data provider chosen for live WC events with API key obtained
✅ StatsBomb + OpenFootball confirmed as historical WC sources
✅ Transfermarkt identified as career data source
✅ YouTube Data API access set up
✅ Stadium coordinate collection strategy defined
✅ Kit color curation plan in place

---

<a name="phase-2"></a>
## 🔴 PHASE 2 — Data Ingestion Layer

> **Goal:** Build every fetcher and scraper that pulls raw data from external sources into your system. This is the entry point of your entire pipeline.

**Duration:** 3–4 weeks
**Phase Tech Skills:** Python, REST API consumption, web scraping, rate limiting, retry logic, error handling, data serialization, Pydantic, environment variable management

---

### Step 2.1 — API Fetcher Architecture

**What to do:**

Build a structured Python fetching system — not just ad hoc scripts. Each data source gets its own fetcher class following the same pattern: initialize with credentials, fetch data, handle errors, return raw data untouched.

**Key design principles:**

- **Never transform data in the fetcher.** The fetcher only retrieves and returns raw API responses. Transformation happens downstream.
- **Always serialize to disk first** before doing anything. Write raw JSON to `/data/raw/` before any processing. This means if a transformation step fails, you never need to re-fetch.
- **Rate limiting is non-negotiable.** Every commercial API has a rate limit. Exceeding it gets your key banned. Build in delays and backoff logic from the start.
- **Retry with exponential backoff.** Networks fail. API servers go down. Your fetcher must retry automatically with increasing delays.
- **Log everything.** Every successful fetch and every error goes to a structured log. This is how you debug data gaps days later.

**Fetchers to build:**

- Live match event fetcher (Sportradar / API-Football)
- Historical WC match data fetcher (StatsBomb via statsbombpy)
- OpenFootball parser (reads their JSON files from GitHub)
- YouTube Data API clip searcher (searches by player + match + year)
- Wikidata stadium coordinate fetcher (SPARQL query)

**📚 Where to read:**

- Python `requests` library docs: `docs.python-requests.org`
- Pydantic for data validation: `docs.pydantic.dev` — use Pydantic to validate every API response shape
- Python `tenacity` library for retries: `tenacity.readthedocs.io`
- Python `ratelimit` library: `pypi.org/project/ratelimit`
- Structlog for structured logging: `www.structlog.org`

---

### Step 2.2 — Transfermarkt Scraper

**What to do:**

Transfermarkt does not have a public API. You scrape it. This is legal for personal/research use as long as you are respectful — low request rates, no commercial resale of the data.

**Scraping approach:**

- Use **Playwright** (not BeautifulSoup alone) because Transfermarkt uses some JavaScript rendering
- Scrape each player's full appearance history one page at a time
- Parse each table row into a structured appearance record
- Run only once (or once per season to add new matches) — not continuously

**Data to extract per appearance row:**

- Date, competition name, club name, opponent name, home/away flag, result, goals, assists, minutes played, yellow cards, red cards

**Key scraping rules:**

- Add 2–5 second random delays between requests — never hammer the server
- Use a realistic browser User-Agent string
- Respect `robots.txt` — verify it allows scraping before running
- Store all raw scraped HTML to disk before parsing — so you never need to re-scrape if parsing logic changes

**📚 Where to read:**

- Playwright Python docs: `playwright.dev/python/docs/intro`
- Web scraping ethics: `www.scrapingbee.com/blog/web-scraping-without-getting-blocked`
- BeautifulSoup docs: `www.crummy.com/software/BeautifulSoup/bs4/doc`
- Python `robots` library: `pypi.org/project/robotparser`

---

### Step 2.3 — Event Normalization Layer

**What to do:**

Every data source returns data in a different format. Sportradar looks nothing like StatsBomb. StatsBomb looks nothing like OpenFootball. You need a normalizer that converts all of them into your one canonical event schema.

**What the normalizer does:**

- Reads raw API/scraped data in the source's native format
- Maps the source's event type names to your canonical type names (e.g. Sportradar's `"shot_on_target"` → your `"shot"`)
- Converts pitch coordinates to your normalized 0–1 range regardless of source's native coordinate system
- Extracts player, team, timestamp, and outcome into your standard fields
- Wraps result in a Pydantic model for validation before passing downstream

**Canonical event types to define:**

- `pass`, `shot`, `goal`, `carry`, `tackle`, `foul`, `card`, `substitution`, `corner`, `free_kick`, `offside`, `save`, `clearance`, `interception`, `dribble`, `pressure`

**📚 Where to read:**

- Pydantic validators: `docs.pydantic.dev/latest/concepts/validators`
- StatsBomb event types reference: StatsBomb Data Spec PDF on their GitHub
- "Clean Architecture" by Robert C. Martin — Chapter on data transformation boundaries

---

### Step 2.4 — YouTube Clip Metadata Collection

**What to do:**

For each match where Messi or Ronaldo scored or assisted, run a YouTube API search and store the most relevant video ID. The start/end timestamps will be added manually later.

**Automated part:**

- For each appearance in the career database where goals > 0 or assists > 0, build a search query
- Query format: `"[Player name] goal [opponent] [year] [competition]"` for goals, similar for assists
- Store top 3 search results per match so you have options during manual curation
- Store: video ID, title, channel name, view count, publish date, thumbnail URL

**Manual curation part (one-time effort):**

- For each stored video ID, watch the clip
- Record the exact start and end seconds for the specific goal/assist moment
- Mark the clip type: `goal`, `assist`, `skill`, `freekick_goal`
- This takes approximately 15–20 hours per player — do it progressively, not all at once

**📚 Where to read:**

- YouTube Data API Search endpoint: `developers.google.com/youtube/v3/docs/search/list`
- YouTube IFrame Player parameters: `developers.google.com/youtube/player_parameters` — for start/end time embedding

---

### Phase 2 End Result

✅ All external data sources have working fetchers
✅ Transfermarkt scraper producing structured career appearance records
✅ Event normalizer converting all sources to one canonical schema
✅ Raw data being written to disk before any processing
✅ YouTube clip metadata collected for all goal/assist matches
✅ Rate limiting and retry logic protecting all external API calls

---

<a name="phase-3"></a>
## 🟡 PHASE 3 — Real-Time Stream Processing

> **Goal:** Build the live data pipeline that processes WC match events in real time — from API fetch to WebSocket delivery in under 2 seconds.

**Duration:** 3–4 weeks
**Phase Tech Skills:** Apache Kafka, Apache Flink (PyFlink), stream processing concepts, producer/consumer patterns, topic design, partitioning, deduplication, event enrichment

---

### Step 3.1 — Apache Kafka Setup & Topic Design

**What to do:**

Kafka is the central nervous system of your live pipeline. Everything flows through Kafka topics. Before writing any producer or consumer code, design your topic architecture carefully — changing it later is painful.

**Why Kafka for this project:**

- Multiple matches can be live simultaneously — Kafka handles fan-out naturally
- Multiple consumers need the same events independently (database writer, WebSocket broadcaster, analytics engine) — Kafka's consumer groups handle this without duplication
- If your WebSocket server restarts mid-match, it can replay missed events from Kafka's offset — no events are lost

**Topic design:**

| Topic Name | What Flows Through It | Partitioned By |
|---|---|---|
| `football.events.raw` | Raw unprocessed API events | match_id |
| `football.events.normalized` | After normalization | match_id |
| `football.events.enriched` | After player/team metadata added | match_id |
| `football.tracking.frames` | 25fps position frames (if available) | match_id |
| `football.match.status` | Match start, halftime, end signals | match_id |
| `football.errors` | Processing failures (dead letter queue) | none |

**Partitioning by match_id** ensures all events for one match go to the same partition in the same order — critical for correct timeline reconstruction.

**📚 Where to read:**

- Kafka official docs: `kafka.apache.org/documentation`
- "Kafka: The Definitive Guide" (free PDF from Confluent): `confluent.io/resources/kafka-the-definitive-guide`
- Kafka topic design best practices: `developer.confluent.io/courses/apache-kafka/topics`
- Confluent developer courses: `developer.confluent.io/courses/apache-kafka/events` — free, excellent

---

### Step 3.2 — Kafka Producers (Live Event Publishers)

**What to do:**

Build producers that fetch live match events from the API and publish them to Kafka topics. The producer polls the API at a regular interval (every 5 seconds for live events) and publishes only new events it hasn't seen before.

**Key producer behaviors:**

- Poll live match API every 5 seconds
- Deduplicate by tracking seen event IDs in memory (and in Redis for distributed producers)
- Publish each event as a JSON message with match_id as the Kafka message key
- Publish match status signals (kicked off, halftime, full time) to the match.status topic
- Handle API downtime gracefully — buffer locally and publish when connection restores

**📚 Where to read:**

- kafka-python producer docs: `kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html`
- Producer reliability settings: `docs.confluent.io/platform/current/installation/configuration/producer-configs.html`
- Idempotent producers in Kafka: `kafka.apache.org/documentation/#producerconfigs_enable.idempotence`

---

### Step 3.3 — Apache Flink Stream Processing Jobs

**What to do:**

Flink reads from Kafka topics, processes events in real time, and writes results back to Kafka or directly to Redis/PostgreSQL. It runs as always-on jobs, not batch scripts.

**Flink jobs to build:**

**Job 1 — Event Deduplicator:**
Reads from `football.events.raw`, filters duplicate events (same source event ID arriving multiple times), publishes clean events to `football.events.normalized`

**Job 2 — Event Enricher:**
Reads from `football.events.normalized`, looks up player name, jersey number, team name, and kit color from Redis cache, attaches them to the event, publishes to `football.events.enriched`

**Job 3 — Live State Updater:**
Reads from `football.events.enriched`, maintains a running match state in Redis (current score, last event, minute), updates after every event

**Job 4 — Windowed Statistics Calculator:**
Reads from `football.events.enriched`, computes rolling statistics over 5-minute and 15-minute windows (possession percentage, pass accuracy, shots on target in last 10 minutes), publishes to a stats topic consumed by the WebSocket broadcaster

**📚 Where to read:**

- Apache Flink docs: `nightlies.apache.org/flink/flink-docs-master`
- PyFlink (Python Flink API): `nightlies.apache.org/flink/flink-docs-master/docs/dev/python/overview`
- Flink + Kafka connector: `nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka`
- "Stream Processing with Apache Flink" by Fabian Hueske (O'Reilly) — most complete Flink book
- Flink stateful stream processing concepts: `flink.apache.org/what-is-flink/flink-architecture`

---

### Step 3.4 — WebSocket Broadcaster (Kafka Consumer → Clients)

**What to do:**

This consumer reads enriched events from Kafka and immediately pushes them to all WebSocket clients watching that match. It is the last step in the live pipeline — from here the data reaches the browser.

**Key behaviors:**

- Consumes from `football.events.enriched` Kafka topic
- For each event, identifies which match_id it belongs to
- Broadcasts to all WebSocket clients subscribed to that match_id's room
- When a new client connects mid-match, it sends the full current Redis state immediately so they see the current situation without waiting for the next event

**📚 Where to read:**

- Django Channels docs: `channels.readthedocs.io`
- Kafka consumer groups: `developer.confluent.io/courses/apache-kafka/consumers-hands-on`
- Redis Pub/Sub as channel layer: `channels.readthedocs.io/en/stable/topics/channel_layers.html`

---

### Phase 3 End Result

✅ Kafka running with all topics created and partitioned correctly
✅ Live event producer polling API and publishing to Kafka
✅ Flink deduplication job removing duplicate events
✅ Flink enricher adding player/team metadata to every event
✅ Flink windowed stats job producing rolling match statistics
✅ WebSocket broadcaster pushing events to connected browsers in under 2 seconds

---

<a name="phase-4"></a>
## 🟢 PHASE 4 — Batch Processing & Orchestration

> **Goal:** Use Apache Airflow to orchestrate all historical data loading — WC 2002–2022, career appearances, stadium coordinates, kit colors, clip metadata.

**Duration:** 3–4 weeks
**Phase Tech Skills:** Apache Airflow, DAG design, task dependencies, XCom, backfilling, schedule intervals, connection management, Python operators, sensors

---

### Step 4.1 — Apache Airflow Architecture

**What to do:**

Airflow orchestrates all batch jobs. Every historical data load, every career data update, every new match data load after a game ends — all managed by Airflow DAGs (Directed Acyclic Graphs). A DAG is just a workflow: a set of tasks with defined dependencies and a schedule.

**Why Airflow:**

- If a task fails halfway through loading WC 2014 data, Airflow retries only the failed task — not the whole pipeline
- You can backfill historical data on a schedule (load WC 2006 first, then 2010, etc.)
- Complete audit trail of every run — you can see exactly what ran when and why something failed
- Every DE job in industry uses either Airflow or a similar orchestrator (Prefect, Dagster) — knowing Airflow is a core DE skill

**DAGs to build:**

| DAG Name | Schedule | What It Does |
|---|---|---|
| `historical_wc_ingest` | Once (backfill) | Loads all WC 2002–2022 match + event data |
| `career_data_ingest` | Once + monthly | Scrapes Transfermarkt career appearances |
| `youtube_clip_search` | Once per player | Searches YouTube API for all goal/assist matches |
| `stadium_coordinates_ingest` | Once | Fetches all WC stadium coords from Wikidata |
| `kit_colors_ingest` | Once per tournament | Loads curated kit color data |
| `finished_match_archive` | Daily | After each WC26 match ends, archives it to historical store |
| `dbt_daily_refresh` | Daily | Runs all dbt models to refresh ClickHouse analytics |

**📚 Where to read:**

- Airflow official docs: `airflow.apache.org/docs/apache-airflow/stable`
- "Data Pipelines with Apache Airflow" by Bas Harenslak — best Airflow book, covers everything
- Airflow best practices: `airflow.apache.org/docs/apache-airflow/stable/best-practices.html`
- Airflow with Docker: `airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html`

---

### Step 4.2 — Historical WC Data Loading DAG

**What to do:**

Design the DAG that loads all historical World Cup data. It runs in sequence — tournament by tournament, match by match, event by event — with proper dependency chains.

**DAG task sequence:**

1. Fetch tournament metadata (name, year, host) → store to PostgreSQL
2. Fetch all matches for that tournament → store match records
3. For each match, fetch full event list → normalize events → store to PostgreSQL
4. Compute pre-aggregated analytics per match → write to ClickHouse
5. Run Great Expectations validation on loaded data → report on failures
6. Mark match as `replay_available = true` once all data verified

**Key Airflow concepts used here:**

- **TaskGroups** — group all tasks for one tournament together visually
- **XCom** — pass data between tasks (e.g. pass match IDs from task 2 to task 3)
- **Dynamic task mapping** — generate one task per match automatically instead of hardcoding
- **Sensors** — wait for external conditions (e.g. wait for StatsBomb data file to be available before starting)

**📚 Where to read:**

- Airflow XCom: `airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html`
- Airflow dynamic task mapping: `airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html`
- Airflow TaskGroups: `airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#taskgroups`

---

### Step 4.3 — Career Data Loading DAG

**What to do:**

The career DAG runs the Transfermarkt scraper for Messi and Ronaldo, parses the results, and loads appearance records into PostgreSQL. It runs once initially, then monthly to pick up new matches.

**DAG task sequence:**

1. Scrape Transfermarkt appearance pages for each player
2. Parse HTML into structured appearance records
3. Deduplicate against existing records in PostgreSQL (don't re-insert existing matches)
4. Assign career_match_number sequentially by date
5. For new appearances: trigger YouTube search task for matches with goals/assists
6. Validate loaded data with Great Expectations
7. Refresh dbt career models

**📚 Where to read:**

- Airflow PythonOperator: `airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html`
- Airflow connections for PostgreSQL: `airflow.apache.org/docs/apache-airflow/stable/howto/connection.html`

---

### Phase 4 End Result

✅ Airflow running with all DAGs defined
✅ Historical WC data (2002–2022) fully loaded via backfill
✅ Messi and Ronaldo career appearance data loaded
✅ YouTube clip metadata populated for all goal/assist matches
✅ Stadium coordinates loaded for all WC tournaments
✅ Kit colors loaded for all teams across all tournaments
✅ Daily archival DAG ready to archive live WC26 matches as they finish

---

<a name="phase-5"></a>
## 🔵 PHASE 5 — Data Modeling

> **Goal:** Design and implement the complete PostgreSQL schema — every table, every relationship, every index. This is the single most important phase for long-term data reliability.

**Duration:** 2 weeks
**Phase Tech Skills:** PostgreSQL, SQLAlchemy ORM, Alembic migrations, relational database design, normalization, indexing strategy, UUID usage, JSON columns

---

### Step 5.1 — Core Schema Implementation

**What to do:**

Implement all data models designed in Phase 0 as actual SQLAlchemy ORM models. Use UUIDs as primary keys throughout (not integer sequences) — they are globally unique, safe to generate anywhere in a distributed system, and safe to expose in URLs.

**Tables to implement:**

- tournaments, stadiums, teams, players
- matches, match_events
- player_appearances (career data for Messi/Ronaldo)
- highlight_clips (YouTube video_id + timestamps)
- commentary
- world_map_data (per tournament per country, best result)

**Critical schema decisions to make correctly:**

- Store pitch coordinates as normalized floats (0.0 to 1.0) — never store raw provider coordinates
- Store kit colors as 7-character hex strings (`#FFFFFF`) — not RGB tuples, not color names
- Use PostgreSQL's native JSONB column for event metadata — gives you flexible storage with indexable JSON paths
- Store timestamps in milliseconds from kickoff as integers — never as clock time strings
- The `career_match_number` is computed via SQL window function, not stored as a fixed column — it stays correct automatically

**📚 Where to read:**

- SQLAlchemy docs: `docs.sqlalchemy.org/en/20`
- SQLAlchemy ORM tutorial: `docs.sqlalchemy.org/en/20/tutorial/orm_related_objects.html`
- PostgreSQL JSONB: `postgresql.org/docs/current/datatype-json.html`
- UUID in PostgreSQL: `postgresql.org/docs/current/datatype-uuid.html`
- "PostgreSQL: Up and Running" by Regina Obe (O'Reilly)

---

### Step 5.2 — Migration Management with Alembic

**What to do:**

Never manually alter your database schema. Use Alembic to manage every schema change as a versioned migration file. This is non-negotiable in any production system.

**How Alembic works:**

- Every schema change (add table, add column, change type) becomes a migration file
- Alembic tracks which migrations have been applied
- To apply changes: run `alembic upgrade head`
- To roll back: run `alembic downgrade -1`
- Migration files are committed to Git — your entire schema history is version controlled

**📚 Where to read:**

- Alembic docs: `alembic.sqlalchemy.org`
- Alembic tutorial: `alembic.sqlalchemy.org/en/latest/tutorial.html`

---

### Step 5.3 — Indexing Strategy

**What to do:**

Without the right indexes, your queries will be slow. An unindexed query on the match_events table (which can have millions of rows) will time out. Plan every index before any data is loaded.

**Mandatory indexes to create:**

- `match_events(match_id)` — all event queries filter by match first
- `match_events(match_id, timestamp_ms)` — for timeline queries
- `match_events(event_type)` — for filtering by event type across matches
- `match_events(player_id)` — for career event lookups
- `commentary(match_id, language_code)` — commentary queries always filter by both
- `matches(tournament_id, status)` — fixture browser queries
- `matches(tournament_id, matchday)` — round/matchday navigation
- `player_appearances(player_id, match_date)` — career timeline queries
- `highlight_clips(match_id, player_id)` — clip lookup per match per player

**📚 Where to read:**

- PostgreSQL indexing guide: `postgresql.org/docs/current/indexes.html`
- `use-the-index-luke.com` — entire free online book on database indexing, most practical guide available
- PostgreSQL EXPLAIN ANALYZE: `postgresql.org/docs/current/sql-explain.html` — how to profile slow queries

---

### Phase 5 End Result

✅ All tables implemented as SQLAlchemy models
✅ All foreign key relationships enforced at database level
✅ Alembic migration history tracking every schema change
✅ All critical indexes created and verified
✅ JSONB columns used where flexible metadata is needed
✅ Coordinate and color storage conventions enforced

---

<a name="phase-6"></a>
## 🟣 PHASE 6 — Data Transformation with dbt

> **Goal:** Build the dbt project that transforms raw PostgreSQL event data into clean, tested, documented analytical models in ClickHouse.

**Duration:** 2–3 weeks
**Phase Tech Skills:** dbt Core, SQL, data modeling patterns (staging/intermediate/mart layers), dbt tests, dbt documentation, dbt sources, Jinja templating in SQL

---

### Step 6.1 — Understanding dbt's Role

**What to do:**

Before building dbt models, understand exactly what dbt does and why it is the most important tool addition to your stack.

**What dbt does:**

- Takes raw data in your data warehouse (PostgreSQL → ClickHouse) and transforms it into clean, analytics-ready tables using SQL
- Every transformation is a `.sql` file tracked in Git
- dbt adds automated testing to your SQL (does this column have any nulls? are foreign keys intact? are values in the expected range?)
- dbt generates a full documentation website showing every model, its columns, its tests, and its lineage (which models depend on which)
- When you run `dbt run`, it executes all transformations in the correct dependency order

**The three-layer architecture dbt follows:**

```
Layer 1 — Staging (stg_)
    Direct 1:1 copy of source tables, lightly cleaned
    Rename columns to consistent names, cast types, nothing else
    
Layer 2 — Intermediate (int_)
    Join staging models together, business logic applied
    e.g. int_match_events_enriched joins events with player + team data
    
Layer 3 — Marts (mart_)
    Final analytics-ready tables consumed by ClickHouse + frontend
    e.g. mart_career_stats, mart_wc_match_summary, mart_goal_library
```

**📚 Where to read:**

- dbt official docs: `docs.getdbt.com` — comprehensive, well-written
- dbt courses (free): `courses.getdbt.com` — "dbt Fundamentals" course, 5 hours, covers everything
- dbt project structure: `docs.getdbt.com/docs/build/projects`
- "The Analytics Engineering Guide" by dbt Labs: `getdbt.com/analytics-engineering`
- dbt Discourse community: `discourse.getdbt.com` — best forum for dbt questions

---

### Step 6.2 — dbt Models to Build

**What to do:**

Design and build the specific dbt models your frontend and API will query.

**Staging models (stg_):**

- `stg_matches` — clean match records with consistent column names
- `stg_match_events` — clean events, coordinates verified in 0–1 range
- `stg_player_appearances` — career appearances with career_match_number computed
- `stg_teams` — team metadata with kit colors
- `stg_players` — player metadata

**Intermediate models (int_):**

- `int_events_with_context` — events joined with player name, team name, kit color
- `int_match_summary` — aggregated match-level statistics (shots, possession estimate, goals)
- `int_career_appearances_enriched` — career appearances joined with match metadata and clip availability flags

**Mart models (mart_):**

- `mart_wc_match_summary` — one row per WC match with all stats ready for the frontend stats panel
- `mart_career_stats_by_season` — Messi/Ronaldo stats aggregated by season and competition
- `mart_goal_library` — every goal event joined with clip metadata and match context
- `mart_world_map_by_tournament` — per-country per-tournament best result for the World Map feature
- `mart_stadium_directory` — all stadiums per tournament for the stadium map feature

**📚 Where to read:**

- dbt model materializations: `docs.getdbt.com/docs/build/materializations`
- dbt Jinja + macros: `docs.getdbt.com/docs/build/jinja-macros`
- dbt incremental models: `docs.getdbt.com/docs/build/incremental-models` — critical for large event tables

---

### Step 6.3 — dbt Tests

**What to do:**

Every dbt model gets tests. Tests run automatically before data reaches ClickHouse. Failed tests block the pipeline and alert you.

**Tests to add:**

- `not_null` on every primary key and critical foreign key
- `unique` on all primary key columns
- `accepted_values` for event_type (only the canonical types you defined), language_code, match status
- `relationships` to verify foreign key integrity between models
- Custom test: all x/y coordinates are between 0.0 and 1.0
- Custom test: no match has more than 11 home or away players in any single frame
- Custom test: career_match_numbers are sequential per player with no gaps

**📚 Where to read:**

- dbt tests: `docs.getdbt.com/docs/build/data-tests`
- Custom dbt generic tests: `docs.getdbt.com/guides/writing-custom-generic-tests`

---

### Phase 6 End Result

✅ Full three-layer dbt project (staging, intermediate, marts)
✅ All mart models producing clean analytics-ready tables
✅ dbt tests passing on all models before data reaches ClickHouse
✅ dbt documentation site auto-generated with lineage graphs
✅ Airflow running `dbt run` daily via dbt Cloud CLI or dbt Core

---

<a name="phase-7"></a>
## 🟠 PHASE 7 — Analytical Storage

> **Goal:** Configure ClickHouse as the analytical query layer. Pre-aggregated dbt mart tables land here and the frontend queries this for all stats — fast.

**Duration:** 1–2 weeks
**Phase Tech Skills:** ClickHouse, columnar databases, OLAP concepts, partitioning, MergeTree engine, materialized views, ClickHouse HTTP interface

---

### Step 7.1 — Why ClickHouse

**What to do:**

Understand what ClickHouse is and why you use it separately from PostgreSQL.

**PostgreSQL vs ClickHouse — when to use each:**

| Situation | Use PostgreSQL | Use ClickHouse |
|---|---|---|
| Writing a new match event | ✅ | ❌ |
| Fetching one player's full career record | ✅ | ❌ |
| "What is Brazil's total xG across all WC matches?" | ❌ slow | ✅ fast |
| "How many passes were completed in the final third this half?" | ❌ slow | ✅ fast |
| Any aggregation across millions of rows | ❌ | ✅ |

ClickHouse is a **columnar** database — it stores each column separately, so aggregation queries only read the specific columns they need. A `SUM(goals)` query on 10 million events reads only the goals column. PostgreSQL would read every row. For analytics, this is 100x faster.

**📚 Where to read:**

- ClickHouse docs: `clickhouse.com/docs`
- ClickHouse vs PostgreSQL: `clickhouse.com/comparison/postgresql`
- ClickHouse MergeTree engine: `clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree`
- "ClickHouse in Action" by Mark Needham — best practical ClickHouse guide

---

### Step 7.2 — ClickHouse Schema Design

**What to do:**

Design ClickHouse tables to receive dbt mart model outputs. Every table uses the MergeTree engine and is partitioned by date for query efficiency.

**Tables to create:**

- `match_events_analytics` — all events across all tournaments, partitioned by month
- `career_appearances_analytics` — all career appearances for Messi and Ronaldo
- `match_summary_analytics` — one row per match with all pre-aggregated stats
- `goal_library_analytics` — every goal with context and clip metadata
- `world_map_analytics` — per-country per-tournament result data

**Key ClickHouse-specific settings:**

- `ORDER BY` clause determines physical sort order — choose columns that match your most common query filters (tournament_id, player_id, match_id)
- `PARTITION BY` splits data into physical parts by date — queries filtering by date skip entire partitions
- Use `LowCardinality(String)` for columns with few distinct values (event_type, language_code, match_status) — this compresses and speeds up those columns dramatically

**📚 Where to read:**

- ClickHouse MergeTree partitioning: `clickhouse.com/docs/en/engines/table-engines/mergetree-family/custom-partitioning-key`
- LowCardinality: `clickhouse.com/docs/en/sql-reference/data-types/lowcardinality`
- ClickHouse query optimization: `clickhouse.com/docs/en/optimize/query-optimization`

---

### Phase 7 End Result

✅ ClickHouse running with all analytics tables created
✅ dbt outputting mart models directly into ClickHouse tables
✅ Sample queries running sub-second on full tournament datasets
✅ FastAPI analytics endpoints querying ClickHouse, not PostgreSQL

---

<a name="phase-8"></a>
## ✅ PHASE 8 — Data Quality

> **Goal:** Implement automated data quality checks at every stage of the pipeline using Great Expectations. No bad data silently enters your system.

**Duration:** 1–2 weeks
**Phase Tech Skills:** Great Expectations, data profiling, expectation suites, checkpoints, data documentation, validation reporting

---

### Step 8.1 — Great Expectations Setup

**What to do:**

Great Expectations (GX) is a Python library that lets you define what "good data" looks like — called Expectations — and then automatically checks every batch of data against those expectations. Failed expectations alert you before bad data corrupts your analytics.

**Where to place GX checkpoints in your pipeline:**

- After raw data is fetched (before normalization) — catch API response format changes
- After normalization (before PostgreSQL write) — catch coordinate range violations
- After Airflow historical data load — verify all expected matches are present
- After dbt run (before ClickHouse write) — verify mart model outputs

**Expectations to define per data type:**

For match events:
- timestamp_ms is never null and is between 0 and 7,200,000 (120 minutes max)
- x and y coordinates are between 0.0 and 1.0 (95th percentile — some edge cases allowed)
- event_type is always in your defined canonical type list
- every event has a match_id that exists in the matches table

For career appearances:
- career_match_number is unique per player
- goals and assists are non-negative integers
- minutes_played is between 1 and 120

**📚 Where to read:**

- Great Expectations docs: `docs.greatexpectations.io`
- GX quickstart tutorial: `docs.greatexpectations.io/docs/tutorials/quickstart`
- "Data Quality Fundamentals" by Barr Moses (O'Reilly) — broader data quality concepts
- GX + Airflow integration: `docs.greatexpectations.io/docs/deployment_patterns/how_to_use_great_expectations_with_airflow`

---

### Phase 8 End Result

✅ Great Expectations installed and configured
✅ Expectation suites defined for all major data types
✅ Checkpoints integrated into Airflow DAGs
✅ Data docs site generated showing pass/fail history
✅ Alerts configured for expectation failures

---

<a name="phase-9"></a>
## 🔴 PHASE 9 — Caching & Live State

> **Goal:** Use Redis to store and serve the real-time state of any live match so new WebSocket clients get immediate full context when they connect.

**Duration:** 1 week
**Phase Tech Skills:** Redis, hash data structures, key expiry, Redis Pub/Sub, atomic operations, cache invalidation patterns

---

### Step 9.1 — What Redis Stores

**What to do:**

Redis holds only live, temporary data. It is not a permanent store. Everything in Redis has a TTL (time to live) — it automatically deletes after the match ends.

**What Redis holds per live match:**

- Current score (home and away)
- Current match minute and period
- All 22 player positions (last known x/y from most recent event)
- Ball position (last known x/y)
- Last 20 events (for new client sync)
- Which team has possession currently
- Match status (first_half / halftime / second_half / full_time)

**Key naming convention:**

All keys follow `match:live:{match_id}:{data_type}` format. Example: `match:live:abc123:score`. This namespace avoids key collisions and makes all keys for one match easy to identify and delete together.

**TTL strategy:**

All live match keys expire after 4 hours from creation. No match lasts 4 hours. This prevents Redis filling up with stale data without requiring manual cleanup.

**📚 Where to read:**

- Redis data types: `redis.io/docs/data-types`
- Redis key expiry: `redis.io/commands/expire`
- Redis Hash commands: `redis.io/commands/?group=hash`
- redis-py Python client: `redis-py.readthedocs.io`
- "Redis in Action" by Josiah Carlson — chapters 2 and 3 on data structures

---

### Phase 9 End Result

✅ Redis storing full live match state for all concurrent live matches
✅ All Redis keys have TTLs — no manual cleanup needed
✅ New WebSocket clients receive full current state on connect from Redis
✅ Redis Pub/Sub wiring events from Kafka consumers to WebSocket broadcaster

---

<a name="phase-10"></a>
## 💾 PHASE 10 — Object Storage & CDN

> **Goal:** Store all large binary assets (pre-computed animation files, heatmap images, stadium photos) on Cloudflare R2 with global CDN delivery.

**Duration:** 1 week
**Phase Tech Skills:** Cloudflare R2, S3-compatible API, CDN concepts, content addressing, gzip compression, cache headers

---

### Step 10.1 — Why Cloudflare R2 Over AWS S3

**What to do:**

Understand the cost difference before choosing storage.

AWS S3 charges for both storage AND egress (data transfer out). A read-heavy platform where thousands of users load match animations pays egress fees on every animation file loaded. Cloudflare R2 has zero egress fees — you only pay for storage itself. For a platform serving animation files repeatedly, R2 is 80–90% cheaper.

R2 uses the same S3-compatible API as AWS S3. No code changes — just change the endpoint URL and credentials.

**What goes in R2:**

| Asset Type | Size Estimate | How Often Served |
|---|---|---|
| Pre-computed match animation (.json.gz) | 30–80 MB per match | Every time someone watches that match |
| Player heatmap images (.png) | 100–200 KB each | Every match view |
| Stadium photos (.webp) | 200–400 KB each | Per WC tournament page load |
| Team logo images (.svg) | 5–20 KB each | Every page load |

**What does NOT go in R2:**

- Video files — use YouTube embeds (no storage cost at all)
- Match event data — lives in PostgreSQL
- Analytics data — lives in ClickHouse

**📚 Where to read:**

- Cloudflare R2 docs: `developers.cloudflare.com/r2`
- R2 S3 compatibility: `developers.cloudflare.com/r2/api/s3/api`
- boto3 with R2: `developers.cloudflare.com/r2/examples/boto3` — same library as AWS S3
- Cloudflare CDN caching: `developers.cloudflare.com/cache`

---

### Step 10.2 — Animation File Pre-computation Strategy

**What to do:**

For historical matches, animation files are pre-computed offline and stored in R2. The frontend fetches the file once, plays it locally — no WebSocket needed for historical replays.

**Pre-computation process:**

1. Take all MatchEvent records for a finished match from PostgreSQL
2. Run the ball physics interpolation algorithm (Phase 11) to generate 60fps ball positions
3. Run the player position estimation algorithm to generate all 22 player positions per frame
4. Package everything as a single compressed JSON file
5. Upload to R2 at path `animations/{match_id}/full_match.json.gz`
6. Update the match record in PostgreSQL with the R2 URL
7. Set `replay_available = true` on the match record

**Compression strategy:**

Use gzip compression. A raw animation JSON for 90 minutes at 60fps with 23 entities is approximately 400–600MB. After gzip compression it reduces to 30–80MB. This compression step is mandatory — never serve uncompressed animation files.

**📚 Where to read:**

- Python gzip module: `docs.python.org/3/library/gzip.html`
- HTTP compression headers: `developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding`

---

### Phase 10 End Result

✅ Cloudflare R2 bucket created with correct public access settings
✅ All pre-computed animations uploaded and accessible via CDN URL
✅ All heatmap images and stadium photos in R2
✅ Match records updated with R2 URLs once animations are ready
✅ Cache headers set correctly so browsers cache animation files aggressively

---

<a name="phase-11"></a>
## ⚽ PHASE 11 — Ball & Player Tracking Algorithm Engine

> **Goal:** Build the algorithms that convert sparse match event data into smooth, continuous 60fps animation — including parabolic ball flight, carrying bounce simulation, and formation-based player movement.

**Duration:** 3–5 weeks
**Phase Tech Skills:** NumPy, SciPy, interpolation theory, Bezier curves, physics simulation, parametric equations, signal processing, formation analysis

---

### Step 11.1 — The Core Problem

**What to do:**

Understand the interpolation challenge before writing any algorithm.

Raw event data gives you the ball position at discrete moments only — a pass starts at position A at time T1, and the receiving player is at position B at time T2. But T2 might be 8 seconds after T1. At 60fps, you need 480 frames between those two known positions.

Simply drawing a straight line from A to B looks robotic. You need physically realistic motion for each event type:

- **Pass / Cross / Shot** → parabolic arc (ball goes up then comes down)
- **Carry / Dribble** → curved path with small bounce (ball bounces near player's feet while running)
- **Free kick / Corner** → higher arc, slower ascent
- **Ball out of play** → immediate stop

For players, you need formation-based positional estimation — players don't stand still between events. They move toward or away from the ball based on their role and which team has possession.

**📚 Where to read:**

- Bezier curves (interactive guide): `pomax.github.io/bezierinfo` — best resource for understanding curve interpolation visually
- NumPy docs: `numpy.org/doc/stable`
- SciPy interpolation: `docs.scipy.org/doc/scipy/reference/interpolate.html`
- "Physics for Game Developers" by David Bourg — projectile motion chapter
- Laurie Shaw tracking tutorial: `github.com/Friends-of-Tracking-Data-FoTDA/LaurieOnTracking` — direct implementation reference

---

### Step 11.2 — Ball Physics Interpolation

**What to do:**

Build separate interpolation functions for each event type.

**Pass/Shot interpolation:**
Uses parametric parabolic equations. The ball travels horizontally at constant speed while vertically following a parabola that peaks at the midpoint of the journey. Peak height varies by pass type — ground pass stays near zero height, a cross peaks at 3–4 meters normalized, a shot peaks at 1–2 meters.

**Carry/Dribble interpolation:**
Uses a quadratic Bezier curve with a slight random midpoint offset (players never run in perfectly straight lines) combined with a sinusoidal height oscillation to simulate the ball bouncing near the player's feet during a run. Bounce frequency is approximately once every 0.3 seconds.

**Key parameters to tune:**
- LERP factor: controls how fast rendered elements catch up to their target position (lower = smoother but adds latency)
- FPS target: 60fps for smooth playback
- Bounce amplitude: small enough to be subtle, large enough to be visible

**📚 Where to read:**

- Parametric equations: any first-year calculus textbook, chapter on parametric curves
- Quadratic Bezier curve math: `pomax.github.io/bezierinfo/#introduction`
- Linear interpolation (LERP): search "game dev LERP smooth movement" on YouTube — many visual explanations

---

### Step 11.3 — Player Position Estimation

**What to do:**

When you only have event-level data (not full 25fps tracking), players need to move to believable positions between events. Build a formation-based estimator.

**Estimation logic:**

1. Every team has a formation (4-3-3, 4-4-2, etc.) — this gives each player a base position on the pitch
2. Each player role has an "attraction factor" toward the ball — strikers are more attracted to ball position than center backs
3. Possession state affects positioning — the team with possession pushes higher up the pitch
4. Special situations override base logic — after a goal, all outfield players return to their half; at a corner, defenders cluster at the near post

**Formation position data to build:**

Define the normalized x/y coordinates for each role in each common formation (4-3-3, 4-4-2, 4-2-3-1, 3-5-2). These become the "rest positions" that get modified by ball attraction and possession state.

**📚 Where to read:**

- "Beyond Expected Goals" by William Spearman — pitch control and player positioning (search ResearchGate)
- William Spearman's pitch control implementation: `github.com/Friends-of-Tracking-Data-FoTDA` — community implementations
- NumPy linear algebra: `numpy.org/doc/stable/reference/routines.linalg.html`

---

### Phase 11 End Result

✅ Ball parabolic arc working for all pass/shot event types
✅ Ball carrying bounce simulation implemented
✅ Bezier curve paths eliminating robotic straight-line movement
✅ Formation-based player position estimation for all common formations
✅ Pre-computation pipeline generating and compressing animation files for all historical matches

---

<a name="phase-12"></a>
## 🔌 PHASE 12 — Backend API Layer

> **Goal:** Build the FastAPI REST API and Django Channels WebSocket server that serve all data to the frontend.

**Duration:** 3–4 weeks
**Phase Tech Skills:** FastAPI, async Python, Pydantic response models, SQLAlchemy async, Django Channels, WebSocket protocol, JWT authentication, rate limiting, API versioning

---

### Step 12.1 — FastAPI REST API

**What to do:**

Build clean, versioned REST endpoints. All endpoints are prefixed with `/api/v1/` — this allows you to make breaking changes later under `/api/v2/` without breaking existing clients.

**Endpoint groups to build:**

**Tournaments & Fixtures:**
- GET all tournaments → returns tournament list with years and host countries
- GET fixtures for a tournament → returns all matches grouped by round, with status indicators
- GET stadium list for a tournament → returns all venues with coordinates for the map

**Match Data:**
- GET match by ID → full metadata including team info, kit colors, formations
- GET match events → all events, optionally filtered by type and time range
- GET match animation URL → returns R2 pre-signed URL for the animation JSON
- GET match analytics → calls ClickHouse for all stats

**Career Data:**
- GET career summary for a player → season-by-season stats
- GET career appearance list → all matches with career_match_number
- GET appearance detail → one match with events, stats, clip metadata

**Commentary:**
- GET commentary for a match → filtered by language code

**World Map:**
- GET world map data for a tournament → per-country best result for choropleth coloring

**Key API design rules:**
- All responses use consistent Pydantic response models — never return raw SQLAlchemy objects
- Pagination on all list endpoints — never return unbounded lists
- Use `select_related` / `joinedload` in SQLAlchemy to avoid N+1 query problems
- Cache expensive ClickHouse queries in Redis with a 5-minute TTL

**📚 Where to read:**

- FastAPI official tutorial: `fastapi.tiangolo.com/tutorial` — comprehensive, free, excellent
- FastAPI async SQLAlchemy: `fastapi.tiangolo.com/tutorial/sql-databases`
- Pydantic v2 response models: `docs.pydantic.dev/latest/concepts/json_schema`
- SQLAlchemy async: `docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html`
- REST API design: "REST API Design Rulebook" by Mark Masse

---

### Step 12.2 — WebSocket Server

**What to do:**

The WebSocket server handles all real-time communication for live matches. It is separate from the REST API — it runs as Django Channels with a Redis channel layer.

**WebSocket connection flow:**

1. Client connects to `wss://api.yourdomain.com/ws/match/{match_id}/`
2. Server verifies the match is live (or returns error if not)
3. Server immediately sends full current match state from Redis
4. Client stays connected and receives pushed events as they happen
5. If client sends a "seek" message (for historical replay scrubbing), server responds with state at that timestamp
6. On disconnect, server cleans up channel layer registration

**Messages the server pushes to clients:**
- `initial_state` — full current match state on first connect
- `match_event` — each new event as it arrives from Kafka
- `ball_update` — ball position frames for smooth animation
- `score_update` — when a goal is scored
- `match_status` — halftime, full time signals

**📚 Where to read:**

- Django Channels docs: `channels.readthedocs.io`
- WebSocket API (MDN): `developer.mozilla.org/en-US/docs/Web/API/WebSocket`
- Django Channels tutorial: `channels.readthedocs.io/en/stable/tutorial/index.html`
- Channel layers with Redis: `channels.readthedocs.io/en/stable/topics/channel_layers.html`

---

### Phase 12 End Result

✅ All REST endpoints implemented and documented at /docs
✅ WebSocket server broadcasting live match events to connected clients
✅ Rate limiting on all public endpoints
✅ JWT authentication on write endpoints
✅ Pydantic response models for all endpoints
✅ API response times under 200ms for all non-analytics endpoints

---

<a name="phase-13"></a>
## 🎨 PHASE 13 — Frontend Development

> **Goal:** Build the complete frontend — pitch tracker, World Cup archive, stadium maps, World Football Map, Legends career tabs, and analytics panels.

**Duration:** 6–8 weeks
**Phase Tech Skills:** Next.js 14, React, TypeScript, PixiJS, Mapbox GL JS, Recharts, Framer Motion, Tailwind CSS, HLS.js, WebSocket client, custom React hooks, i18n

---

### Step 13.1 — Next.js Project Setup & Architecture

**What to do:**

Use Next.js 14 with the App Router. Structure the frontend to match the three main sections of the platform.

**Route structure:**

```
/                         → Landing page
/world-cup                → WC tournament selector
/world-cup/[year]         → Tournament home: fixture list + stadium map + world map
/world-cup/[year]/[match] → Match view: tracker + stats + commentary
/legends/messi            → Messi career tab
/legends/messi/[matchNum] → Specific career match for Messi
/legends/ronaldo          → Ronaldo career tab
/legends/ronaldo/[matchNum] → Specific career match for Ronaldo
```

**Global layout components:**
- Top navigation bar with section links and active state
- Theme: dark background (deep gray/black), green pitch accents, white text
- Responsive: desktop-first but mobile-friendly

**📚 Where to read:**

- Next.js 14 App Router: `nextjs.org/docs/app`
- Next.js routing: `nextjs.org/docs/app/building-your-application/routing`
- TypeScript with Next.js: `nextjs.org/docs/app/building-your-application/configuring/typescript`
- Tailwind CSS: `tailwindcss.com/docs`

---

### Step 13.2 — Pitch Tracker Canvas (PixiJS)

**What to do:**

The top-down pitch tracker is the centerpiece of the entire platform. It renders at 60fps using PixiJS — a GPU-accelerated HTML5 canvas library. Regular SVG or React DOM elements cannot maintain 60fps with 22 moving circles plus ball animation.

**Components to build:**

**Pitch Background:**
- Green background in the exact shade of natural grass
- White pitch markings: outer boundary, center line, center circle, both penalty areas, both goal areas, penalty spots, corner arcs
- Goals drawn in gold at each end
- All markings use normalized coordinates — the pitch scales to any container size

**Player Circles:**
- One circle per player on pitch (11 home + 11 away = 22)
- Circle fill color = that team's kit primary color for this match
- Jersey number centered in the circle in white or black (whichever has better contrast against the kit color)
- Circle outline in black for visibility against the pitch background
- Player name tooltip on hover

**Ball:**
- Slightly smaller circle than players
- White with black outline
- Z-height simulated by scaling the circle slightly larger when the ball is high in the air (3D illusion on 2D pitch)
- Shadow circle on the pitch ground when ball is airborne (depth cue)

**Movement:**
- All positions updated via LERP (linear interpolation) on every frame — not teleporting to new position
- Ball follows pre-computed Bezier/parabolic path for passes and carries

**📚 Where to read:**

- PixiJS v8 docs: `pixijs.com/8.x/guides`
- `@pixi/react` library: `pixijs.io/pixi-react`
- LERP explained: search "game development LERP smooth movement" on YouTube
- PixiJS performance tips: `pixijs.com/8.x/guides/production/performance-tips`

---

### Step 13.3 — Stadium Map (Mapbox GL JS)

**What to do:**

Each World Cup tournament page shows a map of the host country with all official stadiums pinned. Clicking a stadium pin shows the stadium name, city, capacity, and a list of matches played there.

**Map features to build:**

- Map centered and zoomed to the host country automatically based on tournament year
- Custom football-shaped markers at each stadium latitude/longitude
- Marker color changes if that stadium hosted the Final (gold) vs regular matches (white/green)
- Clicking a marker opens a popup showing stadium metadata and match list
- Map style: dark theme to match the overall platform aesthetic (Mapbox provides dark style presets)

**For multi-country hosts (WC 2002 Korea/Japan, WC 2026 USA/Mexico/Canada):**
- Map zoomed out to show all host countries
- Country boundaries highlighted

**📚 Where to read:**

- Mapbox GL JS docs: `docs.mapbox.com/mapbox-gl-js/api`
- Mapbox custom markers: `docs.mapbox.com/mapbox-gl-js/example/custom-marker-icons`
- Mapbox popups: `docs.mapbox.com/mapbox-gl-js/example/popup`
- Mapbox dark style: `docs.mapbox.com/api/maps/styles`
- Mapbox free tier: 50,000 free map loads per month — sufficient for this project

---

### Step 13.4 — World Cup World Map

**What to do:**

Each tournament page also shows a world map with every participating country colored by their result in that tournament. This is a choropleth map — each country polygon is filled with a color corresponding to their best result.

**Color scheme (per result tier):**

- 🏆 Tournament Winner → Gold
- 🥈 Runner Up → Silver
- 🥉 Third Place → Bronze
- Semi-Final → Dark green
- Quarter-Final → Medium green
- Round of 16 → Light green
- Group Stage exit → Very light green / teal
- Did not qualify → Dark gray
- Did not exist as country → Lighter gray

**Technical approach:**

- Use GeoJSON world country boundaries (available free from Natural Earth: `naturalearthdata.com`)
- Load boundaries into Mapbox as a fill layer
- Color each country polygon based on `mart_world_map_by_tournament` ClickHouse data
- On hover: show country name + result tooltip
- This map is entirely different per tournament year — 2002 map looks very different from 2022

**📚 Where to read:**

- Natural Earth GeoJSON: `naturalearthdata.com` (free country boundaries)
- Mapbox choropleth tutorial: `docs.mapbox.com/mapbox-gl-js/example/choropleth-layer`
- GeoJSON format: `geojson.org`

---

### Step 13.5 — Legends Career Tab (Messi / Ronaldo)

**What to do:**

Build the career timeline interface for each legend. It is a full-section experience — career match list on one side, selected match detail on the other.

**Career tab layout:**

Left panel — Career Match List:
- Scrollable list of all career appearances, newest at top
- Each item shows: career match number, date, competition, opponent, result, goals, assists
- Color indicator: green dot if they scored, blue dot if they assisted, gray if no contribution
- Filter options: by competition (Champions League, La Liga, World Cup etc.), by year, by goal/assist only
- Click any match → loads match detail on the right panel

Right panel — Match Detail:
- Team badge + opponent badge + score
- Their personal stat card: minutes played, goals, assists, cards, estimated rating
- If goals/assists: embedded YouTube clip at exact goal/assist timestamp
- Pitch tracker replay (if event data available)
- Match summary stats panel (same as WC match stats)

**📚 Where to read:**

- YouTube IFrame API: `developers.google.com/youtube/iframe_api_reference`
- YouTube embed parameters: `developers.google.com/youtube/player_parameters`
- Framer Motion list animations: `motion.dev/docs/react-animation`
- React virtualization for long lists: `tanstack.com/virtual/latest` — use TanStack Virtual for the career list to avoid rendering 1,000+ DOM nodes

---

### Step 13.6 — Fixture Browser (World Cup Archive)

**What to do:**

The WC archive fixture browser shows all matches from MD1 to Final for a selected tournament. Navigation flows: WC Tab → Select Year → See Fixture Browser + Maps → Click Match → See Match Detail.

**Fixture browser components:**

- Round navigation tabs (MD1, MD2, MD3, R32, R16, QF, SF, 3rd, Final) — horizontally scrollable
- Fixture cards per round: home team + flag, score (or kickoff time if scheduled), away team + flag
- Status badges: LIVE (red pulsing), FT (gray), scheduled time
- Action badges: ▶ Replay (if animation ready), 📺 Watch (if YouTube embed available)
- Clicking a fixture card navigates to the full match view

**📚 Where to read:**

- Framer Motion AnimatePresence: `motion.dev/docs/react-animate-presence` — smooth tab switching
- Tailwind responsive: `tailwindcss.com/docs/responsive-design`

---

### Step 13.7 — Analytics Panel

**What to do:**

Below the pitch tracker on every match view is the analytics panel showing all match statistics. It displays in a tabbed layout — no swiping, just tab buttons.

**Tabs to build:**

- **Summary** — possession bar, pass accuracy, shots, shots on target, corners, fouls, cards — all as comparison bars between the two teams
- **xG Race** — line chart showing cumulative xG for both teams over 90 minutes
- **Shot Map** — mini pitch with dots at each shot location, sized by xG, colored by outcome
- **Pass Network** — nodes at average player positions, edges for passes between players, edge weight = number of passes
- **Heatmap** — team territory map showing where each team concentrated their actions

**Commentary panel (separate from stats):**

- Positioned alongside or below the analytics panel
- Language dropdown with 5 options: English 🇬🇧, Arabic 🇸🇦, Spanish 🇪🇸, Hindi 🇮🇳, Portuguese 🇧🇷
- Commentary lines appear in sequence synced to the match timeline
- RTL layout automatically applied when Arabic is selected

**📚 Where to read:**

- Recharts docs: `recharts.org` — for xG race and possession charts
- D3.js: `d3js.org` — for pass network graph (force-directed layout)
- mplsoccer Python: `mplsoccer.readthedocs.io` — for server-side shot map generation
- MDN `dir` attribute: `developer.mozilla.org/en-US/docs/Web/HTML/Global_attributes/dir` — for RTL Arabic

---

### Step 13.8 — YouTube Clip Player

**What to do:**

For goal and assist clips in the Legends tab and WC match view, embed YouTube videos directly at the curated start/end timestamps.

**Implementation:**

- Use YouTube IFrame API (not just iframe embed tag) for programmatic control
- When a goal event is clicked, load the associated video and seek to `start_second` automatically
- YouTube player is hidden until a clip is actively requested (lazy loading)
- For the Goal Library section: clips autoplay when the goal dot is clicked on the pitch tracker timeline

**📚 Where to read:**

- YouTube IFrame Player API: `developers.google.com/youtube/iframe_api_reference`
- Lazy loading in React: `react.dev/reference/react/lazy`

---

### Phase 13 End Result

✅ Pitch tracker rendering at 60fps with all 22 players and ball
✅ Player circles in correct jersey colors with jersey numbers
✅ Stadium map working for all WC years with correct pins
✅ World Cup World Map choropleth showing results per tournament
✅ WC fixture browser navigable from MD1 to Final
✅ Messi and Ronaldo career tabs fully functional
✅ YouTube clips loading at correct timestamps for goals/assists
✅ Analytics panel with all stat types in tabbed layout
✅ Commentary panel with 5-language selector and RTL support

---

<a name="phase-14"></a>
## 🚀 PHASE 14 — DevOps, Deployment & Monitoring

> **Goal:** Containerize everything, deploy to production, set up auto-scaling, and monitor the full platform.

**Duration:** 2–3 weeks
**Phase Tech Skills:** Docker, Docker Compose, Kubernetes, Helm charts, Terraform, GitHub Actions, Nginx, Prometheus, Grafana, alerting, log aggregation

---

### Step 14.1 — Docker Containerization

**What to do:**

Every service gets its own Dockerfile. Docker Compose orchestrates them locally. Kubernetes orchestrates them in production.

**Services to containerize:**

- Frontend (Next.js) — Node.js base image
- Backend API (FastAPI) — Python slim base image
- WebSocket server (Django Channels) — Python slim base image
- Ingestion service — Python slim base image
- Flink job runner — official Apache Flink image
- Airflow — official Apache Airflow image
- PostgreSQL — official postgres:16 image
- Redis — official redis:7.2-alpine image
- ClickHouse — official clickhouse/clickhouse-server image
- Kafka — Confluent cp-kafka image
- Prometheus — official prom/prometheus image
- Grafana — official grafana/grafana image

**Key containerization rules:**

- Never hardcode credentials in Dockerfiles or compose files — use environment variables and Docker secrets
- Use multi-stage builds for production images to minimize image size
- Pin every image to a specific version tag — never use `latest` in production

**📚 Where to read:**

- Docker multi-stage builds: `docs.docker.com/build/building/multi-stage`
- Docker best practices: `docs.docker.com/develop/develop-images/guidelines`
- Docker Compose production: `docs.docker.com/compose/production`

---

### Step 14.2 — GitHub Actions CI/CD

**What to do:**

Set up automated pipelines that run on every code push and every merge to main.

**CI pipeline (on every pull request):**

1. Run Python unit tests for all backend services
2. Run dbt tests against a test ClickHouse instance
3. Run Great Expectations checkpoints against sample data
4. Build Docker images and verify they build without errors
5. Run frontend build and type check

**CD pipeline (on merge to main):**

1. Build and push Docker images to container registry (Docker Hub or GitHub Container Registry)
2. Run Terraform plan to show any infrastructure changes
3. Deploy to staging environment
4. Run smoke tests against staging
5. If passing: deploy to production with rolling update

**📚 Where to read:**

- GitHub Actions docs: `docs.github.com/en/actions`
- GitHub Actions for Docker: `docs.github.com/en/actions/publishing-packages/publishing-docker-images`
- Terraform docs: `developer.hashicorp.com/terraform/docs`

---

### Step 14.3 — Prometheus + Grafana Monitoring

**What to do:**

Instrument every service with Prometheus metrics. Build Grafana dashboards to visualize health at a glance.

**Metrics to track per service:**

- FastAPI: request rate, response time (p50/p95/p99), error rate per endpoint
- WebSocket server: active connection count, message throughput
- Kafka: consumer lag per topic (CRITICAL — must stay near zero for live feel)
- Flink jobs: events processed per second, processing latency
- PostgreSQL: query duration, active connections, cache hit ratio
- ClickHouse: query duration, rows read per second
- Redis: memory usage, cache hit ratio, key count

**Alerts to configure (non-negotiable):**

- Kafka consumer lag exceeds 500ms → alert immediately (live match is degraded)
- API error rate exceeds 1% → alert
- WebSocket server connections drop to zero during a live match → alert
- PostgreSQL connection pool exhausted → alert
- Any Flink job fails → alert

**📚 Where to read:**

- Prometheus docs: `prometheus.io/docs`
- Grafana docs: `grafana.com/docs/grafana/latest`
- "Site Reliability Engineering" by Google: `sre.google/sre-book/table-of-contents` — free online, chapters on monitoring and alerting
- Prometheus Python client: `prometheus.io/docs/instrumenting/clientlibs`

---

### Phase 14 End Result

✅ All services containerized and running in Docker Compose locally
✅ Kubernetes deployed to production with correct resource limits
✅ GitHub Actions CI running on every pull request
✅ GitHub Actions CD deploying on every merge to main
✅ Grafana dashboards live for all critical services
✅ Alerts configured for all critical failure conditions
✅ Kafka consumer lag monitoring confirmed working

---

## 📅 Realistic Timeline

| Phase | Name | Duration | Cumulative |
|---|---|---|---|
| 0 | Foundation & System Design | 2–3 weeks | 3 weeks |
| 1 | Data Source Strategy | 1 week | 4 weeks |
| 2 | Data Ingestion Layer | 3–4 weeks | 8 weeks |
| 3 | Real-Time Stream Processing | 3–4 weeks | 12 weeks |
| 4 | Batch Processing & Orchestration | 3–4 weeks | 16 weeks |
| 5 | Data Modeling | 2 weeks | 18 weeks |
| 6 | Data Transformation (dbt) | 2–3 weeks | 21 weeks |
| 7 | Analytical Storage (ClickHouse) | 1–2 weeks | 23 weeks |
| 8 | Data Quality | 1–2 weeks | 25 weeks |
| 9 | Caching & Live State | 1 week | 26 weeks |
| 10 | Object Storage & CDN | 1 week | 27 weeks |
| 11 | Tracking Algorithm Engine | 3–5 weeks | 32 weeks |
| 12 | Backend API Layer | 3–4 weeks | 36 weeks |
| 13 | Frontend Development | 6–8 weeks | 44 weeks |
| 14 | DevOps & Monitoring | 2–3 weeks | 47 weeks |

**Solo developer full-time: ~10–12 months**
**Team of 3 (1 DE + 1 backend + 1 frontend): ~5–6 months**

---

## 📚 Master Learning Resource List

### Books — Read These in This Order

| # | Book | Why | When to Read |
|---|---|---|---|
| 1 | "Designing Data-Intensive Applications" — Martin Kleppmann | THE most important book for this stack. Covers Kafka, stream processing, databases | Before Phase 3 |
| 2 | "Kafka: The Definitive Guide" — Neha Narkhede (free PDF from Confluent) | Deep Kafka understanding | Before Phase 3 |
| 3 | "Data Pipelines with Apache Airflow" — Bas Harenslak | Complete Airflow reference | Before Phase 4 |
| 4 | "PostgreSQL: Up and Running" — Regina Obe | Practical PostgreSQL | Before Phase 5 |
| 5 | "The Analytics Engineering Guide" — dbt Labs (free online) | dbt patterns and philosophy | Before Phase 6 |
| 6 | "Soccermatics" — David Sumpter | Football analytics context | Anytime |

### YouTube Channels

- **Friends of Tracking** (`youtube.com/@friendsoftracking`) — best football data science channel, entire playlist essential
- **TechWorld with Nana** — Docker, Kubernetes, CI/CD tutorials
- **ArjanCodes** — Python software architecture
- **Fiora (dbt)** — dbt tutorial series
- **Fireship** — Fast Next.js + WebSocket tutorials

### Free Online Courses

| Course | Platform | For Which Phase |
|---|---|---|
| Apache Kafka fundamentals | `developer.confluent.io/courses` | Phase 3 |
| dbt Fundamentals | `courses.getdbt.com` | Phase 6 |
| FastAPI full tutorial | `fastapi.tiangolo.com/tutorial` | Phase 12 |
| Next.js official tutorial | `nextjs.org/learn` | Phase 13 |
| PixiJS getting started | `pixijs.com/8.x/guides/basics` | Phase 13 |
| Mapbox GL JS tutorials | `docs.mapbox.com/mapbox-gl-js/example` | Phase 13 |

### Key GitHub Repositories to Study

| Repository | Why Study It |
|---|---|
| `github.com/statsbomb/open-data` | Real World Cup event data to practice with |
| `github.com/statsbomb/statsbombpy` | Python library for loading StatsBomb data |
| `github.com/Friends-of-Tracking-Data-FoTDA/LaurieOnTracking` | Direct ball/player tracking algorithm implementations |
| `github.com/andrewRowlinson/mplsoccer` | Football pitch visualization in Python |
| `github.com/openfootball/world-cup` | Free historical WC data |

---

## 🚫 Non-Negotiables — Never Skip These

### Data & Legal

- **Never store raw Sportradar/Opta data without a commercial license.** Use it, transform it, store your derived data — do not redistribute their raw feeds.
- **Transfermarkt scraping**: run only at low frequency, never re-scrape if data is already stored. Respect their `robots.txt`.
- **YouTube embeds are legal.** Downloading and re-hosting YouTube videos is not. Only use embeds.
- **Self-counted stats disclaimer:** add a small note in the UI that stats are derived from tracked match events, not official federation records.

### Architecture

- **Never query PostgreSQL directly from WebSocket handlers.** Live state always comes from Redis. PostgreSQL is for persistence, Redis is for real-time serving.
- **Never transform data in the ingestion layer.** Raw data written to disk first, transformation always downstream.
- **Never serve uncompressed animation files.** Always gzip. Uncompressed will kill your bandwidth.
- **Normalize all pitch coordinates to 0–1 in the backend.** The frontend never does coordinate conversion.

### Frontend

- **Never use `localStorage` or `sessionStorage` in Next.js components.** They cause SSR hydration errors.
- **Always use LERP for position updates**, never teleport. A player jumping from one side of the pitch to another instantly destroys the experience.
- **Kit color conflict detection is mandatory.** If home and away kit colors are too similar, switch the away team to their alternate kit automatically. Never show two identical-looking teams on the pitch.

### Performance

- **Kafka consumer lag must stay below 500ms** during live matches — monitor this actively
- **Pitch tracker must maintain 60fps** — profile with Chrome DevTools if dropping frames
- **ClickHouse analytics queries must respond under 500ms** — add materialized views if they don't
- **All Airflow DAGs must have retry logic and failure alerts** — a silently failed DAG means missing data

### Career Data Integrity

- **Career match numbers are computed dynamically** (via SQL window function), never hardcoded. Adding a new match automatically adjusts all numbers correctly.
- **Stats are counted from your event data only.** Do not mix counts from your pipeline with counts from external sources — they will contradict each other and confuse users.

---

> *This document is the complete, final, definitive specification. Build in phase order. Never skip a phase. The pipeline only works as a whole system — each phase depends on the previous.*
