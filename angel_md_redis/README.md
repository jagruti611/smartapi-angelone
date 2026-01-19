## Angel One Market Data → Redis Streams (WS + REST Greeks)

### 1) Start Redis
docker compose up -d

### 2) Setup python venv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

### 3) Configure
- Copy .env.example to .env and fill values
- Put all your symbols in symbols.txt (one per line)

### 4) Run producer (WebSocket → Redis)
python run_producer.py

Streams written:
- md:ticks:eq
- md:ticks:opt

### 5) Run greeks poller (REST → Redis)
Option A (simple): run combined WS+greeks:
python run_greeks.py

Writes:
- md:greeks:snap
and cache key:
- md:greeks:latest:{UNDERLYING}:{EXPIRY_ISO}

### 6) Run joiner (ticks + latest greeks → training stream)
python run_joiner.py

Writes:
- md:features:opt


docker compose -f docker-compose.yml up -d
