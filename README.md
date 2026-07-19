# VoxReach — AI Voice Sales Agent

VoxReach is a voice-first outbound engagement platform that combines speech recognition, LLM reasoning, and speech synthesis into a single real-time calling workflow.

It is built for teams that need configurable AI call agents, centralized campaign context, and operational analytics without stitching multiple services by hand.

## What It Delivers

- Multi-tenant account model with organizations, teams, and agents
- WebSocket-based real-time voice conversation loop
- Pluggable STT, LLM, and TTS backends
- Agent memory/context handling per conversation
- Campaign context generation from crawled site content
- Analytics dashboard for call volume and response-time trends

## Technical Stack

- Backend: FastAPI, Starlette sessions, SQLAlchemy
- Realtime transport: WebSockets
- Database: SQLite by default (via SQLAlchemy)
- AI components:
	- STT: Faster-Whisper / Deepgram / HF adapters
	- LLM: OpenAI-compatible endpoints and local adapters
	- TTS: ElevenLabs, XTTS, Piper, and other adapter modules
- Crawler service: Embedded TypeScript crawler module in `gpt-crawler/`

## Architecture Overview

1. Browser client streams audio to `/chatws`.
2. STT adapter transcribes incremental user speech.
3. LLM adapter generates response tokens using agent prompt context.
4. TTS adapter synthesizes response audio chunks.
5. Audio is streamed back to the browser for low-latency playback.
6. Conversation metadata is persisted for history and analytics.

## Repository Layout

- `app.py`: main FastAPI application and orchestration layer
- `sql/`: ORM models, CRUD helpers, schema definitions
- `openvoicechat/`: STT/LLM/TTS adapters and runtime utilities
- `templates/` and `static/`: web UI views and assets
- `gpt-crawler/`: website crawling pipeline used for knowledge ingestion
- `utils/`: auth, logging, cookie/session helpers, prompt utilities

## Quick Start

### 1. Clone and prepare environment

```bash
git clone https://github.com/MuhammadAamirGulzar/ai-voice-sales-agent.git
cd ai-voice-sales-agent
python -m venv .venv
```

On Windows:

```bash
.venv\Scripts\activate
```

On Linux/macOS:

```bash
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Fill required values for JWT/session keys and provider credentials.

### 4. Run the API

```bash
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

Open `http://localhost:8000`.

## Security Notes

- Passwords are stored as hashed values, not plaintext.
- Do not commit `.env`, certificates, generated audio, or crawler output.
- Rotate all keys before any public deployment.

## Deployment Notes

- Replace SQLite with managed Postgres for production workloads.
- Terminate TLS at a reverse proxy (Nginx, Caddy, or cloud LB).
- Run workers and API separately if scaling concurrent calls.
- Add centralized logs/metrics for call latency and model failures.

## License

This project is licensed under Creative Commons Attribution-NonCommercial 4.0 International. See `LICENSE`.
