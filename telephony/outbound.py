"""
Outbound calling over Twilio: originate → media stream → streaming voice
pipeline (voice/pipeline.py — the same engine as the inbound platform).

Flow:
  1. POST /api/outbound-call {to_number, agent_id}
       → Twilio REST originates a call from TWILIO_PHONE_NUMBER.
  2. When the callee answers, Twilio requests POST /outbound-voice
       → TwiML <Connect><Stream> back to this server, carrying agent_id.
       Optional answering-machine detection: voicemail drop + hangup.
  3. WS /twilio-media-stream-out
       → CallSession with the sales agent's prompt (organization → team
         → agent context via utils.prompts.get_prompt) speaking first,
         with barge-in, transfer/end-call tools, and per-turn metrics.

Requires: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER,
PUBLIC_BASE_URL (Twilio must be able to reach the webhook — ngrok in dev).
Guard: if OUTBOUND_API_KEY is set, /api/outbound-call requires it in the
X-API-Key header.
"""

import asyncio
import base64
import json
import os
import time
import urllib.parse
from typing import Optional

import httpx
from fastapi import APIRouter, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from sql.database import SessionLocal
from sql import models
from voice.config import VoiceConfig
from voice.pipeline import CallSession, TwilioTransport

router = APIRouter()

TWILIO_API = "https://api.twilio.com/2010-04-01"

DEFAULT_SALES_PROMPT = (
    "You are a friendly outbound sales agent. Introduce yourself and the "
    "company, ask for a moment of the person's time, and keep every reply "
    "under 30 words. Use the end_call tool when the conversation is over "
    "or the person asks not to be called; use transfer_call if they want "
    "to speak with a human."
)


class OutboundCallRequest(BaseModel):
    to_number: str
    agent_id: int


def _twilio_creds():
    sid = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
    token = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
    from_number = os.getenv("TWILIO_PHONE_NUMBER", "").strip()
    if sid.startswith("replace-with"):
        sid = ""
    if token.startswith("replace-with"):
        token = ""
    if from_number.startswith("replace-with"):
        from_number = ""
    return sid, token, from_number


def _load_agent_context(agent_id: int) -> dict:
    """Resolve agent → team → organization and build the sales prompt."""
    db = SessionLocal()
    try:
        agent = db.query(models.Agent).filter(models.Agent.id == agent_id).first()
        if not agent:
            return {}
        team = agent.team
        organization = team.organization if team else None
        try:
            from utils.prompts import get_prompt
            system_prompt = get_prompt(organization, team, agent)
        except Exception as e:
            print(f"[outbound] prompt build failed ({e}); using default")
            system_prompt = DEFAULT_SALES_PROMPT
        greeting = (
            f"Hello! This is {agent.name} calling from "
            f"{organization.name if organization else 'our company'}. "
            f"Do you have a quick minute?"
        )
        return {
            "agent_id": agent.id,
            "team_id": team.id if team else None,
            "organization_id": organization.id if organization else None,
            "system_prompt": system_prompt,
            "greeting": greeting,
            "use_elevenlabs": bool(agent.use_elevenlabs),
            "voice_id": agent.voice_id,
        }
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────
# 1. Originate
# ─────────────────────────────────────────────────────────────────────────
@router.post("/api/outbound-call")
async def outbound_call(body: OutboundCallRequest,
                        x_api_key: Optional[str] = Header(default=None)):
    required_key = os.getenv("OUTBOUND_API_KEY", "").strip()
    if required_key and x_api_key != required_key:
        raise HTTPException(status_code=401, detail="Bad or missing X-API-Key.")

    sid, token, from_number = _twilio_creds()
    public_base = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
    if not (sid and token and from_number and public_base):
        raise HTTPException(status_code=503, detail=(
            "Outbound calling needs TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, "
            "TWILIO_PHONE_NUMBER and PUBLIC_BASE_URL."))

    context = await asyncio.to_thread(_load_agent_context, body.agent_id)
    if not context:
        raise HTTPException(status_code=404, detail="Agent not found.")

    webhook = (f"{public_base}/outbound-voice?"
               f"agent_id={body.agent_id}"
               f"&to_number={urllib.parse.quote(body.to_number)}")
    data = {
        "To": body.to_number,
        "From": from_number,
        "Url": webhook,
        "Method": "POST",
    }
    if os.getenv("MACHINE_DETECTION", "false").lower() in ("1", "true", "yes"):
        data["MachineDetection"] = "DetectMessageEnd"

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            f"{TWILIO_API}/Accounts/{sid}/Calls.json",
            data=data, auth=(sid, token))
    if resp.status_code >= 300:
        raise HTTPException(status_code=502,
                            detail=f"Twilio originate failed: {resp.text[:300]}")
    call = resp.json()
    print(f"[outbound] originated call {call.get('sid')} → {body.to_number}")
    return {"call_sid": call.get("sid"), "status": call.get("status")}


# ─────────────────────────────────────────────────────────────────────────
# 2. Answer webhook → media stream TwiML (with optional AMD handling)
# ─────────────────────────────────────────────────────────────────────────
@router.api_route("/outbound-voice", methods=["GET", "POST"])
async def outbound_voice(request: Request):
    params = dict(request.query_params)
    form = await request.form() if request.method == "POST" else {}
    answered_by = (form.get("AnsweredBy") or "").lower()

    from twilio.twiml.voice_response import VoiceResponse, Connect

    response = VoiceResponse()
    if answered_by.startswith("machine"):
        # Voicemail: leave a short message instead of talking to a beep.
        response.say(
            "Hello! Sorry we missed you. We'll try to reach you again "
            "at a better time. Goodbye.")
        response.hangup()
        return HTMLResponse(content=str(response), media_type="application/xml")

    host = request.headers.get("host", "localhost:8000")
    scheme = ("wss" if request.headers.get("x-forwarded-proto") == "https"
              or request.url.scheme == "https" else "ws")
    agent_id = params.get("agent_id", "")
    to_number = urllib.parse.quote(params.get("to_number", ""))
    ws_url = (f"{scheme}://{host}/twilio-media-stream-out"
              f"?agent_id={agent_id}&to_number={to_number}")

    connect = Connect()
    connect.stream(url=ws_url)
    response.append(connect)
    return HTMLResponse(content=str(response), media_type="application/xml")


# ─────────────────────────────────────────────────────────────────────────
# 3. Media stream — same engine as inbound, sales-agent flavored
# ─────────────────────────────────────────────────────────────────────────
@router.websocket("/twilio-media-stream-out")
async def twilio_media_stream_out(websocket: WebSocket,
                                  agent_id: Optional[int] = None,
                                  to_number: Optional[str] = None):
    await websocket.accept()
    print(f"[outbound] media stream connected (agent={agent_id}, to={to_number})")

    config = VoiceConfig.from_env()
    if not config.streaming_ready:
        print("[outbound] missing DEEPGRAM_API_KEY / LLM key — cannot run "
              "streaming pipeline; closing.")
        await websocket.close()
        return

    stream_sid = None
    call_sid = ""
    session: Optional[CallSession] = None
    session_task = None
    call_start = None
    context: dict = {}

    async def handle_start(message: dict):
        nonlocal stream_sid, call_sid, session, session_task, call_start, context
        start = message.get("start", {})
        stream_sid = start.get("streamSid") or message.get("streamSid")
        call_sid = start.get("callSid", "")
        call_start = time.time()

        context = await asyncio.to_thread(_load_agent_context, agent_id or 0)
        config.system_prompt = context.get("system_prompt", DEFAULT_SALES_PROMPT)
        config.greeting = context.get(
            "greeting", "Hello! Do you have a quick minute?")
        if context.get("use_elevenlabs") and config.elevenlabs_api_key:
            config.tts_provider = "elevenlabs"
            if context.get("voice_id") and context["voice_id"] != "None":
                config.elevenlabs_voice_id = context["voice_id"]

        transport = TwilioTransport(websocket, stream_sid)
        session = CallSession(config, transport, call_sid=call_sid,
                              caller_number=to_number or "")
        session_task = asyncio.create_task(session.run())

    receive_task = asyncio.create_task(websocket.receive_text())
    ended_task = None
    try:
        while True:
            wait_for = {receive_task}
            if session is not None and ended_task is None:
                ended_task = asyncio.create_task(session.ended.wait())
            if ended_task is not None:
                wait_for.add(ended_task)
            done, _ = await asyncio.wait(wait_for,
                                         return_when=asyncio.FIRST_COMPLETED)
            if ended_task is not None and ended_task in done:
                break
            data = receive_task.result()
            message = json.loads(data)
            event = message.get("event")
            if event == "media":
                if session is not None:
                    await session.feed_audio(
                        base64.b64decode(message["media"]["payload"]))
            elif event == "mark":
                if session is not None:
                    session.on_mark(message.get("mark", {}).get("name", ""))
            elif event == "start":
                await handle_start(message)
            elif event == "stop":
                break
            receive_task = asyncio.create_task(websocket.receive_text())
    except WebSocketDisconnect:
        print("[outbound] media stream disconnected")
    except Exception as e:
        print(f"[outbound] media stream error: {e}")
    finally:
        receive_task.cancel()
        if ended_task is not None:
            ended_task.cancel()
        if session is not None:
            await session.shutdown()
        if session_task is not None:
            session_task.cancel()

        if session is not None and call_start is not None and context:
            duration = int(time.time() - call_start)
            metrics_dict = session.metrics.to_dict()
            print(session.metrics.log_line())
            await asyncio.to_thread(
                _persist_call, context, session.messages, metrics_dict, duration)

        try:
            await websocket.close()
        except Exception:
            pass


def _persist_call(context: dict, messages: list, metrics_dict: dict,
                  duration_seconds: int):
    if not (context.get("organization_id") and context.get("team_id")
            and context.get("agent_id")):
        return
    db = SessionLocal()
    try:
        record = models.ChatHistory(
            organization_id=context["organization_id"],
            team_id=context["team_id"],
            agent_id=context["agent_id"],
            chat_data=[m for m in messages if m.get("role") != "system"],
        )
        summary = metrics_dict.get("summary") or {}
        if summary.get("response_ms_p50") is not None:
            record.response_time = summary["response_ms_p50"] / 1000.0
        if hasattr(record, "metrics"):
            record.metrics = metrics_dict
        db.add(record)
        db.commit()
        print(f"[outbound] call persisted ({duration_seconds}s, "
              f"{summary.get('turn_count', 0)} turns)")
    except Exception as e:
        print(f"[outbound] persist failed: {e}")
        db.rollback()
    finally:
        db.close()
