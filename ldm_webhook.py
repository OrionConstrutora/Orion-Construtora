"""
Webhook Kommo CRM + Sofia (LDM Incorporadora)

Fluxo:
  Cliente envia mensagem no WhatsApp
  → Kommo dispara webhook para /webhook
  → Sofia qualifica o lead e agenda visita
  → Resposta enviada via Meta WhatsApp API

Variáveis de ambiente:
  ANTHROPIC_API_KEY
  KOMMO_SUBDOMAIN      → ldmincorporadora
  KOMMO_TOKEN          → access token OAuth
  WHATSAPP_TOKEN       → token permanente Meta
  WHATSAPP_PHONE_ID    → ID do número Meta
  KOMMO_WEBHOOK_SECRET → token secreto (opcional)
"""

import os, json, time, asyncio, logging
import httpx
import anthropic
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("ldm-sofia")

app = FastAPI(title="Sofia — LDM Incorporadora")

# CORS — permite chamadas do quiz (construtoraorion.com)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://construtoraorion.com", "https://quiz-orion-lrq9-production.up.railway.app"],
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["*"],
)

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

KOMMO_SUBDOMAIN   = os.environ["KOMMO_SUBDOMAIN"]
KOMMO_TOKEN       = os.environ["KOMMO_TOKEN"]
KOMMO_SECRET      = os.environ.get("KOMMO_WEBHOOK_SECRET", "")
WHATSAPP_TOKEN    = os.environ.get("WHATSAPP_TOKEN", "")
WHATSAPP_PHONE_ID = os.environ.get("WHATSAPP_PHONE_ID", "")

KOMMO_API = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4"

# IDs do agente
with open("ldm_ids.json") as f:
    _ids = json.load(f)

SOFIA_ID = _ids["sofia_id"]

# Sessões por talk_id
_sessoes: dict[str, str] = {}
_telefones: dict[str, str] = {}

# Dedup de mensagens
_processadas: dict[str, float] = {}
_DEDUP_TTL = 60.0


def _e_duplicada(talk_id: str, msg_id: str, texto: str) -> bool:
    agora = time.time()
    chave = f"id:{msg_id}" if msg_id else f"{talk_id}:{hash(texto)}:{int(agora / 4)}"
    ttl   = _DEDUP_TTL if msg_id else 4.0
    if chave in _processadas and agora - _processadas[chave] < ttl:
        return True
    _processadas[chave] = agora
    expirados = [k for k, t in _processadas.items() if agora - t > 120]
    for k in expirados:
        del _processadas[k]
    return False


# ---------------------------------------------------------------------------
# Sessões
# ---------------------------------------------------------------------------
def _criar_sessao(talk_id: str) -> str:
    sessao = client.beta.sessions.create(
        agent=SOFIA_ID,
        title=f"LDM Talk {talk_id}",
    )
    log.info(f"Nova sessão para talk {talk_id}: {sessao.id}")
    return sessao.id


def _obter_sessao(talk_id: str) -> str:
    if talk_id not in _sessoes:
        _sessoes[talk_id] = _criar_sessao(talk_id)
    return _sessoes[talk_id]


def _reiniciar_sessao(talk_id: str) -> str:
    log.warning(f"Reiniciando sessão para talk {talk_id}")
    _sessoes[talk_id] = _criar_sessao(talk_id)
    return _sessoes[talk_id]


# ---------------------------------------------------------------------------
# Comunicação com Sofia
# ---------------------------------------------------------------------------
def _perguntar_sofia(sessao_id: str, mensagem: str) -> str:
    resposta = ""
    processando = False
    with client.beta.sessions.events.stream(session_id=sessao_id) as stream:
        client.beta.sessions.events.send(
            session_id=sessao_id,
            events=[{
                "type": "user.message",
                "content": [{"type": "text", "text": mensagem}],
            }],
        )
        for evento in stream:
            if evento.type in ("session.status_processing", "agent.message"):
                processando = True
            if evento.type == "agent.message":
                for bloco in evento.content:
                    if bloco.type == "text":
                        resposta += bloco.text
            elif evento.type == "session.status_terminated":
                break
            elif (
                evento.type == "session.status_idle"
                and processando
                and getattr(getattr(evento, "stop_reason", None), "type", None) != "requires_action"
            ):
                break
    return resposta


# ---------------------------------------------------------------------------
# Envio via Meta WhatsApp API
# ---------------------------------------------------------------------------
async def _enviar_whatsapp(telefone: str, texto: str) -> bool:
    if not WHATSAPP_TOKEN or not WHATSAPP_PHONE_ID:
        return False
    tel = telefone.replace("+", "").replace(" ", "").replace("-", "")
    url = f"https://graph.facebook.com/v20.0/{WHATSAPP_PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": tel,
        "type": "text",
        "text": {"preview_url": False, "body": texto},
    }
    async with httpx.AsyncClient(timeout=12.0) as http:
        resp = await http.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            log.info(f"✅ Enviado via Meta API para {tel}")
            return True
        log.error(f"❌ Meta API ({resp.status_code}): {resp.text[:200]}")
        return False


# ---------------------------------------------------------------------------
# Nota interna no lead
# ---------------------------------------------------------------------------
async def _nota_lead(lead_id: str, texto: str):
    if not lead_id or lead_id in ("None", "0", ""):
        return
    url = f"{KOMMO_API}/leads/{lead_id}/notes"
    headers = {
        "Authorization": f"Bearer {KOMMO_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = [{"note_type": "common", "params": {"text": f"🤖 *Sofia*:\n\n{texto}"}}]
    async with httpx.AsyncClient(timeout=10.0) as http:
        await http.post(url, json=payload, headers=headers)


# ---------------------------------------------------------------------------
# Busca telefone do contato
# ---------------------------------------------------------------------------
async def _telefone_do_talk(talk_id: str) -> str:
    if talk_id in _telefones:
        return _telefones[talk_id]
    headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
    async with httpx.AsyncClient(timeout=8.0) as http:
        r = await http.get(f"{KOMMO_API}/talks/{talk_id}", headers=headers)
        if r.status_code != 200:
            return ""
        contact_id = str(r.json().get("contact_id", ""))
        if not contact_id:
            return ""
        rc = await http.get(f"{KOMMO_API}/contacts/{contact_id}", headers=headers)
        if rc.status_code != 200:
            return ""
        for field in rc.json().get("custom_fields_values", []) or []:
            if field.get("field_type") in ("phone", "multitext"):
                vals = field.get("values", [])
                if vals:
                    tel = str(vals[0].get("value", "")).strip()
                    _telefones[talk_id] = tel
                    return tel
    return ""


# ---------------------------------------------------------------------------
# Envio inteligente
# ---------------------------------------------------------------------------
async def _enviar_resposta(talk_id: str, lead_id: str | None, texto: str):
    if WHATSAPP_TOKEN and WHATSAPP_PHONE_ID:
        telefone = await _telefone_do_talk(talk_id)
        if telefone:
            for i in range(0, len(texto), 4000):
                await _enviar_whatsapp(telefone, texto[i:i+4000])
                if i + 4000 < len(texto):
                    await asyncio.sleep(0.5)
            if lead_id and lead_id not in ("None", "0", ""):
                await _nota_lead(lead_id, texto)
            return

    if lead_id and lead_id not in ("None", "0", ""):
        await _nota_lead(lead_id, texto)
    else:
        log.error(f"❌ Sem mecanismo de envio para talk {talk_id}")


# ---------------------------------------------------------------------------
# Processamento principal
# ---------------------------------------------------------------------------
async def _processar(talk_id: str, lead_id: str | None, texto: str, msg_id: str = ""):
    if _e_duplicada(talk_id, msg_id, texto):
        log.debug(f"[talk:{talk_id}] Duplicada ignorada")
        return

    if texto == "__AUDIO__":
        await _enviar_resposta(talk_id, lead_id,
            "Olá! Recebi seu áudio, mas ainda não consigo ouvi-lo. "
            "Por favor, me envie sua mensagem em *texto*! 😊")
        return

    if texto in ("__MIDIA__", "__ARQUIVO__"):
        await _enviar_resposta(talk_id, lead_id,
            "Olá! Recebi seu arquivo. Me descreva em *texto* como posso ajudar! 😊")
        return

    log.info(f"[talk:{talk_id}] Recebido: {texto[:100]}")
    sessao_id = _obter_sessao(talk_id)

    try:
        resposta = await asyncio.to_thread(_perguntar_sofia, sessao_id, texto)
    except Exception as e:
        log.warning(f"Erro na sessão — recriando. Detalhe: {e}")
        sessao_id = _reiniciar_sessao(talk_id)
        try:
            resposta = await asyncio.to_thread(_perguntar_sofia, sessao_id, texto)
        except Exception as err:
            log.error(f"Falha definitiva para talk {talk_id}: {err}")
            await _enviar_resposta(talk_id, lead_id,
                "Olá! Estou com uma instabilidade. Nossa equipe entrará em contato em breve. 🙏")
            return

    if not resposta:
        log.warning(f"[talk:{talk_id}] Sofia retornou resposta vazia")
        return

    await _enviar_resposta(talk_id, lead_id, resposta)
    log.info(f"[talk:{talk_id}] Resposta enviada ({len(resposta)} chars)")


# ---------------------------------------------------------------------------
# Parser do payload Kommo
# ---------------------------------------------------------------------------
def _extrair_mensagens(dados: dict) -> list[dict]:
    mensagens = []
    msg_block = dados.get("message", {})
    if not isinstance(msg_block, dict):
        return mensagens

    adicionadas = msg_block.get("add", [])
    if isinstance(adicionadas, dict):
        adicionadas = list(adicionadas.values())

    for msg in adicionadas:
        author_type = msg.get("author", {}).get("type", "")
        if author_type not in ("external", "contact"):
            continue
        msg_type = msg.get("type", "")
        if author_type == "external" and msg_type not in ("incoming", ""):
            continue

        texto = msg.get("text", "").strip()
        if not texto:
            file_type    = str(msg.get("file_type", "")).lower()
            content_type = str(msg.get("content_type", "")).lower()
            attachment   = msg.get("attachment", {}) or {}
            attach_type  = str(attachment.get("type", "")).lower()
            tipo_raw     = " ".join([file_type, content_type, attach_type, msg_type])
            if any(t in tipo_raw for t in ("audio", "voice", "ptt", "ogg", "mp3", "m4a")):
                texto = "__AUDIO__"
            elif any(t in tipo_raw for t in ("image", "video", "sticker", "document", "gif")):
                texto = "__MIDIA__"
            elif attachment or file_type or content_type:
                texto = "__ARQUIVO__"
            else:
                continue

        mensagens.append({
            "talk_id":    str(msg.get("talk_id", "")),
            "lead_id":    str(msg.get("element_id", msg.get("lead_id", ""))),
            "text":       texto,
            "msg_id":     str(msg.get("id", "")),
        })

    return mensagens


async def _parse_request(request: Request) -> dict:
    content_type = request.headers.get("content-type", "")
    body = await request.body()
    if not body:
        return {}
    if "application/json" in content_type:
        try:
            return await request.json()
        except Exception:
            pass
    try:
        from urllib.parse import parse_qs
        raw = parse_qs(body.decode("utf-8", errors="replace"))
        dados: dict = {}
        for key, values in raw.items():
            value = values[0] if values else ""
            partes = key.replace("]", "").split("[")
            ref = dados
            for parte in partes[:-1]:
                ref = ref.setdefault(parte, {})
            ref[partes[-1]] = value
        return dados
    except Exception as e:
        log.warning(f"Erro ao parsear request: {e}")
        return {}


# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------
@app.post("/webhook")
async def receber_evento(request: Request):
    try:
        dados = await _parse_request(request)
    except Exception as e:
        log.warning(f"Erro ao parsear payload: {e}")
        return JSONResponse({"status": "parse_error"}, status_code=400)

    mensagens = _extrair_mensagens(dados)
    for msg in mensagens:
        if msg["talk_id"]:
            asyncio.create_task(
                _processar(msg["talk_id"], msg["lead_id"] or None, msg["text"], msg.get("msg_id", ""))
            )

    return JSONResponse({"status": "ok", "processadas": len(mensagens)})


@app.get("/webhook")
async def verificar():
    return JSONResponse({"status": "online", "agente": "Sofia — LDM Incorporadora"})


@app.get("/")
async def status():
    return {
        "status": "online",
        "agente": "Sofia",
        "sofia_id": SOFIA_ID,
        "kommo": f"{KOMMO_SUBDOMAIN}.kommo.com",
        "sessoes_ativas": len(_sessoes),
        "envio_meta_api": bool(WHATSAPP_TOKEN and WHATSAPP_PHONE_ID),
    }


# ---------------------------------------------------------------------------
# Endpoint /lead — recebe respostas do quiz e cria lead no Kommo
# ---------------------------------------------------------------------------
@app.post("/lead")
async def capturar_lead(request: Request):
    """
    Recebe dados do quiz e cria contato + lead no Kommo com todas as respostas.
    Payload esperado:
    {
        "nome": "João Silva",
        "telefone": "(92) 99999-9999",
        "tem_terreno": true/false,
        "renda_ok": true/false,
        "entrada_ok": true/false,
        "resultado": "QUALIFICADO" / "DESQUALIFICADO"
    }
    """
    try:
        dados = await request.json()
    except Exception:
        return JSONResponse({"erro": "JSON inválido"}, status_code=400)

    nome      = dados.get("nome", "Lead Quiz").strip()
    telefone  = dados.get("telefone", "").strip()
    terreno   = dados.get("tem_terreno")
    renda     = dados.get("renda_ok")
    entrada   = dados.get("entrada_ok")
    resultado = dados.get("resultado", "DESCONHECIDO")

    # Normaliza telefone para formato internacional
    tel_limpo = "".join(c for c in telefone if c.isdigit())
    if not tel_limpo.startswith("55"):
        tel_limpo = "55" + tel_limpo

    headers = {
        "Authorization": f"Bearer {KOMMO_TOKEN}",
        "Content-Type": "application/json",
    }

    emoji = "✅" if resultado == "QUALIFICADO" else "❌"
    tag_resultado = f"{emoji} {resultado}"

    # Nota com todas as respostas do quiz
    nota = (
        f"📋 *Respostas do Quiz — construtoraorion.com*\n\n"
        f"👤 Nome: {nome}\n"
        f"📱 Telefone: {telefone}\n\n"
        f"🏗️ Possui terreno: {'✅ Sim' if terreno else '❌ Não'}\n"
        f"💰 Renda ≥ R$15k/mês: {'✅ Sim' if renda else '❌ Não'}\n"
        f"💵 Tem entrada disponível: {'✅ Sim' if entrada else '❌ Não'}\n\n"
        f"🎯 Resultado: *{tag_resultado}*\n"
        f"🌐 Origem: Quiz construtoraorion.com"
    )

    async with httpx.AsyncClient(timeout=15.0) as http:
        # 1. Cria contato
        contato_payload = [{
            "name": nome,
            "custom_fields_values": [
                {"field_code": "PHONE", "values": [{"value": tel_limpo, "enum_code": "MOB"}]},
            ],
        }]
        r_contato = await http.post(f"{KOMMO_API}/contacts", json=contato_payload, headers=headers)
        contact_id = None
        if r_contato.status_code in (200, 201):
            contact_id = r_contato.json().get("_embedded", {}).get("contacts", [{}])[0].get("id")
            log.info(f"✅ Contato criado: {contact_id} ({nome})")
        else:
            log.warning(f"⚠️ Erro ao criar contato: {r_contato.status_code} {r_contato.text[:150]}")

        # 2. Cria lead
        lead_payload = [{
            "name": f"{tag_resultado} — {nome}",
            "pipeline_id": None,  # usa pipeline padrão
            "_embedded": {"contacts": [{"id": contact_id}]} if contact_id else {},
        }]
        r_lead = await http.post(f"{KOMMO_API}/leads", json=lead_payload, headers=headers)
        lead_id = None
        if r_lead.status_code in (200, 201):
            lead_id = r_lead.json().get("_embedded", {}).get("leads", [{}])[0].get("id")
            log.info(f"✅ Lead criado: {lead_id}")
        else:
            log.warning(f"⚠️ Erro ao criar lead: {r_lead.status_code} {r_lead.text[:150]}")

        # 3. Adiciona nota com respostas do quiz
        if lead_id:
            nota_payload = [{"note_type": "common", "params": {"text": nota}}]
            await http.post(f"{KOMMO_API}/leads/{lead_id}/notes", json=nota_payload, headers=headers)
            log.info(f"📝 Nota adicionada ao lead {lead_id}")

    return JSONResponse({
        "status": "ok",
        "resultado": resultado,
        "lead_id": lead_id,
        "contact_id": contact_id,
    })
