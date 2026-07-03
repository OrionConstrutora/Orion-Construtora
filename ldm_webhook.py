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

import os, json, time, asyncio, logging, hashlib
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

# ── Meta CAPI ─────────────────────────────────────────────
META_PIXEL_ID    = os.environ.get("META_PIXEL_ID",    "1327948448428782")
META_CAPI_TOKEN  = os.environ.get("META_CAPI_TOKEN",  "")
META_CAPI_URL    = f"https://graph.facebook.com/v20.0/{META_PIXEL_ID}/events"
# Page ID da conta Meta (Facebook Page vinculada ao WhatsApp Business)
META_PAGE_ID     = os.environ.get("META_PAGE_ID",     "100627972913181")
# Talks já processados nesta sessão (para detectar novas conversas)
_talks_novos: set[str] = set()
# Talks que vieram de anúncio CTWA — Sofia não responde, João atende manualmente
_talks_anuncio: set[str] = set()
# Telefones que clicaram em anúncio CTWA → rastreia quem salvou o contato e voltou depois
# Formato: { "5592999999999": {"ctwa_clid": "...", "source_id": "...", "ts": 1234567890} }
_telefones_anuncio: dict[str, dict] = {}

# IDs do agente
with open("ldm_ids.json") as f:
    _ids = json.load(f)

SOFIA_ID        = _ids["sofia_id"]
ENVIRONMENT_ID  = _ids["environment_id"]

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
        environment_id=ENVIRONMENT_ID,
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
# Meta CAPI — funções auxiliares
# ---------------------------------------------------------------------------
def _sha256(value: str) -> str:
    """SHA-256 de uma string normalizada (lowercase, sem espaços extras)."""
    return hashlib.sha256(value.strip().lower().encode()).hexdigest()

def _normalizar_tel(tel: str) -> str:
    d = "".join(c for c in tel if c.isdigit())
    return d if d.startswith("55") else "55" + d

def _normalizar_nome(nome: str):
    """Retorna (first_name, last_name) normalizados."""
    import unicodedata
    def sem_acento(s):
        return "".join(
            c for c in unicodedata.normalize("NFD", s)
            if unicodedata.category(c) != "Mn"
        ).lower().strip()
    partes = nome.strip().split()
    fn = sem_acento(partes[0]) if partes else ""
    ln = sem_acento(" ".join(partes[1:])) if len(partes) > 1 else fn
    return fn, ln

async def _enviar_capi(
    event_name: str,
    event_id: str,
    telefone: str,
    nome: str,
    email: str = "",
    external_id: str = "",
    fbc: str = "",
    fbp: str = "",
    client_ip: str = "",
    user_agent: str = "",
    event_source_url: str = "https://construtoraorion.com/",
    custom_data: dict | None = None,
) -> bool:
    """Envia evento para Meta Conversions API (server-side)."""
    if not META_CAPI_TOKEN:
        log.warning("META_CAPI_TOKEN não configurado — CAPI ignorado")
        return False

    tel    = _normalizar_tel(telefone) if telefone else ""
    fn, ln = _normalizar_nome(nome) if nome else ("", "")
    ext_id = _normalizar_tel(telefone) if not external_id else external_id

    user_data: dict = {}
    if tel:
        user_data["ph"] = [_sha256(tel)]
    if fn:
        user_data["fn"] = [_sha256(fn)]
    if ln:
        user_data["ln"] = [_sha256(ln)]
    # Email — campo de maior peso no EMQ após phone
    if email:
        em = email.strip().lower()
        if "@" in em:
            user_data["em"] = [_sha256(em)]
    if ext_id:
        user_data["external_id"] = [_sha256(ext_id)]
    if fbc:
        user_data["fbc"] = fbc
    if fbp:
        user_data["fbp"] = fbp
    if client_ip:
        user_data["client_ip_address"] = client_ip
    if user_agent:
        user_data["client_user_agent"] = user_agent

    payload = {
        "data": [{
            "event_name"      : event_name,
            "event_time"      : int(time.time()),
            "event_id"        : event_id,
            "event_source_url": event_source_url,
            "action_source"   : "website",
            "user_data"       : user_data,
        }],
        "access_token": META_CAPI_TOKEN,
    }
    if custom_data:
        payload["data"][0]["custom_data"] = custom_data

    try:
        async with httpx.AsyncClient(timeout=10.0) as http:
            r = await http.post(META_CAPI_URL, json=payload)
            if r.status_code == 200:
                log.info(f"📡 CAPI [{event_name}] enviado ✅ event_id={event_id[:8]}...")
                return True
            else:
                log.warning(f"⚠️ CAPI [{event_name}] erro {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.error(f"❌ CAPI erro: {e}")
    return False


# ---------------------------------------------------------------------------
# CAPI — Evento de nova conversa WhatsApp (messaging_conversation_started_7d)
# Disparado quando um lead inicia conversa pela 1ª vez — atribui ao anúncio
# ---------------------------------------------------------------------------
async def _capi_nova_conversa_whatsapp(telefone: str, talk_id: str, ctwa_clid: str = "") -> bool:
    """
    Envia evento CAPI para nova conversa WhatsApp.

    Dois cenários:
    1. COM ctwa_clid (veio de anúncio CTWA):
       → LeadSubmitted + action_source=business_messaging
       → Atribuição direta ao anúncio — evento mais valioso
    2. SEM ctwa_clid (orgânico / link direto):
       → Lead + action_source=website com phone
       → Ainda enriquece o EMQ e permite remarketing

    Parâmetros:
        telefone  : número E.164 do lead (ex: 5592999999999)
        talk_id   : ID do talk no Kommo (usado como event_id)
        ctwa_clid : Click-to-WhatsApp ID (obrigatório para evento CTWA)
    """
    if not META_CAPI_TOKEN:
        log.warning("META_CAPI_TOKEN não configurado — CAPI WhatsApp ignorado")
        return False
    if not telefone:
        log.warning(f"[talk:{talk_id}] Sem telefone — CAPI WhatsApp não disparado")
        return False

    tel_hash = _sha256(_normalizar_tel(telefone))
    event_id = f"wa-{talk_id}"

    if ctwa_clid:
        # ── Veio de anúncio CTWA → evento business_messaging ──────────────
        payload = {
            "data": [{
                "event_name"       : "LeadSubmitted",
                "event_time"       : int(time.time()),
                "event_id"         : event_id,
                "action_source"    : "business_messaging",
                "messaging_channel": "whatsapp",
                "user_data": {
                    "ph"       : [tel_hash],
                    "page_id"  : META_PAGE_ID,
                    "ctwa_clid": ctwa_clid,
                },
                "custom_data": {"currency": "BRL", "value": 1000},
            }],
            "access_token": META_CAPI_TOKEN,
        }
        log.info(f"📱 CAPI CTWA [LeadSubmitted+ctwa_clid] → talk={talk_id}")
    else:
        # ── Orgânico / link direto → evento website com phone ─────────────
        payload = {
            "data": [{
                "event_name"      : "Lead",
                "event_time"      : int(time.time()),
                "event_id"        : event_id,
                "event_source_url": "https://construtoraorion.com/",
                "action_source"   : "website",
                "user_data": {
                    "ph"         : [tel_hash],
                    "external_id": [tel_hash],
                },
                "custom_data": {
                    "content_name": "Nova conversa WhatsApp — Orion",
                    "currency"    : "BRL",
                    "value"       : 1000,
                    "content_ids" : ["orion-alto-padrao"],
                },
            }],
            "access_token": META_CAPI_TOKEN,
        }
        log.info(f"📱 CAPI orgânico [Lead/website] → talk={talk_id}")

    try:
        async with httpx.AsyncClient(timeout=10.0) as http:
            r = await http.post(META_CAPI_URL, json=payload)
            if r.status_code == 200:
                log.info(f"✅ CAPI WhatsApp enviado — talk={talk_id} tel=...{telefone[-4:]}")
                return True
            else:
                log.warning(f"⚠️ CAPI WhatsApp erro {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.error(f"❌ CAPI WhatsApp erro: {e}")
    return False


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
async def _buscar_ctwa_do_talk(talk_id: str) -> tuple[str, str]:
    """
    Busca ctwa_clid e source_id via API do Kommo no objeto talk.
    Retorna (ctwa_clid, source_id) ou ("", "") se não encontrar.
    """
    if not talk_id:
        return "", ""
    headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
    try:
        async with httpx.AsyncClient(timeout=8.0) as http:
            r = await http.get(f"{KOMMO_API}/talks/{talk_id}", headers=headers)
            if r.status_code != 200:
                return "", ""
            data = r.json()
            log.info(f"🔍 Talk {talk_id} data keys: {list(data.keys())}")
            # Busca ctwa_clid em vários campos possíveis
            ctwa = (
                str(data.get("ctwa_clid", ""))
                or str(data.get("referral", {}).get("ctwa_clid", ""))
                or str(data.get("origin", {}).get("ctwa_clid", ""))
                or str(data.get("source", {}).get("ctwa_clid", ""))
                or ""
            )
            source_id = (
                str(data.get("referral", {}).get("source_id", ""))
                or str(data.get("source_id", ""))
                or ""
            )
            if ctwa or source_id:
                log.info(f"✅ ctwa_clid via API talk: {ctwa[:20]} | source_id: {source_id}")
            return ctwa, source_id
    except Exception as e:
        log.debug(f"[talk:{talk_id}] Erro ao buscar talk data: {e}")
        return "", ""


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
async def _processar(talk_id: str, lead_id: str | None, texto: str, msg_id: str = "", ctwa_clid: str = "", source_id: str = "", ad_headline: str = "", is_instagram: bool = False, source_type: str = ""):
    if _e_duplicada(talk_id, msg_id, texto):
        log.debug(f"[talk:{talk_id}] Duplicada ignorada")
        return

    # ── Detecta NOVA conversa ─────────────────────────────────────────────
    is_nova_conversa = talk_id not in _talks_novos and talk_id not in _sessoes
    if is_nova_conversa:
        _talks_novos.add(talk_id)
        log.info(f"[talk:{talk_id}] 🆕 Nova conversa detectada")

        # Identifica origem da conversa
        is_ctwa    = bool(ctwa_clid)                        # WhatsApp CTWA direto
        is_ig_ad   = is_instagram and source_type == "ad"   # Instagram DM de anúncio
        is_anuncio = is_ctwa or is_ig_ad
        tem_quiz   = await _lead_tem_tag_quiz(lead_id or "")

        # Se ctwa_clid não veio no webhook, busca via API do Kommo
        if not is_anuncio and not tem_quiz:
            ctwa_api, source_api = await _buscar_ctwa_do_talk(talk_id)
            if ctwa_api:
                ctwa_clid = ctwa_api
                source_id = source_api or source_id
                is_ctwa   = True
                is_anuncio = True
                log.info(f"[talk:{talk_id}] 📱 ctwa_clid recuperado via API Kommo")

        # Busca o telefone/ID do contato para rastreamento
        telefone_lead = await _telefone_do_talk(talk_id)
        tel_norm      = _normalizar_tel(telefone_lead) if telefone_lead else ""

        if is_ctwa:
            # ── WhatsApp: clique direto no anúncio CTWA ───────────────────────
            log.info(f"[talk:{talk_id}] 📱 WhatsApp CTWA direto | ad={source_id}")
            _talks_anuncio.add(talk_id)
            if tel_norm:
                _telefones_anuncio[tel_norm] = {
                    "ctwa_clid"  : ctwa_clid,
                    "source_id"  : source_id,
                    "ad_headline": ad_headline,
                    "canal"      : "whatsapp",
                    "ts"         : time.time(),
                }
            if lead_id:
                asyncio.create_task(_identificar_lead_anuncio(
                    lead_id, ctwa_clid, source_id, ad_headline,
                    canal="whatsapp", retornou=False
                ))

        elif is_ig_ad:
            # ── Instagram DM: clique no anúncio ───────────────────────────────
            log.info(f"[talk:{talk_id}] 📸 Instagram DM de anúncio | ad={source_id}")
            _talks_anuncio.add(talk_id)
            if lead_id:
                asyncio.create_task(_identificar_lead_anuncio(
                    lead_id, ctwa_clid, source_id, ad_headline,
                    canal="instagram", retornou=False
                ))

        elif tel_norm and tel_norm in _telefones_anuncio:
            # ── Salvou contato via anúncio e voltou dias depois ───────────────
            info = _telefones_anuncio[tel_norm]
            dias = round((time.time() - info["ts"]) / 86400, 1)
            canal = info.get("canal", "whatsapp")
            log.info(f"[talk:{talk_id}] ♻️ Lead retornou {dias}d após anúncio ({canal})")
            _talks_anuncio.add(talk_id)
            if lead_id:
                asyncio.create_task(_identificar_lead_anuncio(
                    lead_id, info["ctwa_clid"], info["source_id"], info["ad_headline"],
                    canal=canal, retornou=True, dias=dias
                ))

        elif not tem_quiz:
            # ── Orgânico sem quiz e sem anúncio → silencia ───────────────────
            asyncio.create_task(_silenciar_talk(talk_id))
            log.info(f"[talk:{talk_id}] Contato orgânico sem quiz — notificação suprimida")

        # Dispara CAPI WhatsApp (quiz, anúncio ou orgânico)
        asyncio.create_task(
            _capi_nova_conversa_whatsapp(telefone_lead, talk_id, ctwa_clid)
        )

    if texto == "__AUDIO__":
        await _enviar_resposta(talk_id, lead_id,
            "Olá! Recebi seu áudio, mas ainda não consigo ouvi-lo. "
            "Por favor, me envie sua mensagem em *texto*! 😊")
        return

    if texto in ("__MIDIA__", "__ARQUIVO__"):
        await _enviar_resposta(talk_id, lead_id,
            "Olá! Recebi seu arquivo. Me descreva em *texto* como posso ajudar! 😊")
        return

    # ── Lead de anúncio → João atende manualmente, Sofia fica quieta ─────────
    if talk_id in _talks_anuncio:
        log.info(f"[talk:{talk_id}] 📱 Anúncio CTWA — aguardando atendimento manual do João")
        return

    # ── Sofia desativada manualmente ──────────────────────────────────────────
    if os.environ.get("SOFIA_DISABLED", "").lower() in ("1", "true", "yes"):
        log.info(f"[talk:{talk_id}] Sofia desativada (SOFIA_DISABLED) — mensagem ignorada")
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

        # ctwa_clid — click-to-WhatsApp ID (atribuição ao anúncio Meta)
        referral    = msg.get("referral", {}) or {}
        ctwa_clid   = str(msg.get("ctwa_clid", "") or referral.get("ctwa_clid", ""))
        # source_id = ID do anúncio Meta que gerou o clique
        source_id   = str(referral.get("source_id", "") or msg.get("source_id", ""))
        # source_type = "ad" quando veio de anúncio (Instagram ou WhatsApp)
        source_type = str(referral.get("source_type", "") or msg.get("source_type", "")).lower()
        # headline do anúncio (título do criativo)
        ad_headline = str(referral.get("headline", "") or msg.get("headline", ""))
        # Canal de origem: detecta Instagram pelo author ou channel
        author_info = msg.get("author", {}) or {}
        channel_raw = str(
            msg.get("channel", "") or
            author_info.get("type", "") or
            msg.get("origin", "") or ""
        ).lower()
        is_instagram = "instagram" in channel_raw or "ig" in channel_raw

        mensagens.append({
            "talk_id"     : str(msg.get("talk_id", "")),
            "lead_id"     : str(msg.get("element_id", msg.get("lead_id", ""))),
            "text"        : texto,
            "msg_id"      : str(msg.get("id", "")),
            "ctwa_clid"   : ctwa_clid,
            "source_id"   : source_id,
            "source_type" : source_type,   # "ad" quando veio de anúncio
            "ad_headline" : ad_headline,
            "is_instagram": is_instagram,  # True se veio do Instagram DM
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

    # 🔍 DEBUG TEMPORÁRIO — loga payload completo para detectar campo ctwa_clid
    try:
        import json as _json
        payload_str = _json.dumps(dados, ensure_ascii=False, default=str)
        if "ctwa" in payload_str.lower() or "referral" in payload_str.lower() or "source_id" in payload_str.lower():
            log.info(f"🔍 DEBUG CTWA DETECTADO: {payload_str[:2000]}")
        else:
            # Loga apenas msgs add para inspecionar campos disponíveis
            msgs_add = dados.get("message", {}).get("add", {})
            if msgs_add:
                first_msg = list(msgs_add.values())[0] if isinstance(msgs_add, dict) else msgs_add[0] if msgs_add else {}
                log.info(f"🔍 DEBUG MSG FIELDS: {list(first_msg.keys())} | ctwa={first_msg.get('ctwa_clid','')} | ref={first_msg.get('referral',{})}")
    except Exception:
        pass

    mensagens = _extrair_mensagens(dados)
    for msg in mensagens:
        if msg["talk_id"]:
            asyncio.create_task(
                _processar(
                    msg["talk_id"],
                    msg["lead_id"] or None,
                    msg["text"],
                    msg.get("msg_id", ""),
                    msg.get("ctwa_clid", ""),
                    msg.get("source_id", ""),
                    msg.get("ad_headline", ""),
                    msg.get("is_instagram", False),
                    msg.get("source_type", ""),
                )
            )

    # Mudanças de etapa → CAPI (qualificado/reunião agendada)
    status_leads = _extrair_status_leads(dados)
    for st in status_leads:
        asyncio.create_task(_capi_etapa_lead(st["lead_id"], st["status_id"]))

    return JSONResponse({"status": "ok", "processadas": len(mensagens), "etapas": len(status_leads)})


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
# Pipelines e etapas do Kommo — LDM Incorporadora
# ---------------------------------------------------------------------------
PIPELINE_ID         = 13873179   # Funil de vendas
STAGE_ACOMPANHAR    = 107051143  # Etapa de leads de entrada → ficou no meio
STAGE_QUALIFICADO   = 107051147  # Contato inicial → lead qualificado
STAGE_PERDIDO       = 143        # Fechado - perdido → desqualificado

# Tag que identifica leads que vieram pelo quiz (usada para filtrar notificações)
TAG_QUIZ    = "quiz-orion"
TAG_ANUNCIO = "anuncio-whatsapp"

# ── Pipeline exclusivo de Anúncios WhatsApp ────────────────────────────────
PIPELINE_ANUNCIO_WA   = 13982575   # 🟢 Anuncios WhatsApp
STAGE_WA_NOVA_CONV    = 107914367  # Nova Conversa    (entrada)
STAGE_WA_QUALIFICANDO = 107914371  # Em Qualificacao
STAGE_WA_QUALIFICADO  = 107914375  # Qualificado
STAGE_WA_REUNIAO      = 107914379  # Reuniao Agendada
STAGE_WA_NAO_QUAL     = 107914383  # Nao Qualificado

# ── Pipeline exclusivo de Anúncios Instagram ───────────────────────────────
PIPELINE_ANUNCIO_IG   = 13982647   # 📸 Anuncios Instagram
STAGE_IG_NOVA_CONV    = 107914887  # Nova Conversa    (entrada)
STAGE_IG_QUALIFICANDO = 107914891  # Em Qualificacao
STAGE_IG_QUALIFICADO  = 107914895  # Qualificado
STAGE_IG_REUNIAO      = 107914899  # Reuniao Agendada
STAGE_IG_NAO_QUAL     = 107914903  # Nao Qualificado


# ---------------------------------------------------------------------------
# CAPI — qualificação feita no Kommo (webhook status_lead)
# Devolve ao Meta o sinal de qualificação humana/Sofia para otimização:
#   etapa Qualificado (quiz/WA/IG)  → CompleteRegistration
#   etapa Reunião Agendada (WA/IG)  → Schedule (sinal mais forte)
# Leads do quiz já enviam CompleteRegistration pela própria página — são pulados.
# ---------------------------------------------------------------------------
STAGES_CAPI_QUALIFICADO = {STAGE_QUALIFICADO, STAGE_WA_QUALIFICADO, STAGE_IG_QUALIFICADO}
STAGES_CAPI_REUNIAO     = {STAGE_WA_REUNIAO, STAGE_IG_REUNIAO}
VALOR_QUALIFICADO       = 50000  # entrada mínima do projeto (mesmo valor usado no quiz)

# Dedup local: evita reenvio se o Kommo repetir o webhook (TTL 48h)
_capi_etapas_enviadas: dict[str, float] = {}


def _extrair_status_leads(dados: dict) -> list[dict]:
    """Extrai mudanças de etapa do payload Kommo (leads[status][N][...])."""
    out = []
    status = (dados.get("leads") or {}).get("status") or {}
    itens = status.values() if isinstance(status, dict) else status
    for it in itens or []:
        if isinstance(it, dict) and it.get("id"):
            try:
                out.append({
                    "lead_id"  : str(it["id"]),
                    "status_id": int(it.get("status_id", 0)),
                })
            except (ValueError, TypeError):
                pass
    return out


async def _capi_etapa_lead(lead_id: str, status_id: int):
    """Envia CAPI quando lead muda para etapa de qualificação/reunião no Kommo."""
    if status_id in STAGES_CAPI_REUNIAO:
        event_name, event_id = "Schedule", f"kr-{lead_id}"
    elif status_id in STAGES_CAPI_QUALIFICADO:
        event_name, event_id = "CompleteRegistration", f"kq-{lead_id}"
    else:
        return

    # Dedup local (Kommo pode reenviar o mesmo webhook)
    agora = time.time()
    chave = f"{lead_id}:{event_name}"
    if agora - _capi_etapas_enviadas.get(chave, 0) < 172800:
        return
    _capi_etapas_enviadas[chave] = agora
    for k in [k for k, t in _capi_etapas_enviadas.items() if agora - t > 172800]:
        del _capi_etapas_enviadas[k]

    # Lead do quiz já disparou CompleteRegistration na página — não duplica
    if event_name == "CompleteRegistration" and status_id == STAGE_QUALIFICADO \
            and await _lead_tem_tag_quiz(lead_id):
        log.info(f"[capi-etapa] lead {lead_id} veio do quiz — CR já enviado pela página, pulando")
        return

    # Busca nome/telefone/email do contato no Kommo
    nome = telefone = email = ""
    headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
    try:
        async with httpx.AsyncClient(timeout=10.0) as http:
            r = await http.get(f"{KOMMO_API}/leads/{lead_id}",
                               params={"with": "contacts"}, headers=headers)
            if r.status_code != 200:
                log.warning(f"[capi-etapa] lead {lead_id}: erro {r.status_code} ao buscar")
                return
            contatos = r.json().get("_embedded", {}).get("contacts", [])
            if contatos:
                rc = await http.get(f"{KOMMO_API}/contacts/{contatos[0]['id']}", headers=headers)
                if rc.status_code == 200:
                    c = rc.json()
                    nome = c.get("name", "")
                    for cf in (c.get("custom_fields_values") or []):
                        if cf.get("field_code") == "PHONE" and cf.get("values"):
                            telefone = cf["values"][0].get("value", "")
                        elif cf.get("field_code") == "EMAIL" and cf.get("values"):
                            email = cf["values"][0].get("value", "")
    except Exception as e:
        log.warning(f"[capi-etapa] lead {lead_id}: erro ao buscar contato: {e}")
        return

    if not telefone:
        log.info(f"[capi-etapa] lead {lead_id} sem telefone — CAPI não enviado")
        return

    ok = await _enviar_capi(
        event_name  = event_name,
        event_id    = event_id,
        telefone    = telefone,
        nome        = nome,
        email       = email,
        custom_data = {
            "content_category": "Imóveis Alto Padrão",
            "content_name"    : f"Qualificação CRM Kommo — etapa {status_id}",
            "currency"        : "BRL",
            "value"           : VALOR_QUALIFICADO,
        },
    )
    if ok:
        log.info(f"📡 [capi-etapa] {event_name} → Meta | lead {lead_id} (etapa {status_id})")


def _tel_limpo(telefone: str) -> str:
    t = "".join(c for c in telefone if c.isdigit())
    return t if t.startswith("55") else "55" + t


async def _criar_contato(http: httpx.AsyncClient, nome: str, telefone: str) -> int | None:
    headers = {"Authorization": f"Bearer {KOMMO_TOKEN}", "Content-Type": "application/json"}
    payload = [{"name": nome, "custom_fields_values": [
        {"field_code": "PHONE", "values": [{"value": _tel_limpo(telefone), "enum_code": "MOB"}]}
    ]}]
    r = await http.post(f"{KOMMO_API}/contacts", json=payload, headers=headers)
    if r.status_code in (200, 201):
        cid = r.json().get("_embedded", {}).get("contacts", [{}])[0].get("id")
        log.info(f"✅ Contato criado: {cid} ({nome})")
        return cid
    log.warning(f"⚠️ Contato erro {r.status_code}: {r.text[:100]}")
    return None


async def _criar_lead(http: httpx.AsyncClient, nome: str, contact_id: int | None,
                      stage_id: int, tag: str, tags: list[str] | None = None) -> int | None:
    headers = {"Authorization": f"Bearer {KOMMO_TOKEN}", "Content-Type": "application/json"}
    embedded: dict = {}
    if contact_id:
        embedded["contacts"] = [{"id": contact_id}]
    if tags:
        embedded["tags"] = [{"name": t} for t in tags]
    payload = [{
        "name": f"{tag} — {nome}",
        "pipeline_id": PIPELINE_ID,
        "status_id": stage_id,
        "_embedded": embedded,
    }]
    r = await http.post(f"{KOMMO_API}/leads", json=payload, headers=headers)
    if r.status_code in (200, 201):
        lid = r.json().get("_embedded", {}).get("leads", [{}])[0].get("id")
        log.info(f"✅ Lead criado: {lid} → etapa {stage_id}")
        return lid
    log.warning(f"⚠️ Lead erro {r.status_code}: {r.text[:100]}")
    return None


async def _lead_tem_tag_quiz(lead_id: str) -> bool:
    """Retorna True se o lead tem a tag quiz-orion (veio pelo funil do quiz)."""
    if not lead_id or lead_id in ("None", "0", ""):
        return False
    headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
    try:
        async with httpx.AsyncClient(timeout=8.0) as http:
            r = await http.get(
                f"{KOMMO_API}/leads/{lead_id}",
                params={"with": "tags"},
                headers=headers,
            )
            if r.status_code != 200:
                return False
            tags = r.json().get("_embedded", {}).get("tags", []) or []
            return any(t.get("name") == TAG_QUIZ for t in tags)
    except Exception:
        return False


async def _identificar_lead_anuncio(
    lead_id: str,
    ctwa_clid: str,
    source_id: str = "",
    ad_headline: str = "",
    canal: str = "whatsapp",   # "whatsapp" ou "instagram"
    retornou: bool = False,
    dias: float = 0.0,
):
    """
    Move o lead para o pipeline exclusivo 'Anuncios WhatsApp' e registra rastreamento.

    Cenários:
      retornou=False → clique direto no anúncio (ctwa_clid presente na 1ª mensagem)
      retornou=True  → salvou o contato via anúncio e voltou dias depois
    """
    if not lead_id or lead_id in ("None", "0", ""):
        return
    headers = {"Authorization": f"Bearer {KOMMO_TOKEN}", "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=10.0) as http:
            # Busca o lead atual para obter o nome
            r = await http.get(f"{KOMMO_API}/leads/{lead_id}", headers=headers)
            if r.status_code != 200:
                log.warning(f"[anuncio] Não encontrou lead {lead_id}: {r.status_code}")
                return
            lead     = r.json()
            nome_raw = lead.get("name", "Lead WhatsApp")

            # Define pipeline e emoji conforme canal
            is_ig = canal == "instagram"
            emoji     = "📸" if is_ig else "🟢"
            canal_txt = "IG" if is_ig else "WA"
            pipeline  = PIPELINE_ANUNCIO_IG if is_ig else PIPELINE_ANUNCIO_WA
            stage     = STAGE_IG_NOVA_CONV  if is_ig else STAGE_WA_NOVA_CONV
            tag_canal = f"anuncio-instagram" if is_ig else TAG_ANUNCIO

            # Renomeia com emoji se ainda não foi marcado
            nome_limpo = nome_raw.replace("⏳ ACOMPANHAR — ", "").replace("✅ QUALIFICADO — ", "").strip()
            ja_marcado = "🟢" in nome_raw or "📸" in nome_raw
            novo_nome  = f"{emoji} ANÚNCIO {canal_txt} — {nome_limpo}" if not ja_marcado else nome_raw

            # Move para pipeline exclusivo + renomeia + tag
            patch_r = await http.patch(
                f"{KOMMO_API}/leads",
                json=[{
                    "id"         : int(lead_id),
                    "name"       : novo_nome,
                    "pipeline_id": pipeline,
                    "status_id"  : stage,
                    "_embedded"  : {"tags": [{"name": tag_canal}]},
                }],
                headers=headers,
            )
            if patch_r.status_code in (200, 201):
                log.info(f"✅ Lead {lead_id} → pipeline Anúncios WA | '{novo_nome}'")
            else:
                log.warning(f"⚠️ Patch lead {lead_id}: {patch_r.status_code} {patch_r.text[:150]}")

            # ── Nota de rastreamento completa ─────────────────────────────────
            canal_nome = "Instagram DM" if is_ig else "WhatsApp"
            if retornou:
                origem_txt = (
                    f"♻️ *Lead Retornou via Contato Salvo ({canal_nome})*\n\n"
                    f"📌 Esta pessoa clicou no anúncio, salvou o contato\n"
                    f"   e voltou a mensagem {dias:.1f} dia(s) depois.\n"
                )
            else:
                icone = "📸" if is_ig else "📱"
                origem_txt = f"{icone} *Lead de Anúncio {canal_nome} (clique direto)*\n\n"

            nota = (
                f"{origem_txt}"
                f"🎯 Origem: Campanha paga Meta — {canal_nome}\n"
                + (f"📢 Anúncio: {ad_headline}\n" if ad_headline else "")
                + (f"🔑 Ad ID (source_id): {source_id}\n" if source_id else "")
                + (f"🆔 ctwa_clid: {ctwa_clid}\n" if ctwa_clid else "")
                + f"🕐 {time.strftime('%d/%m/%Y %H:%M')}\n\n"
                f"⚡ Identificado automaticamente pelo sistema."
            )
            await http.post(
                f"{KOMMO_API}/leads/{lead_id}/notes",
                json=[{"note_type": "common", "params": {"text": nota}}],
                headers=headers,
            )
            log.info(f"📝 Nota anúncio adicionada ao lead {lead_id} | retornou={retornou}")
    except Exception as e:
        log.error(f"❌ Erro ao identificar lead de anúncio {lead_id}: {e}")


async def _silenciar_talk(talk_id: str):
    """Marca a conversa como lida no Kommo — suprime o badge de notificação."""
    if not talk_id:
        return
    headers = {"Authorization": f"Bearer {KOMMO_TOKEN}", "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=8.0) as http:
            r = await http.patch(
                f"{KOMMO_API}/talks/{talk_id}",
                json={"user_last_read_at": int(time.time())},
                headers=headers,
            )
            if r.status_code in (200, 201, 204):
                log.info(f"🔕 Talk {talk_id} silenciado — contato não veio do quiz")
            else:
                log.debug(f"Silenciar talk {talk_id}: {r.status_code} {r.text[:100]}")
    except Exception as e:
        log.debug(f"Silenciar talk {talk_id} erro: {e}")


async def _mover_lead(http: httpx.AsyncClient, lead_id: int, stage_id: int, nome: str, tag: str):
    headers = {"Authorization": f"Bearer {KOMMO_TOKEN}", "Content-Type": "application/json"}
    payload = [{"id": lead_id, "name": f"{tag} — {nome}", "status_id": stage_id}]
    r = await http.patch(f"{KOMMO_API}/leads", json=payload, headers=headers)
    log.info(f"📊 Lead {lead_id} → etapa {stage_id} ({r.status_code})")


async def _nota_kommo(http: httpx.AsyncClient, lead_id: int, texto: str):
    headers = {"Authorization": f"Bearer {KOMMO_TOKEN}", "Content-Type": "application/json"}
    await http.post(f"{KOMMO_API}/leads/{lead_id}/notes",
                    json=[{"note_type": "common", "params": {"text": texto}}],
                    headers=headers)


# ---------------------------------------------------------------------------
# POST /lead/init — cria lead em "Acompanhar" quando lead preenche nome+tel
# ---------------------------------------------------------------------------
@app.post("/lead/init")
async def lead_init(request: Request):
    """
    Chamado assim que o lead preenche nome + telefone (etapa 1 do quiz).
    Cria contato + lead em ACOMPANHAR e retorna o lead_id para atualização futura.
    """
    try:
        dados = await request.json()
    except Exception:
        return JSONResponse({"erro": "JSON inválido"}, status_code=400)

    nome     = dados.get("nome", "Lead Quiz").strip()
    telefone = dados.get("telefone", "").strip()
    terreno  = dados.get("tem_terreno")

    # UTMs capturados pelo quiz
    utm_source   = dados.get("utm_source", "")
    utm_medium   = dados.get("utm_medium", "")
    utm_campaign = dados.get("utm_campaign", "")
    utm_content  = dados.get("utm_content", "")
    utm_term     = dados.get("utm_term", "")

    utms_texto = ""
    if utm_source:
        utms_texto = (
            f"\n📊 *UTMs:*\n"
            f"  Source: {utm_source}\n"
            f"  Medium: {utm_medium}\n"
            f"  Campaign: {utm_campaign}\n"
        )
        if utm_content: utms_texto += f"  Content: {utm_content}\n"
        if utm_term:    utms_texto += f"  Term: {utm_term}\n"

    nota = (
        f"📋 *Quiz — construtoraorion.com*\n\n"
        f"👤 Nome: {nome}\n"
        f"📱 Telefone: {telefone}\n"
        f"🏗️ Possui terreno: {'✅ Sim' if terreno else '❌ Não'}\n\n"
        f"⏳ Status: *Aguardando conclusão do quiz...*\n"
        f"🌐 Origem: Quiz construtoraorion.com"
        f"{utms_texto}"
    )

    # Extrai IP e user-agent do request para CAPI
    client_ip  = request.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
                 request.headers.get("x-real-ip", "")
    user_agent = dados.get("user_agent", request.headers.get("user-agent", ""))
    fbc        = dados.get("fbc", "")
    fbp        = dados.get("fbp", "")
    event_id   = dados.get("event_id", "")
    event_url  = dados.get("event_source_url", "https://construtoraorion.com/")
    external_id= dados.get("external_id", "")

    async with httpx.AsyncClient(timeout=15.0) as http:
        contact_id = await _criar_contato(http, nome, telefone)
        lead_id    = await _criar_lead(http, nome, contact_id, STAGE_ACOMPANHAR, "⏳ ACOMPANHAR", tags=[TAG_QUIZ])
        if lead_id:
            await _nota_kommo(http, lead_id, nota)

    # CAPI — Lead (server-side, deduplicado com pixel via event_id)
    email = dados.get("email", "")
    if event_id:
        # Monta custom_data com UTMs para atribuição de campanha
        capi_custom = {
            "content_name": "Quiz Orion — Dados Capturados",
            "content_ids" : dados.get("content_ids", ["orion-alto-padrao"]),
            "currency"    : dados.get("currency", "BRL"),
            "value"       : dados.get("value", 1000),
        }
        if utm_source:   capi_custom["utm_source"]   = utm_source
        if utm_medium:   capi_custom["utm_medium"]   = utm_medium
        if utm_campaign: capi_custom["utm_campaign"] = utm_campaign
        if utm_content:  capi_custom["utm_content"]  = utm_content
        if utm_term:     capi_custom["utm_term"]     = utm_term

        asyncio.create_task(_enviar_capi(
            event_name       = "Lead",
            event_id         = event_id,
            telefone         = telefone,
            nome             = nome,
            email            = email,
            external_id      = external_id,
            fbc              = fbc,
            fbp              = fbp,
            client_ip        = client_ip,
            user_agent       = user_agent,
            event_source_url = event_url,
            custom_data      = capi_custom,
        ))

    return JSONResponse({"status": "ok", "lead_id": lead_id, "contact_id": contact_id})


# ---------------------------------------------------------------------------
# POST /lead/complete — atualiza lead com resultado final do quiz
# ---------------------------------------------------------------------------
@app.post("/lead/complete")
async def lead_complete(request: Request):
    """
    Chamado quando o lead termina o quiz.
    Atualiza a etapa do lead conforme o resultado:
      QUALIFICADO   → Contato inicial
      DESQUALIFICADO → Fechado - perdido
    """
    try:
        dados = await request.json()
    except Exception:
        return JSONResponse({"erro": "JSON inválido"}, status_code=400)

    lead_id   = dados.get("lead_id")
    nome      = dados.get("nome", "Lead").strip()
    resultado = dados.get("resultado", "DESQUALIFICADO")
    terreno   = dados.get("tem_terreno")
    renda     = dados.get("renda_ok")
    entrada   = dados.get("entrada_ok")

    if resultado == "QUALIFICADO":
        stage_id = STAGE_QUALIFICADO
        tag      = "✅ QUALIFICADO"
    else:
        stage_id = STAGE_PERDIDO
        tag      = "❌ DESQUALIFICADO"

    nota = (
        f"📋 *Respostas do Quiz — construtoraorion.com*\n\n"
        f"👤 Nome: {nome}\n\n"
        f"🏗️ Possui terreno: {'✅ Sim' if terreno else '❌ Não'}\n"
        f"💰 Renda ≥ R$15k/mês: {'✅ Sim' if renda else '❌ Não'}\n"
        f"💵 Tem entrada disponível: {'✅ Sim' if entrada else '❌ Não'}\n\n"
        f"🎯 Resultado final: *{tag}*\n"
        f"🌐 Origem: Quiz construtoraorion.com"
    )

    # Extrai metadados do request
    client_ip  = request.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
                 request.headers.get("x-real-ip", "")
    user_agent = dados.get("user_agent", request.headers.get("user-agent", ""))
    fbc        = dados.get("fbc", "")
    fbp        = dados.get("fbp", "")
    event_id   = dados.get("event_id", "")
    event_url  = dados.get("event_source_url", "https://construtoraorion.com/")
    external_id= dados.get("external_id", "")
    telefone   = dados.get("telefone", "")
    capi_event = dados.get("capi_event_name", "CompleteRegistration" if resultado == "QUALIFICADO" else "LeadDesqualificado")

    async with httpx.AsyncClient(timeout=15.0) as http:
        if lead_id:
            await _mover_lead(http, int(lead_id), stage_id, nome, tag)
            await _nota_kommo(http, int(lead_id), nota)
        else:
            contact_id = await _criar_contato(http, nome, telefone)
            lead_id    = await _criar_lead(http, nome, contact_id, stage_id, tag, tags=[TAG_QUIZ])
            if lead_id:
                await _nota_kommo(http, lead_id, nota)

    # CAPI — CompleteRegistration ou LeadDesqualificado
    email = dados.get("email", "")
    if event_id:
        asyncio.create_task(_enviar_capi(
            event_name       = capi_event,
            event_id         = event_id,
            telefone         = telefone,
            nome             = nome,
            email            = email,
            external_id      = external_id,
            fbc              = fbc,
            fbp              = fbp,
            client_ip        = client_ip,
            user_agent       = user_agent,
            event_source_url = event_url,
            custom_data      = {
                "content_name": f"Quiz Orion — {resultado}",
                "content_ids" : dados.get("content_ids", ["orion-alto-padrao"]),
                "status"      : resultado == "QUALIFICADO",
                "currency"    : dados.get("currency", "BRL"),
                "value"       : dados.get("value", 50000 if resultado == "QUALIFICADO" else 0),
            },
        ))

    return JSONResponse({"status": "ok", "resultado": resultado, "lead_id": lead_id})


# ---------------------------------------------------------------------------
# POST /lead/capi — dispara evento CAPI avulso (ex: Contact no WhatsApp)
# ---------------------------------------------------------------------------
@app.post("/lead/capi")
async def lead_capi(request: Request):
    """
    Endpoint genérico para disparar qualquer evento CAPI server-side.
    Usado pelo quiz para eventos como Contact (clique no botão WhatsApp).
    """
    try:
        dados = await request.json()
    except Exception:
        return JSONResponse({"erro": "JSON inválido"}, status_code=400)

    client_ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
                request.headers.get("x-real-ip", "")

    asyncio.create_task(_enviar_capi(
        event_name       = dados.get("event_name", "Contact"),
        event_id         = dados.get("event_id", ""),
        telefone         = dados.get("telefone", ""),
        nome             = dados.get("nome", ""),
        email            = dados.get("email", ""),
        external_id      = dados.get("external_id", ""),
        fbc              = dados.get("fbc", ""),
        fbp              = dados.get("fbp", ""),
        client_ip        = client_ip,
        user_agent       = dados.get("user_agent", request.headers.get("user-agent", "")),
        event_source_url = dados.get("event_source_url", "https://construtoraorion.com/"),
        custom_data      = {
            "content_name": "WhatsApp Diretor — Orion Construtora",
            "content_ids" : dados.get("content_ids", ["orion-alto-padrao"]),
            "currency"    : dados.get("currency", "BRL"),
            "value"       : dados.get("value", 50000),
        },
    ))

    return JSONResponse({"status": "ok"})
