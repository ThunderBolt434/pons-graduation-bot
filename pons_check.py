"""
Pons Launchpad Graduation Notifier -- V1 + V2, graduare detectata prin eveniment
-----------------------------------------------------------------------------------
Ce face:
1. Scaneaza blocurile noi pentru evenimente TokenLaunched (V1 activ, V1 legacy, V2) --
   descopera tokenuri noi.
2. Scaneaza ACELASI interval de blocuri pentru evenimentul PoolGraduated de pe fabrica V2.
   Asta inseamna ca graduarea V2 e detectata IMEDIAT, in aceeasi trecere -- fara "coada"
   de verificare (polling), care era cauza principala a notificarilor "in rafala" dupa
   perioade de pauza. Notificarea se trimite in aceeasi rulare in care e detectata graduarea.
3. Pentru V1 (aproape neutilizat acum, fara eveniment de graduare documentat), se pastreaza
   polling-ul clasic graduationStatus(), cu rotatie prin coada.

Ruleaza o singura trecere per invocare (potrivit pentru GitHub Actions cron).
Config prin variabile de mediu: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (obligatorii).
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from web3 import Web3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("pons-bot")

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    sys.exit("Lipsesc TELEGRAM_BOT_TOKEN si/sau TELEGRAM_CHAT_ID din variabilele de mediu.")

RPC_URL = os.environ.get("RPC_URL", "https://rpc.mainnet.chain.robinhood.com")
LOG_BATCH_BLOCKS = int(os.environ.get("LOG_BATCH_BLOCKS", "2000"))
MAX_CHECKS_PER_CYCLE = int(os.environ.get("MAX_GRADUATION_CHECKS_PER_CYCLE", "1500"))
RETIRE_AFTER_DAYS = float(os.environ.get("RETIRE_AFTER_DAYS", "21"))
BLOCKSCOUT_API_V2 = "https://robinhoodchain.blockscout.com/api/v2"
STATE_PATH = Path(__file__).with_name("state.json")

w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 25}))

# ---------------------------------------------------------------------------
# SURSE (V1 activ, V1 legacy, V2)
# ---------------------------------------------------------------------------
V1_EVENT_SIG = (
    "TokenLaunched(address,address,address,address,address,"
    "uint256,uint256,uint256,uint256,uint256)"
)
V1_ABI = json.loads("""
[
  {"anonymous": false, "inputs": [
      {"indexed": true, "internalType": "address", "name": "token", "type": "address"},
      {"indexed": true, "internalType": "address", "name": "deployer", "type": "address"},
      {"indexed": true, "internalType": "address", "name": "dexFactory", "type": "address"},
      {"indexed": false, "internalType": "address", "name": "pairToken", "type": "address"},
      {"indexed": false, "internalType": "address", "name": "pool", "type": "address"},
      {"indexed": false, "internalType": "uint256", "name": "dexId", "type": "uint256"},
      {"indexed": false, "internalType": "uint256", "name": "launchConfigId", "type": "uint256"},
      {"indexed": false, "internalType": "uint256", "name": "positionId", "type": "uint256"},
      {"indexed": false, "internalType": "uint256", "name": "restrictionsEndBlock", "type": "uint256"},
      {"indexed": false, "internalType": "uint256", "name": "initialBuyAmount", "type": "uint256"}
    ],
    "name": "TokenLaunched", "type": "event"},
  {"inputs": [{"internalType": "address", "name": "token", "type": "address"}],
   "name": "graduationStatus",
   "outputs": [
      {"internalType": "uint256", "name": "current", "type": "uint256"},
      {"internalType": "uint256", "name": "threshold", "type": "uint256"},
      {"internalType": "bool", "name": "graduated", "type": "bool"}
    ],
   "stateMutability": "view", "type": "function"}
]
""")

V2_EVENT_SIG = "TokenLaunched(address,address,address,address,uint256,uint256)"
V2_ABI = json.loads("""
[
  {"anonymous": false, "inputs": [
      {"indexed": true, "internalType": "address", "name": "token", "type": "address"},
      {"indexed": true, "internalType": "address", "name": "curve", "type": "address"},
      {"indexed": true, "internalType": "address", "name": "deployer", "type": "address"},
      {"indexed": false, "internalType": "address", "name": "pairToken", "type": "address"},
      {"indexed": false, "internalType": "uint256", "name": "launchConfigId", "type": "uint256"},
      {"indexed": false, "internalType": "uint256", "name": "graduationThreshold", "type": "uint256"}
    ],
    "name": "TokenLaunched", "type": "event"}
]
""")

ERC20_ABI = json.loads("""
[
  {"constant": true, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "type": "function"},
  {"constant": true, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"}
]
""")

SOURCES = {
    "v1_active": {
        "address": Web3.to_checksum_address("0xA5aAb3F0c6EeadF30Ef1D3Eb997108E976351feB"),
        "abi": V1_ABI, "event_sig": V1_EVENT_SIG, "kind": "v1",
    },
    "v1_legacy": {
        "address": Web3.to_checksum_address("0x0c37a24F5D23A486FA692d1500881d698B1F77a4"),
        "abi": V1_ABI, "event_sig": V1_EVENT_SIG, "kind": "v1",
    },
    "v2": {
        "address": Web3.to_checksum_address("0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e"),
        "abi": V2_ABI, "event_sig": V2_EVENT_SIG, "kind": "v2",
    },
}

# Evenimentul PoolGraduated de pe fabrica V2 -- se declanseaza EXACT la graduare.
# topic0 confirmat public (query-uri on-chain pe adresa fabricii V2).
V2_POOL_GRADUATED_TOPIC0 = "0xa0a18f5bf205becee8b268d7cf69addab8548ae8ef361791464cf0e0e17c1361"


# ---------------------------------------------------------------------------
# STATE
# ---------------------------------------------------------------------------
def load_state():
    if STATE_PATH.exists():
        state = json.loads(STATE_PATH.read_text())
        state.setdefault("last_block", None)
        state.setdefault("tokens", {})
        state.setdefault("check_seq", 0)
        return state
    return {"last_block": None, "tokens": {}, "check_seq": 0, "last_checked_at": None}


def save_state(state):
    state["last_checked_at"] = datetime.now(timezone.utc).isoformat()
    STATE_PATH.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# TELEGRAM
# ---------------------------------------------------------------------------
def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=15,
        )
        if not r.ok:
            log.error("Trimitere Telegram esuata: %s", r.text)
    except Exception as e:
        log.error("Exceptie la trimiterea pe Telegram: %s", e)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def get_token_meta(address):
    try:
        c = w3.eth.contract(address=Web3.to_checksum_address(address), abi=ERC20_ABI)
        return c.functions.name().call(), c.functions.symbol().call()
    except Exception:
        return "necunoscut", "?"


def topic_to_address(topic):
    return Web3.to_checksum_address("0x" + topic.hex()[-40:])


def ensure_token_record(state, token_addr, src_name):
    if token_addr not in state["tokens"]:
        name, symbol = get_token_meta(token_addr)
        state["tokens"][token_addr] = {
            "name": name, "symbol": symbol, "source": src_name,
            "graduated": False, "first_seen": datetime.now(timezone.utc).isoformat(),
            "infra_address": None,
        }
    return state["tokens"][token_addr]


def get_top_holders_pct(token_addr, exclude_addresses=None):
    """Procentul din supply detinut de top 10 adrese, excluzand pool-ul/curve-ul si
    fabrica. Foloseste API-ul v2 (REST) Blockscout. None daca nu poate fi aflat."""
    exclude = {a.lower() for a in (exclude_addresses or []) if a}
    try:
        info = requests.get(f"{BLOCKSCOUT_API_V2}/tokens/{token_addr}", timeout=15).json()
        total_supply = int(info["total_supply"])
        if total_supply <= 0:
            return None
        holders_resp = requests.get(f"{BLOCKSCOUT_API_V2}/tokens/{token_addr}/holders", timeout=15).json()

        def holder_address(h):
            if isinstance(h.get("address"), dict):
                return h["address"].get("hash", "")
            return h.get("address_hash") or h.get("address") or ""

        real_holders = [h for h in holders_resp.get("items", []) if holder_address(h).lower() not in exclude]
        top10_sum = sum(int(h["value"]) for h in real_holders[:10])
        return (top10_sum / total_supply) * 100
    except Exception as e:
        log.warning("Nu am putut afla distributia holderilor pentru %s: %s", token_addr, e)
        return None


def notify_graduation(meta, addr, factory_addr):
    """Trimite notificarea de graduare pe Telegram, cu procentul de holderi ca info."""
    exclude = [meta.get("infra_address"), factory_addr]
    top10_pct = get_top_holders_pct(addr, exclude_addresses=exclude)
    distributie_txt = (
        f"Top 10 holderi (fara pool): {top10_pct:.1f}% din supply\n" if top10_pct is not None
        else "Top 10 holderi: nu am putut afla\n"
    )
    msg = (
        "🎓 <b>Token graduat pe Pons Launchpad!</b>\n"
        f"{meta['name']} (${meta['symbol']})\n"
        f"Contract: <code>{addr}</code>\n"
        f"Sursa: {meta['source']}\n"
        f"{distributie_txt}"
        f"Explorer: https://robinhoodchain.blockscout.com/address/{addr}"
    )
    send_telegram(msg)
    log.info("GRADUAT si NOTIFICAT: %s (%s) via %s", meta["symbol"], addr, meta["source"])


def scan_new_launches(state):
    latest = w3.eth.block_number

    if state["last_block"] is None:
        state["last_block"] = latest
        log.info("Prima rulare: pornesc de la blocul %s (fara backfill istoric)", latest)
        return

    start = state["last_block"]
    if start >= latest:
        log.info("Niciun bloc nou de scanat (start=%s, latest=%s)", start, latest)
        return

    log.info("Scanez blocurile %s -> %s (total %s blocuri noi)", start + 1, latest, latest - start)

    for from_block in range(start + 1, latest + 1, LOG_BATCH_BLOCKS):
        to_block = min(from_block + LOG_BATCH_BLOCKS - 1, latest)

        # --- 1) Lansari noi (TokenLaunched), pe toate sursele ---
        for src_name, src in SOURCES.items():
            addr = src["address"]
            topic0 = w3.to_hex(w3.keccak(text=src["event_sig"]))
            contract = w3.eth.contract(address=addr, abi=src["abi"])
            try:
                raw_logs = w3.eth.get_logs({
                    "fromBlock": from_block, "toBlock": to_block,
                    "address": [addr], "topics": [topic0],
                })
            except Exception as e:
                log.warning("get_logs (lansari) a esuat pe %s [%s-%s]: %s", src_name, from_block, to_block, e)
                continue
            if raw_logs:
                log.info("Chunk %s-%s, sursa %s: %s lansari noi", from_block, to_block, src_name, len(raw_logs))
            for raw_log in raw_logs:
                token = topic_to_address(raw_log["topics"][1])
                if token not in state["tokens"]:
                    name, symbol = get_token_meta(token)
                    infra_addr = None
                    try:
                        decoded = contract.events.TokenLaunched().process_log(raw_log)
                        infra_addr = decoded["args"].get("pool") or decoded["args"].get("curve")
                    except Exception as e:
                        log.warning("Nu am putut decodifica evenimentul complet pentru %s: %s", token, e)
                    state["tokens"][token] = {
                        "name": name, "symbol": symbol, "source": src_name,
                        "graduated": False, "first_seen": datetime.now(timezone.utc).isoformat(),
                        "infra_address": infra_addr,
                    }
                    log.info("Token nou detectat: %s (%s) via %s", symbol, token, src_name)

        # --- 2) Graduari V2, direct din evenimentul PoolGraduated (fara polling) ---
        v2_addr = SOURCES["v2"]["address"]
        try:
            grad_logs = w3.eth.get_logs({
                "fromBlock": from_block, "toBlock": to_block,
                "address": [v2_addr], "topics": [V2_POOL_GRADUATED_TOPIC0],
            })
        except Exception as e:
            log.warning("get_logs (graduari V2) a esuat [%s-%s]: %s", from_block, to_block, e)
            grad_logs = []
        for raw_log in grad_logs:
            token = topic_to_address(raw_log["topics"][1])
            meta = ensure_token_record(state, token, "v2")
            if not meta["graduated"]:
                meta["graduated"] = True
                meta["graduated_at"] = datetime.now(timezone.utc).isoformat()
                notify_graduation(meta, token, v2_addr)

        state["last_block"] = to_block


def check_v1_graduations(state):
    """V1 nu are eveniment de graduare documentat -- ramanem la polling
    graduationStatus(), dar doar pentru tokenurile de pe V1 (foarte putine acum)."""
    pending = [addr for addr, m in state["tokens"].items() if m["source"].startswith("v1") and not m["graduated"]]
    pending.sort(key=lambda a: state["tokens"][a].get("last_checked_seq", -1))

    checked = 0
    for addr in pending:
        if checked >= MAX_CHECKS_PER_CYCLE:
            log.info("Am atins limita de %s verificari V1 pentru rularea asta.", MAX_CHECKS_PER_CYCLE)
            break
        meta = state["tokens"][addr]
        src = SOURCES[meta["source"]]
        contract = w3.eth.contract(address=src["address"], abi=src["abi"])
        checked += 1
        try:
            _, _, graduated = contract.functions.graduationStatus(Web3.to_checksum_address(addr)).call()
        except Exception as e:
            log.warning("Verificare graduare V1 esuata pentru %s: %s", addr, e)
            continue
        state["check_seq"] += 1
        meta["last_checked_seq"] = state["check_seq"]
        if graduated:
            meta["graduated"] = True
            meta["graduated_at"] = datetime.now(timezone.utc).isoformat()
            notify_graduation(meta, addr, src["address"])


def retire_old_tokens(state):
    """Scoate din urmarire tokenurile NEGRADUATE mai vechi de RETIRE_AFTER_DAYS."""
    now = datetime.now(timezone.utc)
    to_remove = []
    for addr, meta in state["tokens"].items():
        if meta["graduated"]:
            continue
        first_seen = meta.get("first_seen")
        if not first_seen:
            continue
        age_days = (now - datetime.fromisoformat(first_seen)).total_seconds() / 86400
        if age_days > RETIRE_AFTER_DAYS:
            to_remove.append(addr)
    for addr in to_remove:
        del state["tokens"][addr]
    if to_remove:
        log.info("Pensionate %s tokenuri negraduate, mai vechi de %s zile.", len(to_remove), RETIRE_AFTER_DAYS)


def main():
    state = load_state()
    log.info("Rulare unica pornita. RPC=%s | tokenuri urmarite=%d", RPC_URL, len(state["tokens"]))
    try:
        scan_new_launches(state)
        check_v1_graduations(state)
        retire_old_tokens(state)
    finally:
        save_state(state)
    log.info("Gata. Tokenuri urmarite in total: %d", len(state["tokens"]))


if __name__ == "__main__":
    main()
