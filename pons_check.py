"""
Pons Launchpad Graduation Notifier -- versiune GitHub Actions, cu suport V1 + V2
---------------------------------------------------------------------------------
Urmareste ATAT fabricile V1 (Uniswap V3, graduare verificata prin polling
graduationStatus), CAT SI fabrica V2 (bonding curve + Uniswap V4, graduare
citita direct din campul "phase" al inregistrarii de lansare) ale Pons
Launchpad, pe Robinhood Chain, si trimite alerta pe Telegram quando un token
graduate, indiferent prin care versiune a fost lansat.

Ruleaza o singura trecere per invocare (potrivit pentru GitHub Actions cron).
Config prin variabile de mediu: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID.
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
    sys.exit(
        "Lipsesc TELEGRAM_BOT_TOKEN si/sau TELEGRAM_CHAT_ID din variabilele de mediu."
    )

RPC_URL = os.environ.get("RPC_URL", "https://rpc.mainnet.chain.robinhood.com")
LOG_BATCH_BLOCKS = int(os.environ.get("LOG_BATCH_BLOCKS", "2000"))
MAX_CHECKS_PER_CYCLE = int(os.environ.get("MAX_GRADUATION_CHECKS_PER_CYCLE", "300"))
RETIRE_AFTER_DAYS = float(os.environ.get("RETIRE_AFTER_DAYS", "21"))
TOP_HOLDERS_MAX_PCT = float(os.environ.get("TOP_HOLDERS_MAX_PCT", "30"))
BLOCKSCOUT_API = "https://robinhoodchain.blockscout.com/api"
STATE_PATH = Path(__file__).with_name("state.json")

w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 25}))

# ---------------------------------------------------------------------------
# SURSE (V1 activ, V1 legacy, V2) -- fiecare cu propriul ABI si mod de graduare
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
    "name": "TokenLaunched", "type": "event"},
  {"inputs": [{"internalType": "address", "name": "token", "type": "address"}],
   "name": "getLaunchedToken",
   "outputs": [
      {"components": [
          {"internalType": "address", "name": "token", "type": "address"},
          {"internalType": "address", "name": "curve", "type": "address"},
          {"internalType": "address", "name": "deployer", "type": "address"},
          {"internalType": "address", "name": "creatorFeeRecipient", "type": "address"},
          {"internalType": "address", "name": "pairToken", "type": "address"},
          {"internalType": "uint256", "name": "graduationThreshold", "type": "uint256"},
          {"internalType": "uint24", "name": "poolFee", "type": "uint24"},
          {"internalType": "int24", "name": "tickSpacing", "type": "int24"},
          {"internalType": "uint16", "name": "creatorTaxBps", "type": "uint16"},
          {"internalType": "bool", "name": "buybackEnabled", "type": "bool"},
          {"internalType": "uint8", "name": "phase", "type": "uint8"},
          {"internalType": "uint256", "name": "sweptQuote", "type": "uint256"},
          {"internalType": "uint256", "name": "sweptTokens", "type": "uint256"},
          {"internalType": "uint256", "name": "sweptAt", "type": "uint256"},
          {"internalType": "bool", "name": "exists", "type": "bool"}
        ],
        "internalType": "struct LaunchedToken", "name": "", "type": "tuple"}
    ],
   "stateMutability": "view", "type": "function"}
]
""")

ERC20_ABI = json.loads("""
[
  {"constant": true, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "type": "function"},
  {"constant": true, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"}
]
""")

# "kind" spune codului de mai jos cum sa verifice graduarea pentru tokenurile
# venite din sursa respectiva: "v1" = polling graduationStatus(); "v2" = polling
# campul phase din getLaunchedToken().
SOURCES = {
    "v1_active": {
        "address": Web3.to_checksum_address("0xA5aAb3F0c6EeadF30Ef1D3Eb997108E976351feB"),
        "abi": V1_ABI,
        "event_sig": V1_EVENT_SIG,
        "kind": "v1",
    },
    "v1_legacy": {
        "address": Web3.to_checksum_address("0x0c37a24F5D23A486FA692d1500881d698B1F77a4"),
        "abi": V1_ABI,
        "event_sig": V1_EVENT_SIG,
        "kind": "v1",
    },
    "v2": {
        "address": Web3.to_checksum_address("0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e"),
        "abi": V2_ABI,
        "event_sig": V2_EVENT_SIG,
        "kind": "v2",
    },
}


# ---------------------------------------------------------------------------
# STATE
# ---------------------------------------------------------------------------
def load_state():
    if STATE_PATH.exists():
        state = json.loads(STATE_PATH.read_text())
        state.setdefault("last_block", None)
        state.setdefault("tokens", {})
        return state
    return {"last_block": None, "tokens": {}, "last_checked_at": None}


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
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
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
        for src_name, src in SOURCES.items():
            addr = src["address"]
            topic0 = w3.to_hex(w3.keccak(text=src["event_sig"]))
            contract = w3.eth.contract(address=addr, abi=src["abi"])
            try:
                raw_logs = w3.eth.get_logs({
                    "fromBlock": from_block,
                    "toBlock": to_block,
                    "address": [addr],
                    "topics": [topic0],
                })
            except Exception as e:
                log.warning("get_logs a esuat pe %s [%s-%s]: %s", src_name, from_block, to_block, e)
                continue
            if raw_logs:
                log.info("Chunk %s-%s, sursa %s: %s lansari noi", from_block, to_block, src_name, len(raw_logs))
            for raw_log in raw_logs:
                # topics[1] e primul argument indexat -- "token" la toate sursele noastre.
                token = Web3.to_checksum_address("0x" + raw_log["topics"][1].hex()[-40:])
                if token not in state["tokens"]:
                    name, symbol = get_token_meta(token)
                    infra_addr = None
                    try:
                        decoded = contract.events.TokenLaunched().process_log(raw_log)
                        # "pool" la V1, "curve" la V2 -- adresa care tine tokenurile
                        # inainte/la graduare si care trebuie exclusa din analiza holderilor.
                        infra_addr = decoded["args"].get("pool") or decoded["args"].get("curve")
                    except Exception as e:
                        log.warning("Nu am putut decodifica evenimentul complet pentru %s: %s", token, e)
                    state["tokens"][token] = {
                        "name": name,
                        "symbol": symbol,
                        "source": src_name,
                        "graduated": False,
                        "first_seen": datetime.now(timezone.utc).isoformat(),
                        "infra_address": infra_addr,
                    }
                    log.info("Token nou detectat: %s (%s) via %s", symbol, token, src_name)
        state["last_block"] = to_block


def get_top_holders_pct(token_addr, exclude_addresses=None):
    """Intoarce procentul din supply detinut de top 10 adrese (dupa balanta),
    EXCLUZAND adresele din exclude_addresses (pool-ul/curve-ul tokenului, fabrica
    Pons etc.) -- ca sa nu numaram lichiditatea blocata drept "concentrare la
    detinatori". Intoarce None daca nu poate fi aflat (API indisponibil,
    supply necunoscut etc.) -- in acel caz, tratam ca 'necunoscut', nu ca 'trece filtrul'."""
    exclude = {a.lower() for a in (exclude_addresses or []) if a}
    try:
        info = requests.get(BLOCKSCOUT_API, params={
            "module": "token", "action": "getToken", "contractaddress": token_addr,
        }, timeout=15).json()
        total_supply = int(info["result"]["totalSupply"])
        if total_supply <= 0:
            return None

        # Cerem mai multe decat 10, ca sa avem loc dupa ce eliminam pool-ul/fabrica.
        holders = requests.get(BLOCKSCOUT_API, params={
            "module": "token", "action": "getTokenHolders",
            "contractaddress": token_addr, "page": 1, "offset": 25,
        }, timeout=15).json()
        real_holders = [
            h for h in holders.get("result", [])
            if h.get("address", "").lower() not in exclude
        ]
        top10_sum = sum(int(h["value"]) for h in real_holders[:10])
        return (top10_sum / total_supply) * 100
    except Exception as e:
        log.warning("Nu am putut afla distributia holderilor pentru %s: %s", token_addr, e)
        return None


def check_graduations(state):
    state.setdefault("check_seq", 0)
    pending = [addr for addr, meta in state["tokens"].items() if not meta["graduated"]]
    # Verificam intai tokenurile cele mai rar/niciodata verificate, nu mereu
    # aceleasi cele mai vechi -- asa se roteste prin toata lista in timp, in
    # loc sa ramana blocati la infinit pe aceleasi ~1500 de la inceputul cozii.
    pending.sort(key=lambda a: state["tokens"][a].get("last_checked_seq", -1))

    checked = 0
    for addr in pending:
        if checked >= MAX_CHECKS_PER_CYCLE:
            log.info("Am atins limita de %s verificari pentru rularea asta.", MAX_CHECKS_PER_CYCLE)
            break
        meta = state["tokens"][addr]
        src = SOURCES[meta["source"]]
        contract = w3.eth.contract(address=src["address"], abi=src["abi"])
        checked += 1
        state["check_seq"] += 1
        meta["last_checked_seq"] = state["check_seq"]
        try:
            if src["kind"] == "v1":
                _, _, graduated = contract.functions.graduationStatus(
                    Web3.to_checksum_address(addr)
                ).call()
            else:  # v2
                launched = contract.functions.getLaunchedToken(
                    Web3.to_checksum_address(addr)
                ).call()
                phase = launched[10]  # a 11-a componenta a struct-ului: "phase"
                graduated = phase == 2  # 2 = PoolCreated
        except Exception as e:
            log.warning("Verificare graduare esuata pentru %s (%s): %s", addr, meta["source"], e)
            continue

        if graduated:
            meta["graduated"] = True
            exclude = [meta.get("infra_address"), src["address"]]
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
            log.info("GRADUAT si NOTIFICAT: %s (%s) via %s (top10=%s)", meta["symbol"], addr, meta["source"], top10_pct)

    log.info("Verificare graduare: %s din %s tokenuri negraduate au fost verificate acum.", checked, len(pending))


def retire_old_tokens(state):
    """Scoate din urmarire tokenurile negraduate mai vechi decat RETIRE_AFTER_DAYS.
    Marea majoritate a tokenurilor de pe launchpad-uri de tipul asta nu graduate
    niciodata -- fara asta, coada de verificat ar creste la nesfarsit."""
    now = datetime.now(timezone.utc)
    to_remove = []
    for addr, meta in state["tokens"].items():
        if meta["graduated"]:
            continue
        first_seen = meta.get("first_seen")
        if not first_seen:
            continue  # tokenuri vechi, dinainte de acest update, fara timestamp -- le lasam
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
        check_graduations(state)
        retire_old_tokens(state)
    finally:
        save_state(state)
    log.info("Gata. Tokenuri urmarite in total: %d", len(state["tokens"]))


if __name__ == "__main__":
    main()
