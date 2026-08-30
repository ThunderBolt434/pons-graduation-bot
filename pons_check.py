"""
Pons Launchpad Graduation Notifier — versiune pentru GitHub Actions
--------------------------------------------------------------------
Spre deosebire de versiunea "bucla infinita" (facuta pentru un VPS/PC),
scriptul asta face O SINGURA trecere si se opreste. GitHub Actions il
porneste periodic (cron), conform .github/workflows/pons-graduation.yml.

Starea (ce tokenuri sunt urmarite, care au graduat deja) e pastrata in
state.json, care e comis (git commit) inapoi in repo de catre workflow
dupa fiecare rulare - asa "tine minte" intre executii, desi fiecare
executie porneste de la zero (runner-ele GitHub Actions sunt efemere).

Configurare: NU foloseste config.json (ca varianta pentru VPS), ci
variabile de mediu, care in Actions vin din GitHub Secrets:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
Optional (au valori implicite rezonabile):
  RPC_URL, LOG_BATCH_BLOCKS, MAX_GRADUATION_CHECKS_PER_CYCLE
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
# CONFIG (din variabile de mediu / secrets)
# ---------------------------------------------------------------------------
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    sys.exit(
        "Lipsesc TELEGRAM_BOT_TOKEN si/sau TELEGRAM_CHAT_ID din variabilele de mediu. "
        "In GitHub Actions, verifica ca ai adaugat ambele ca Repository Secrets."
    )

RPC_URL = os.environ.get("RPC_URL", "https://rpc.mainnet.chain.robinhood.com")
LOG_BATCH_BLOCKS = int(os.environ.get("LOG_BATCH_BLOCKS", "2000"))
MAX_CHECKS_PER_CYCLE = int(os.environ.get("MAX_GRADUATION_CHECKS_PER_CYCLE", "300"))
STATE_PATH = Path(__file__).with_name("state.json")

FACTORIES = {
    "active": Web3.to_checksum_address("0xA5aAb3F0c6EeadF30Ef1D3Eb997108E976351feB"),
    "legacy": Web3.to_checksum_address("0x0c37a24F5D23A486FA692d1500881d698B1F77a4"),
}

FACTORY_ABI = json.loads("""
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

ERC20_ABI = json.loads("""
[
  {"constant": true, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "type": "function"},
  {"constant": true, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"}
]
""")

w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 25}))


# ---------------------------------------------------------------------------
# STATE
# ---------------------------------------------------------------------------
def load_state():
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
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
        # Prima rulare vreodata: fara backfill istoric, pornim de la blocul curent.
        state["last_block"] = latest
        log.info("Prima rulare: pornesc de la blocul %s (fara backfill istoric)", latest)
        return

    start = state["last_block"]
    if start >= latest:
        log.info("Niciun bloc nou de scanat (start=%s, latest=%s)", start, latest)
        return

    log.info("Scanez blocurile %s -> %s (total %s blocuri noi)", start + 1, latest, latest - start)

    # Calculam noi insine topic0 (hash-ul semnaturii evenimentului) si facem
    # cererea "bruta" eth_getLogs, ca sa ocolim complet wrapper-ul contract.events,
    # care s-a dovedit deja o data incompatibil cu versiunea de web3.py instalata
    # (bug-ul anterior cu from_block/fromBlock). Decodificam manual dupa aceea.
    event_signature = (
        "TokenLaunched(address,address,address,address,address,"
        "uint256,uint256,uint256,uint256,uint256)"
    )
    topic0 = w3.to_hex(w3.keccak(text=event_signature))
    log.info("topic0 calculat pentru TokenLaunched: %s", topic0)

    for from_block in range(start + 1, latest + 1, LOG_BATCH_BLOCKS):
        to_block = min(from_block + LOG_BATCH_BLOCKS - 1, latest)
        for fac_name, fac_addr in FACTORIES.items():
            contract = w3.eth.contract(address=fac_addr, abi=FACTORY_ABI)
            try:
                raw_logs = w3.eth.get_logs({
                    "fromBlock": from_block,
                    "toBlock": to_block,
                    "address": [fac_addr],
                    "topics": [topic0],
                })
            except Exception as e:
                log.warning("get_logs a esuat pe %s [%s-%s]: %s", fac_name, from_block, to_block, e)
                continue
            log.info("Chunk %s-%s, fabrica %s: %s log-uri brute gasite (cu filtru topic)", from_block, to_block, fac_name, len(raw_logs))
            if len(raw_logs) == 0:
                try:
                    any_logs = w3.eth.get_logs({
                        "fromBlock": from_block,
                        "toBlock": to_block,
                        "address": [fac_addr],
                    })
                    log.info("  -> diagnostic: %s log-uri TOTALE de la fabrica %s in acest interval (fara filtru de topic)", len(any_logs), fac_name)
                except Exception as e:
                    log.warning("  -> diagnostic esuat: %s", e)
            for raw_log in raw_logs:
                try:
                    ev = contract.events.TokenLaunched().process_log(raw_log)
                except Exception as e:
                    log.warning("Nu am putut decodifica un log de la %s: %s", fac_addr, e)
                    continue
                token = ev["args"]["token"]
                if token not in state["tokens"]:
                    name, symbol = get_token_meta(token)
                    state["tokens"][token] = {
                        "name": name,
                        "symbol": symbol,
                        "factory": fac_name,
                        "graduated": False,
                    }
                    log.info("Token nou detectat: %s (%s) via fabrica %s", symbol, token, fac_name)
        state["last_block"] = to_block


def check_graduations(state):
    pending = [addr for addr, meta in state["tokens"].items() if not meta["graduated"]]
    checked = 0
    for addr in pending:
        if checked >= MAX_CHECKS_PER_CYCLE:
            log.info("Am atins limita de %s verificari pentru rularea asta, continui data viitoare.", MAX_CHECKS_PER_CYCLE)
            break
        meta = state["tokens"][addr]
        fac_addr = FACTORIES[meta["factory"]]
        contract = w3.eth.contract(address=fac_addr, abi=FACTORY_ABI)
        try:
            current, threshold, graduated = contract.functions.graduationStatus(
                Web3.to_checksum_address(addr)
            ).call()
        except Exception as e:
            log.warning("graduationStatus a esuat pentru %s: %s", addr, e)
            checked += 1
            continue

        checked += 1
        if graduated:
            meta["graduated"] = True
            msg = (
                "🎓 <b>Token graduat pe Pons Launchpad!</b>\n"
                f"{meta['name']} (${meta['symbol']})\n"
                f"Contract: <code>{addr}</code>\n"
                f"Explorer: https://robinhoodchain.blockscout.com/address/{addr}"
            )
            send_telegram(msg)
            log.info("GRADUAT: %s (%s)", meta["symbol"], addr)


def main():
    state = load_state()
    log.info("Rulare unica pornita. RPC=%s | tokenuri urmarite=%d", RPC_URL, len(state["tokens"]))
    try:
        scan_new_launches(state)
        check_graduations(state)
    finally:
        # salvam mereu starea (inclusiv timestamp-ul), chiar daca a fost o eroare partiala,
        # ca sa nu pierdem progresul si ca sa avem mereu un commit nou (tine repo-ul "activ").
        save_state(state)
    log.info("Gata. Tokenuri urmarite in total: %d", len(state["tokens"]))


if __name__ == "__main__":
    main()
