import asyncio
import logging
import os
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("market-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Tehran")
MORNING_TIME = os.getenv("MORNING_TIME", "09:00")
EVENING_TIME = os.getenv("EVENING_TIME", "20:00")
DB_PATH = os.getenv("DATABASE_PATH", "data/marketbot.sqlite3")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

if not BOT_TOKEN or OWNER_ID <= 0:
    raise RuntimeError("Set BOT_TOKEN and OWNER_ID in the environment before starting.")

os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
bot = Bot(BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=ZoneInfo(TIMEZONE))

COINS = {
    "bitcoin": "Bitcoin (BTC)",
    "ethereum": "Ethereum (ETH)",
    "solana": "Solana (SOL)",
    "ripple": "XRP",
    "binancecoin": "BNB",
    "cardano": "Cardano (ADA)",
    "the-open-network": "Toncoin (TON)",
}
INDICES = {
    "%5EGSPC": "S&P 500",
    "%5EIXIC": "NASDAQ Composite",
    "GC%3DF": "Gold",
    "CL%3DF": "WTI Crude",
    "DX-Y.NYB": "US Dollar Index",
}

def db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS destinations (
        chat_id INTEGER PRIMARY KEY, title TEXT NOT NULL, kind TEXT NOT NULL,
        added_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)""")
    con.commit()
    return con

def is_owner(message: Message) -> bool:
    return bool(message.from_user and message.from_user.id == OWNER_ID)

def save_destination(chat_id: int, title: str, kind: str):
    with db() as con:
        con.execute("INSERT OR REPLACE INTO destinations(chat_id,title,kind) VALUES(?,?,?)",
                    (chat_id, title[:200], kind))

def remove_destination(chat_id: int):
    with db() as con:
        con.execute("DELETE FROM destinations WHERE chat_id=?", (chat_id,))

def destinations():
    with db() as con:
        return con.execute("SELECT chat_id,title,kind FROM destinations ORDER BY title").fetchall()

async def fetch_json(client: httpx.AsyncClient, url: str):
    response = await client.get(url, timeout=15)
    response.raise_for_status()
    return response.json()

async def market_snapshot() -> dict:
    async with httpx.AsyncClient(headers={"User-Agent": "AIVerseMarketBrief/1.0"}) as client:
        coins_task = fetch_json(
            client,
            "https://api.coingecko.com/api/v3/coins/markets"
            "?vs_currency=usd&ids=" + ",".join(COINS.keys()) +
            "&price_change_percentage=24h"
        )
        fng_task = fetch_json(client, "https://api.alternative.me/fng/?limit=1&format=json")
        results = await asyncio.gather(coins_task, fng_task, return_exceptions=True)
        coins = results[0] if not isinstance(results[0], Exception) else []
        fng = results[1] if not isinstance(results[1], Exception) else None

        lines = []
        for coin in coins:
            name = COINS.get(coin.get("id"), coin.get("name", "Asset"))
            price = coin.get("current_price")
            change = coin.get("price_change_percentage_24h")
            if price is None:
                continue
            sign = "+" if (change or 0) >= 0 else ""
            lines.append(f"• {name}: ${price:,.4f}  |  ۲۴ساعت: {sign}{change or 0:.2f}%")

        global_lines = []
        for symbol, label in INDICES.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=5d&interval=1d"
                data = await fetch_json(client, url)
                result = data["chart"]["result"][0]
                quote = result["indicators"]["quote"][0]["close"]
                values = [v for v in quote if v is not None]
                if len(values) >= 2:
                    last, previous = values[-1], values[-2]
                    pct = (last / previous - 1) * 100 if previous else 0
                    sign = "+" if pct >= 0 else ""
                    global_lines.append(f"• {label}: {last:,.2f} ({sign}{pct:.2f}%)")
            except Exception as exc:
                log.warning("Index fetch failed for %s: %s", label, exc)

        sentiment = "در دسترس نیست"
        if fng and fng.get("data"):
            item = fng["data"][0]
            sentiment = f"{item.get('value')} از ۱۰۰ — {item.get('value_classification')}"

    now = datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d %H:%M")
    return {"time": now, "coins": lines, "global": global_lines, "sentiment": sentiment}

async def ai_commentary(snapshot: dict) -> str:
    if not OPENAI_API_KEY:
        return "تحلیل خودکار: تغییرات ۲۴ساعته را در کنار روند چندروزه و شرایط کلان بررسی کنید؛ درصد تغییر به‌تنهایی سیگنال خرید یا فروش نیست."
    prompt = (
        "به فارسی، حداکثر در ۱۲۰ کلمه، یک توضیح خنثی و غیرشخصی از داده‌های بازار زیر بنویس. "
        "هیچ پیش‌بینی قطعی، توصیه خرید/فروش یا تضمین سود نده. داده ناموجود را حدس نزن. "
        f"داده‌ها: {snapshot}"
    )
    async with httpx.AsyncClient(timeout=25) as client:
        response = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={"model": OPENAI_MODEL, "messages":[{"role":"user","content":prompt}],
                  "temperature":0.2, "max_tokens":220}
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"].strip()

async def build_report() -> str:
    snap = await market_snapshot()
    commentary = await ai_commentary(snap)
    coin_text = "\n".join(snap["coins"]) or "• داده رمزارزها فعلاً دریافت نشد."
    global_text = "\n".join(snap["global"]) or "• داده شاخص‌های جهانی فعلاً دریافت نشد."
    return (
        f"📊 <b>گزارش بازار AIVerse</b>\n"
        f"🕒 {snap['time']} ({TIMEZONE})\n\n"
        f"🪙 <b>رمزارزها — قیمت به دلار</b>\n{coin_text}\n\n"
        f"🌍 <b>بازارهای جهانی</b>\n{global_text}\n\n"
        f"🧭 <b>شاخص ترس و طمع:</b> {snap['sentiment']}\n\n"
        f"🧠 <b>جمع‌بندی:</b> {commentary}\n\n"
        f"<i>منابع: CoinGecko، Alternative.me و Yahoo Finance. "
        f"اطلاعات ممکن است با تأخیر همراه باشد و توصیه سرمایه‌گذاری نیست.</i>"
    )

async def broadcast():
    targets = destinations()
    if not targets:
        log.info("No registered destinations; skipping broadcast.")
        return
    try:
        report = await build_report()
    except Exception:
        log.exception("Could not build market report")
        return
    for chat_id, title, kind in targets:
        try:
            await bot.send_message(chat_id, report, parse_mode="HTML", disable_web_page_preview=True)
        except Exception as exc:
            log.error("Send failed to %s (%s): %s", title, chat_id, exc)

@dp.message(CommandStart())
async def start(message: Message):
    await message.answer(
        "سلام! ربات گزارش بازار AIVerse آماده است.\n"
        "برای افزودن گروه، مالک ربات باید دستور /register_here را در همان گروه اجرا کند.\n"
        "برای کانال، ربات را مدیر کن و مالک باید /addchat CHAT_ID را در گفت‌وگوی خصوصی بفرستد."
    )

@dp.message(Command("register_here"))
async def register_here(message: Message):
    if not is_owner(message):
        await message.answer("این دستور فقط برای مالک ربات فعال است.")
        return
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("این دستور را داخل گروه موردنظر اجرا کن.")
        return
    me = await bot.get_me()
    member = await bot.get_chat_member(message.chat.id, me.id)
    if member.status not in ("administrator", "member"):
        await message.answer("ابتدا ربات را به گروه اضافه کن.")
        return
    save_destination(message.chat.id, message.chat.title or str(message.chat.id), "group")
    await message.answer("این گروه برای دریافت گزارش صبح و عصر ثبت شد.")

@dp.message(Command("addchat"))
async def add_chat(message: Message):
    if not is_owner(message) or message.chat.type != "private":
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("فرمت: /addchat -1001234567890")
        return
    try:
        chat_id = int(parts[1])
        chat = await bot.get_chat(chat_id)
        me = await bot.get_me()
        member = await bot.get_chat_member(chat_id, me.id)
        if member.status != "administrator":
            await message.answer("برای ارسال به کانال، ربات باید مدیر کانال باشد.")
            return
        save_destination(chat_id, chat.title or str(chat_id), "channel")
        await message.answer(f"کانال «{chat.title}» ثبت شد.")
    except Exception as exc:
        await message.answer(f"ثبت مقصد ناموفق بود: {str(exc)[:250]}")

@dp.message(Command("removechat"))
async def remove_chat(message: Message):
    if not is_owner(message) or message.chat.type != "private":
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("فرمت: /removechat CHAT_ID")
        return
    try:
        remove_destination(int(parts[1]))
        await message.answer("مقصد حذف شد.")
    except ValueError:
        await message.answer("شناسه چت معتبر نیست.")

@dp.message(Command("destinations"))
async def list_destinations(message: Message):
    if not is_owner(message):
        return
    rows = destinations()
    text = "\n".join(f"• {title} ({kind}) — <code>{chat_id}</code>" for chat_id,title,kind in rows)
    await message.answer(text or "هنوز مقصدی ثبت نشده است.", parse_mode="HTML")

@dp.message(Command("preview"))
async def preview(message: Message):
    if not is_owner(message):
        return
    await message.answer("در حال دریافت داده‌های بازار...")
    await message.answer(await build_report(), parse_mode="HTML", disable_web_page_preview=True)

@dp.message(Command("sendnow"))
async def send_now(message: Message):
    if not is_owner(message):
        return
    await message.answer("ارسال گزارش به مقصدهای ثبت‌شده آغاز شد.")
    await broadcast()
    await message.answer("ارسال به مقصدهای ثبت‌شده پایان یافت؛ خطاهای احتمالی در گزارش سرور ثبت می‌شوند.")

@dp.message(Command("help"))
async def help_command(message: Message):
    await message.answer(
        "/preview — پیش‌نمایش گزارش\n"
        "/sendnow — ارسال فوری به مقصدهای ثبت‌شده\n"
        "/destinations — فهرست مقصدها (مالک)\n"
        "/register_here — ثبت گروه (مالک، داخل گروه)\n"
        "/addchat CHAT_ID — ثبت کانال (مالک، خصوصی)\n"
        "/removechat CHAT_ID — حذف مقصد (مالک)"
    )

async def on_startup():
    for clock, job_id in ((MORNING_TIME, "morning"), (EVENING_TIME, "evening")):
        hour, minute = map(int, clock.split(":"))
        scheduler.add_job(broadcast, CronTrigger(hour=hour, minute=minute, timezone=ZoneInfo(TIMEZONE)),
                          id=job_id, replace_existing=True, max_instances=1, coalesce=True)
    scheduler.start()
    log.info("Scheduler started: %s and %s (%s)", MORNING_TIME, EVENING_TIME, TIMEZONE)

async def main():
    await on_startup()
    try:
        await dp.start_polling(bot)
    finally:
        scheduler.shutdown(wait=False)
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
