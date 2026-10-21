"""
Uzbek Animelar Olami — python-telegram-bot versiyasi (professional panel dizayni).
(aiogram Termux/Android'da o'rnatilmaydigan muhitlar uchun)

Imkoniyatlar:
  • Kod orqali anime qidiruv + deep-link (/start <kod>)
  • Bo'limlar: filmlar / ongoing / tugatigan / premium / barchasi
  • 🌐 AniList jonli ma'lumot qidiruv (poster, reyting, treyler)
  • 🎞 Shorts — admin yuklagan qisqa videolar (tagida Tomosha qilish tugmasi)
  • Majburiy obuna (kanallar bazadan)
  • Premium tizim (1/2 oylik, chek → admin tasdiq)
  • Serial qismlari (5 tadan tugmalar, hammasini yuklash)
  • Admin: anime qo'shish (holat tanlash bilan), qism qo'shish,
           kanalga post, admin qo'shish/o'chirish, kanallarni boshqarish
"""

import html
import logging
import os
import re
import sqlite3
import asyncio
from datetime import datetime, timedelta

import httpx  # python-telegram-bot bilan birga keladi
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger("uzdubanime")

# ============================== SOZLAMALAR ==============================

TOKEN = os.environ.get("BOT_TOKEN", "").strip()
SUPER_ADMIN_ID = int(os.environ.get("SUPER_ADMIN_ID", "0") or 0)
POST_CHANNEL = "@animeb_uz"

ADMIN_USERNAME = "@umid_ss7"
ADMIN_PHONE = os.environ.get("ADMIN_PHONE", "")
CARD_NUMBER = os.environ.get("CARD_NUMBER", "")

DB_PATH = os.environ.get("DB_PATH", "uz.dubanime_v1.db")
DATETIME_FMT = "%Y-%m-%d %H:%M:%S"
PROXY_URL = os.environ.get("ANIMED_PROXY_URL", "").strip() or None
ANILIST_URL = "https://graphql.anilist.co"
# MTProto (katta videolarni uzatish uchun)
API_ID = int(os.environ.get("ANIMED_API_ID", "0") or 0)
API_HASH = os.environ.get("ANIMED_API_HASH", "")

# Pyrogram MTProto mijozi (global)
_pyro = None
_pyro_loop = None


async def _pyro_start():
    """MTProto mijozini ishga tushiradi (katta fayllar uchun)."""
    global _pyro, _pyro_loop
    try:
        from pyrogram import Client
        _pyro_loop = asyncio.get_running_loop()
        _pyro = Client("animed_mt", api_id=API_ID, api_hash=API_HASH,
                       bot_token=TOKEN, workdir="/tmp", no_updates=True)
        await _pyro.start()
        log.info("MTProto client ishga tushdi")
    except Exception as e:
        log.warning("MTProto ishga tushmadi: %s", e)
        _pyro = None


async def _media_session(dc_id: int):
    """Berilgan DC uchun media sessiya (kesh bilan). Pyrogram'ning o'z pattern'i:
    yangi Auth -> session.start() -> ExportAuthorization -> ImportAuthorization."""
    from pyrogram.session import Session, Auth
    from pyrogram.raw import functions as rf
    from pyrogram.errors import AuthBytesInvalid
    async with _pyro.media_sessions_lock:
        if _pyro.media_sessions.get(dc_id):
            return _pyro.media_sessions[dc_id]
        session = _pyro.media_sessions[dc_id] = Session(
            _pyro, dc_id,
            await Auth(_pyro, dc_id, await _pyro.storage.test_mode()).create(),
            await _pyro.storage.test_mode(), is_media=True
        )
        await session.start()
        for _ in range(3):
            exported = await _pyro.invoke(rf.auth.ExportAuthorization(dc_id=dc_id))
            try:
                await session.invoke(rf.auth.ImportAuthorization(id=exported.id, bytes=exported.bytes))
            except AuthBytesInvalid:
                continue
            else:
                break
        else:
            await session.stop()
            _pyro.media_sessions.pop(dc_id, None)
            raise AuthBytesInvalid
        log.info("Media sessiya tayyor: DC%s", dc_id)
        return session


async def _mtproto_fetch(file_id: str, offset: int, limit: int) -> bytes:
    """MTProto orqali faylning bir qismini oladi.
    Fayl qaysi DC'da saqlangan bo'lsa (file_id.dc_id), o'sha DC sessiyasidan oladi."""
    from pyrogram.file_id import FileId, FileType
    from pyrogram.raw import functions, types as rt
    from pyrogram.errors import FileMigrate
    fid = FileId.decode(file_id)
    if fid.file_type in (FileType.DOCUMENT, FileType.VIDEO, FileType.ANIMATION,
                         FileType.AUDIO, FileType.VOICE):
        loc = rt.InputDocumentFileLocation(
            id=fid.media_id, access_hash=fid.access_hash,
            file_reference=fid.file_reference, thumb_size="")
    elif fid.file_type == FileType.PHOTO:
        loc = rt.InputPhotoFileLocation(
            id=fid.media_id, access_hash=fid.access_hash,
            file_reference=fid.file_reference, thumb_size="x")
    else:
        raise ValueError("unsupported file_type")

    def _make_req():
        return functions.upload.GetFile(location=loc, offset=offset, limit=limit, precise=True)

    dc_id = fid.dc_id or await _pyro.storage.dc_id()
    if dc_id == await _pyro.storage.dc_id():
        res = await _pyro.invoke(_make_req())
        return res.bytes
    try:
        session = await _media_session(dc_id)
        res = await session.invoke(_make_req(), sleep_threshold=30)
        return res.bytes
    except FileMigrate as e:
        # Kutilmaganda boshqa DC'ga yo'naltirilsa — o'sha DC sessiyasidan qayta urinamiz
        log.info("FILE_MIGRATE -> DC%s, qayta urinyapmiz", e.value)
        session = await _media_session(int(e.value))
        res = await session.invoke(_make_req(), sleep_threshold=30)
        return res.bytes


def _mtproto_fetch_sync(file_id: str, offset: int, limit: int, timeout: int = 60) -> bytes:
    fut = asyncio.run_coroutine_threadsafe(_mtproto_fetch(file_id, offset, limit), _pyro_loop)
    return fut.result(timeout=timeout)


def _mtproto_get_size(file_id: str) -> int:
    """MTProto orqali fayl hajmini ANIQ aniqlaydi (eksponensial + binary qidiruv).
    Telegram upload.GetFile offset/limitni 4096 ga karrali talab qiladi,
    shuning uchun barcha probalar 4096 ga tekislangan offsetda, limit=4096.
    Oxirida lo offsetdagi chunkni o'qib, aniq hajmni hisoblaymiz."""
    ALIGN = 4096

    def has_data(off: int) -> bool:
        try:
            data = _mtproto_fetch_sync(file_id, off, ALIGN, timeout=30)
            return bool(data)
        except Exception:
            return False

    if not has_data(0):
        return 0
    lo = 0  # ma'lumot bor
    size = 1024 * 1024
    while size < 2 * 1024 * 1024 * 1024:
        if not has_data(size):
            break
        lo = size
        size *= 2
    hi = size  # ma'lumot yo'q (yoki 2GB chegara)
    while hi - lo > ALIGN:
        mid = (lo + hi) // 2
        mid = (mid // ALIGN) * ALIGN
        if mid <= lo:
            mid = lo + ALIGN
        if mid >= hi:
            break
        if has_data(mid):
            lo = mid
        else:
            hi = mid
    # lo = eng katta ma'lumotli tekislangan offset; aniq hajm:
    try:
        data = _mtproto_fetch_sync(file_id, lo, ALIGN, timeout=30)
        return lo + len(data)
    except Exception:
        return lo + 1

# ============================== HOLATLAR ==============================

(S_SEARCH, P_CHECK,
 A_TITLE, A_KIND, A_VIDEO, A_HOLAT,
 EP_KOD, EP_VIDEO, EP_CLEAR,
 EP_REP_KOD, EP_REP_QISM, EP_REP_VIDEO,
 EDIT_KOD, EDIT_TITLE, EDIT_VIDEO,
 BC_TEXT,
 POST_KOD, AD_ADD, AD_DEL, CH_ADD, CH_DEL, PCH_ADD, PCH_DEL,
 ANI_INFO, PREM_GIVE,
 SHORT_KOD, SHORT_VIDEO, SHORT_TITLE, SHORT_DEL_ID) = range(29)

# ============================== BAZA ==============================

def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = _db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS animelar (
            kod INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT, caption TEXT, title TEXT,
            is_premium INTEGER DEFAULT 0, search_count INTEGER DEFAULT 0
        )""")
    # migratsiya: eski bazalarda holat ustuni bo'lmasa qo'shamiz
    cols = [r[1] for r in cur.execute("PRAGMA table_info(animelar)").fetchall()]
    if "holat" not in cols:
        cur.execute("ALTER TABLE animelar ADD COLUMN holat TEXT DEFAULT 'tugatigan'")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS serial_qismlar (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_kod INTEGER, qism_raqam INTEGER, file_id TEXT
        )""")
    # migratsiya: file_size ustuni (MTProto streaming uchun)
    sqcols = [r[1] for r in cur.execute("PRAGMA table_info(serial_qismlar)").fetchall()]
    if "file_size" not in sqcols:
        cur.execute("ALTER TABLE serial_qismlar ADD COLUMN file_size INTEGER DEFAULT 0")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS premium_users (
            user_id INTEGER PRIMARY KEY, tugash_vaqti TEXT
        )""")
    cur.execute("CREATE TABLE IF NOT EXISTS adminlar (user_id INTEGER PRIMARY KEY)")
    cur.execute("CREATE TABLE IF NOT EXISTS channels (username TEXT PRIMARY KEY)")
    cur.execute("INSERT OR IGNORE INTO channels VALUES ('animeb_uz')")
    # post yuboriladigan kanallar (majburiy obuna kanallaridan alohida)
    cur.execute("CREATE TABLE IF NOT EXISTS post_channels (username TEXT PRIMARY KEY)")
    cur.execute("INSERT OR IGNORE INTO post_channels VALUES ('animeb_uz')")
    cur.execute("""CREATE TABLE IF NOT EXISTS foydalanuvchilar (
        user_id INTEGER PRIMARY KEY, first_name TEXT, joined TEXT)""")
    # migratsiya: username ustuni
    fcols = [r[1] for r in cur.execute("PRAGMA table_info(foydalanuvchilar)").fetchall()]
    if "username" not in fcols:
        cur.execute("ALTER TABLE foydalanuvchilar ADD COLUMN username TEXT DEFAULT ''")
    # migratsiya: referal tizimi
    if "referred_by" not in fcols:
        cur.execute("ALTER TABLE foydalanuvchilar ADD COLUMN referred_by INTEGER DEFAULT 0")
    if "ref_rewarded" not in fcols:
        cur.execute("ALTER TABLE foydalanuvchilar ADD COLUMN ref_rewarded INTEGER DEFAULT 0")
    # Shorts: admin yuklagan qisqa videolar (botga joylangan animelar uchun)
    cur.execute("""CREATE TABLE IF NOT EXISTS anime_shorts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        anime_kod INTEGER, file_id TEXT, file_size INTEGER DEFAULT 0,
        sarlavha TEXT DEFAULT '', yaratilgan TEXT DEFAULT '')""")
    conn.commit()
    conn.close()


def get_channels() -> list:
    conn = _db()
    rows = conn.execute("SELECT username FROM channels").fetchall()
    conn.close()
    return [r["username"] for r in rows]


def get_post_channels() -> list:
    conn = _db()
    rows = conn.execute("SELECT username FROM post_channels").fetchall()
    conn.close()
    return [r["username"] for r in rows]


def is_admin(uid: int) -> bool:
    if uid == SUPER_ADMIN_ID:
        return True
    conn = _db()
    row = conn.execute("SELECT 1 FROM adminlar WHERE user_id = ?", (uid,)).fetchone()
    conn.close()
    return row is not None


def is_premium_active(uid: int) -> bool:
    if uid == SUPER_ADMIN_ID:
        return True
    conn = _db()
    row = conn.execute("SELECT tugash_vaqti FROM premium_users WHERE user_id = ?", (uid,)).fetchone()
    if row:
        try:
            if datetime.now() < datetime.strptime(row["tugash_vaqti"], DATETIME_FMT):
                conn.close()
                return True
        except ValueError:
            pass
        conn.execute("DELETE FROM premium_users WHERE user_id = ?", (uid,))
        conn.commit()
    conn.close()
    return False


async def check_subscription(uid: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    for ch in get_channels():
        ch_clean = ch.lstrip("@")
        try:
            m = await context.bot.get_chat_member(chat_id="@" + ch_clean, user_id=uid)
            if m.status in ("left", "kicked"):
                # Zayavka yuborgan bo'lsa — obuna deb hisoblaymiz
                if _has_join_request(uid, ch_clean):
                    continue
                return False
        except Exception:
            # A'zo topilmasa ham zayavka borligini tekshiramiz
            if _has_join_request(uid, ch_clean):
                continue
            return False
    return True


def _has_join_request(uid: int, chat_username: str) -> bool:
    """Foydalanuvchi shu kanalga zayavka yuborganmi?"""
    try:
        conn = _db()
        row = conn.execute(
            "SELECT 1 FROM join_requests WHERE user_id = ? AND chat_username = ?",
            (uid, chat_username)).fetchone()
        conn.close()
        return bool(row)
    except Exception:
        return False


async def join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zayavka yuborganlarni bazaga yozamiz — tasdiqlamaymiz, faqat obuna tekshiruvidan o'tkazamiz."""
    req = update.chat_join_request
    if not req:
        return
    try:
        conn = _db()
        conn.execute("""CREATE TABLE IF NOT EXISTS join_requests (
            user_id INTEGER, chat_username TEXT, requested_at TEXT,
            PRIMARY KEY (user_id, chat_username))""")
        conn.execute("INSERT OR REPLACE INTO join_requests (user_id, chat_username, requested_at) VALUES (?, ?, ?)",
                     (req.from_user.id, (req.chat.username or ""), datetime.now().strftime(DATETIME_FMT)))
        conn.commit()
        conn.close()
    except Exception:
        pass


# ============================== ANILIST ==============================

SEARCH_QUERY = """
query ($search: String) {
  Page(page: 1, perPage: 10) {
    media(search: $search, type: ANIME, sort: SEARCH_MATCH) {
      id
      title { romaji english }
      coverImage { large }
      averageScore
      startDate { year }
    }
  }
}
"""

DETAIL_QUERY = """
query ($id: Int) {
  Media(id: $id, type: ANIME) {
    id
    title { romaji english }
    coverImage { large }
    averageScore
    genres
    episodes
    startDate { year }
    status
    description(asHtml: false)
    trailer { id site }
    siteUrl
  }
}
"""

STATUS_UZ = {
    "FINISHED": "✅ Tugagan",
    "RELEASING": "📡 Hozir chiqyapti",
    "NOT_YET_RELEASED": "🕐 Tez kunda",
    "CANCELLED": "❌ Bekor qilingan",
    "HIATUS": "⏸ Tanaffusda",
}


async def anilist(query: str, variables: dict | None = None):
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(ANILIST_URL, json={"query": query, "variables": variables or {}})
        r.raise_for_status()
        data = r.json()
        if data.get("errors"):
            raise RuntimeError(str(data["errors"])[:200])
        return data["data"]


def al_title(m: dict) -> str:
    t = m.get("title") or {}
    return t.get("english") or t.get("romaji") or "Noma'lum"


def al_score(m: dict) -> str:
    s = m.get("averageScore")
    return f"{s / 10:.1f}" if s else "—"


def al_year(m: dict) -> str:
    return str((m.get("startDate") or {}).get("year") or "—")


def clean_desc(s: str, limit: int = 700) -> str:
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s).strip()
    s = re.sub(r"\n{3,}", "\n\n", s)
    if len(s) > limit:
        s = s[:limit].rstrip() + "…"
    return s


def al_card(m: dict) -> str:
    genres = ", ".join(m.get("genres") or []) or "—"
    eps = m.get("episodes") or "—"
    status = STATUS_UZ.get(m.get("status"), "")
    trailer = ""
    tr = m.get("trailer") or {}
    if tr.get("site") == "youtube" and tr.get("id"):
        trailer = f"\n🎬 Treyler: https://www.youtube.com/watch?v={tr['id']}"
    return (
        f"🎬 <b>{html.escape(al_title(m))}</b>\n"
        f"⭐ Reyting: {al_score(m)} | 📅 Yil: {al_year(m)}\n"
        f"🎭 Janr: {html.escape(genres)}\n"
        f"📺 Qismlar: {eps} {status}\n"
        f"{trailer}\n\n"
        f"{html.escape(clean_desc(m.get('description')))}"
    )

# ============================== KLAVIATURALAR ==============================

USER_BUTTONS = [
    "🔍 Anime qidiruv", "💎 Premium",
    "🎬 Anime filmlar", "➡ Ongoing animelar",
    "✅ Tugatigan animelar",
    "🌐 Anime ma'lumot qidiruv",
    "💎 Premium animelar", "📚 Barcha animelar",
    "🎞 Shorts",
    "🎁 Do'st taklif qilish",
    "📞 Murojaat",
]
ADMIN_BUTTONS = [
    "➕ Anime Qo'shish", "✏️ Anime Tahrirlash",
    "🎞 Qism Qo'shish", "🗑 Qismlarni Tozalash",
    "🔄 Qismni Almashtirish",
    "🎬 Short Qo'shish", "🗑 Short O'chirish",
    "📋 Anime Ro'yxati", "📊 Statistika",
    "📢 Post Joylash", "📣 Broadcast",
    "🎁 Premium Berish", "👑 Admin Qo'shish",
    "🗑 Admin O'chirish",
    "📢 Kanallar", "🚪 Foydalanuvchi Menyusi",
    "📦 Baza nusxasi",
]
BACK_BUTTON = "📱 Orqaga"

USER_PATTERN = "^(" + "|".join(re.escape(t) for t in USER_BUTTONS) + ")$"
ADMIN_PATTERN = "^(" + "|".join(re.escape(t) for t in ADMIN_BUTTONS) + ")$"
MENU_PATTERN = "^(" + "|".join(re.escape(t) for t in ADMIN_BUTTONS + USER_BUTTONS + [BACK_BUTTON]) + ")$"


def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["🔍 Anime qidiruv", "💎 Premium"],
            ["🎬 Anime filmlar", "➡ Ongoing animelar"],
            ["✅ Tugatigan animelar"],
            ["🌐 Anime ma'lumot qidiruv"],
            ["💎 Premium animelar", "📚 Barcha animelar"],
            ["🎞 Shorts"],
            ["🎁 Do'st taklif qilish"],
            ["📞 Murojaat"],
        ],
        resize_keyboard=True,
    )


def back_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[BACK_BUTTON]], resize_keyboard=True)


def admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["➕ Anime Qo'shish", "✏️ Anime Tahrirlash"],
            ["🎞 Qism Qo'shish", "🗑 Qismlarni Tozalash"],
            ["🔄 Qismni Almashtirish"],
            ["🎬 Short Qo'shish", "🗑 Short O'chirish"],
            ["📋 Anime Ro'yxati", "📊 Statistika"],
            ["📢 Post Joylash", "📣 Broadcast"],
            ["🎁 Premium Berish", "👑 Admin Qo'shish"],
            ["🗑 Admin O'chirish"],
            ["📢 Kanallar", "🚪 Foydalanuvchi Menyusi"],
            ["📦 Baza nusxasi"],
        ],
        resize_keyboard=True,
    )


def sub_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("Kanalga a'zo bo'lish 🚀", url=f"tg://resolve?domain={ch.lstrip('@')}")]
        for ch in get_channels()
    ]
    rows.append([InlineKeyboardButton("Tekshirish ✅", callback_data="check_sub")])
    return InlineKeyboardMarkup(rows)


# ============================== ANIME KO'RSATISH ==============================

async def _display_anime(chat_id: int, uid: int, kod_text: str,
                         context: ContextTypes.DEFAULT_TYPE) -> None:
    conn = _db()
    anime = conn.execute(
        "SELECT file_id, caption, title, is_premium, search_count FROM animelar WHERE kod = ?",
        (kod_text,),
    ).fetchone()

    if not anime:
        conn.close()
        await context.bot.send_message(
            chat_id, "<b>😔 Bu kod bilan hech qanday anime topilmadi.</b>",
            parse_mode=ParseMode.HTML)
        return

    if anime["is_premium"] and not is_premium_active(uid):
        conn.close()
        await context.bot.send_message(
            chat_id,
            "🔒 <b>Kechirasiz, ushbu anime faqat faol Premium a'zolar uchun!</b>\n"
            "Sotib olish uchun «💎 Premium» bo'limidan foydalaning.",
            parse_mode=ParseMode.HTML,
        )
        return

    new_count = (anime["search_count"] or 0) + 1
    conn.execute("UPDATE animelar SET search_count = ? WHERE kod = ?", (new_count, kod_text))
    qismlar = conn.execute(
        "SELECT qism_raqam, file_id FROM serial_qismlar WHERE anime_kod = ? ORDER BY qism_raqam ASC",
        (kod_text,),
    ).fetchall()
    conn.commit()
    conn.close()

    bot_username = (await context.bot.get_me()).username
    caption = (
        f"🔍 <b>Nomi:</b> {anime['title']}\n"
        f"🔑 <b>Kod:</b> {kod_text}\n"
        f"🌐 <b>Manzil:</b> @{bot_username}\n"
        f"📊 <b>Qidirishlar soni:</b> {new_count}\n\n"
        f"📌 {anime['caption'] or 'Barcha qismlar quyida joylashtirildi.'}"
    )

    rows = []
    if qismlar:
        row = []
        for q in qismlar:
            row.append(InlineKeyboardButton(f"🟢 {q['qism_raqam']}",
                                            callback_data=f"play_{kod_text}_{q['qism_raqam']}"))
            if len(row) == 5:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([InlineKeyboardButton("❌ Yopish", callback_data="close_message")])
        rows.append([InlineKeyboardButton(f"📥 Yuklash (1-{len(qismlar)})",
                                          callback_data=f"all_download_{kod_text}")])

    # Poster — rasm (photo.file_id). Eski video-posterlar uchun fallback.
    try:
        await context.bot.send_photo(chat_id=chat_id, photo=anime["file_id"],
                                     caption=caption, parse_mode=ParseMode.HTML,
                                     reply_markup=InlineKeyboardMarkup(rows) if rows else None)
    except Exception:
        await context.bot.send_video(chat_id=chat_id, video=anime["file_id"],
                                     caption=caption, parse_mode=ParseMode.HTML,
                                     reply_markup=InlineKeyboardMarkup(rows) if rows else None)


async def send_anime_by_kod(message, kod_text: str, context: ContextTypes.DEFAULT_TYPE):
    await _display_anime(message.chat.id, message.from_user.id, kod_text, context)


# ============================== MENYU ROUTER ==============================

async def list_anime(update: Update, sarlavha: str, where: str, params: tuple = ()):
    """Anime ro'yxatini inline tugmalar bilan ko'rsatish."""
    conn = _db()
    rows = conn.execute(
        f"SELECT kod, title FROM animelar {where} ORDER BY kod DESC LIMIT 50", params).fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text(f"{sarlavha}\n\n😔 Hozircha hech narsa yo'q.")
        return ConversationHandler.END
    kb = [[InlineKeyboardButton(f"🎬 {r['title']}", callback_data=f"view_{r['kod']}")]
          for r in rows]
    await update.message.reply_text(
        f"<b>{sarlavha}</b>\n\n👇 Tomosha qilish uchun tanlang:",
        parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END


async def shorts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin yuklagan shortlar: har biri video bo'lib chiqadi,
    tagida o'sha animeni ochadigan "Tomosha qilish" tugmasi bilan."""
    conn = _db()
    rows = conn.execute(
        "SELECT s.file_id, s.sarlavha, s.anime_kod, a.title FROM anime_shorts s "
        "LEFT JOIN animelar a ON a.kod = s.anime_kod ORDER BY s.id DESC LIMIT 10"
    ).fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text(
            "😔 Hali short yo'q. Admin short yuklagach, shu yerda chiqadi.")
        return ConversationHandler.END
    await update.message.reply_text("🎞 <b>Shorts:</b>", parse_mode=ParseMode.HTML)
    for r in rows:
        title = r["title"] or ""
        sarlavha = (r["sarlavha"] or "").strip()
        caption = f"🎬 <b>{html.escape(sarlavha or title)}</b>"
        if title and sarlavha and sarlavha != title:
            caption += f"\n📌 {html.escape(title)}"
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("▶ Tomosha qilish", callback_data=f"view_{r['anime_kod']}")
        ]])
        try:
            await update.message.reply_video(
                video=r["file_id"], caption=caption,
                parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception as e:
            log.warning("Short yuborilmadi (kod %s): %s", r["anime_kod"], e)
    return ConversationHandler.END


async def user_action(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """Foydalanuvchi pastki-klaviatura tugmasi."""
    uid = update.effective_user.id
    if not await check_subscription(uid, context):
        await update.message.reply_text(
            "<b>❌ Botdan foydalanish uchun homiy kanallarga a'zo bo'ling!</b>",
            parse_mode=ParseMode.HTML, reply_markup=sub_keyboard())
        return ConversationHandler.END

    # eski tugma nomlari uchun moslik
    aliases = {"⚡ Anime qidiruv": "🔍 Anime qidiruv",
               "💎 Premium sotib olish": "💎 Premium",
               "📞 Aloqa": "📞 Murojaat"}
    text = aliases.get(text, text)

    if text == "🔍 Anime qidiruv":
        await update.message.reply_text(
            "<b>🔍 Ko'rmoqchi bo'lgan anime kodini (raqamini) yuboring:</b>",
            parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return S_SEARCH

    if text == "💎 Premium":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 1 oylik (15 000 so'm)", callback_data="buy_1_month")],
            [InlineKeyboardButton("💳 2 oylik (25 000 so'm)", callback_data="buy_2_month")],
            [InlineKeyboardButton("👤 Adminga yozish", url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}")],
        ])
        await update.message.reply_text(
            "<b>💎 PREMIUM OBUNA TARIFLARI:</b>\n\n"
            "🗓 1 oylik obuna — <b>15 000 so'm</b>\n"
            "🗓 2 oylik obuna — <b>25 000 so'm</b>\n\n"
            "<b>To'lov turi:</b> Klik / Payme\n"
            f"💳 Karta raqam: <code>{CARD_NUMBER}</code>\n\n"
            "To'lovni qilib, tarifni tanlang va <b>chekni adminga yuboring!</b>\n"
            f"📞 Savollar bo'lsa — adminga yozing: {ADMIN_USERNAME}",
            parse_mode=ParseMode.HTML, reply_markup=kb)
        return ConversationHandler.END

    if text == "🎬 Anime filmlar":
        await list_anime(update, "🎬 Anime filmlar", "WHERE holat = 'kino'")
        return ConversationHandler.END

    if text == "➡ Ongoing animelar":
        await list_anime(update, "➡ Ongoing animelar", "WHERE holat = 'ongoing'")
        return ConversationHandler.END

    if text == "✅ Tugatigan animelar":
        await list_anime(update, "✅ Tugatigan animelar", "WHERE holat = 'tugatigan'")
        return ConversationHandler.END

    if text == "🌐 Anime ma'lumot qidiruv":
        await update.message.reply_text(
            "<b>🌐 Anime nomini yozing</b> — xalqaro bazadan poster, reyting va treyler topib beraman:",
            parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return ANI_INFO

    if text == "💎 Premium animelar":
        await list_anime(update, "💎 Premium animelar", "WHERE is_premium = 1")
        return ConversationHandler.END

    if text == "📚 Barcha animelar":
        await list_anime(update, "📚 Barcha animelar", "")
        return ConversationHandler.END

    if text == "🎞 Shorts":
        await shorts_cmd(update, context)
        return ConversationHandler.END

    if text == "🎁 Do'st taklif qilish":
        uid = update.effective_user.id
        bot_username = (await context.bot.get_me()).username
        conn = _db()
        cnt = conn.execute("SELECT COUNT(*) c FROM foydalanuvchilar WHERE referred_by = ?", (uid,)).fetchone()["c"]
        conn.close()
        ref_link = f"https://t.me/{bot_username}?start=ref{uid}"
        await update.message.reply_text(
            "<b>🎁 Do'st taklif qilish — Premium yutib oling!</b>\n\n"
            f"🔗 Sizning taklif havolangiz:\n<code>{ref_link}</code>\n\n"
            f"👥 Taklif qilgan do'stlaringiz: <b>{cnt}/3</b>\n\n"
            "🎉 <b>3 ta do'st</b> botga sizning havolangiz orqali qo'shilsa,\n"
            "sizga <b>1 oylik Premium</b> sovg'a qilinadi!",
            parse_mode=ParseMode.HTML)
        return ConversationHandler.END

    if text == "📞 Murojaat":
        await update.message.reply_text(
            f"<b>📞 Biz bilan bog'lanish:</b>\n\n👤 Admin: {ADMIN_USERNAME}\n📞 Telefon: {ADMIN_PHONE}",
            parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return ConversationHandler.END

    return ConversationHandler.END


async def admin_action(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """Admin pastki-klaviatura tugmasi."""
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("⛔ Bu bo'lim faqat adminlar uchun.")
        return ConversationHandler.END

    if text == "➕ Anime Qo'shish":
        await update.message.reply_text("🎬 Yangi animening <b>nomini</b> yozing:",
                                        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return A_TITLE

    if text == "✏️ Anime Tahrirlash":
        await update.message.reply_text("🔢 Tahrirlamoqchi bo'lgan anime <b>botdagi kodini (raqamini)</b> kiriting:",
                                        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return EDIT_KOD

    if text == "🎞 Qism Qo'shish":
        await update.message.reply_text("🔢 Anime <b>botdagi kodini (raqamini)</b> kiriting:",
                                        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return EP_KOD

    if text == "🗑 Qismlarni Tozalash":
        await update.message.reply_text("🔢 Qismlarini tozalamoqchi bo'lgan anime <b>botdagi kodini (raqamini)</b> kiriting:",
                                        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return EP_CLEAR

    if text == "🔄 Qismni Almashtirish":
        await update.message.reply_text("🔢 Qismini almashtirmoqchi bo'lgan anime <b>botdagi kodini (raqamini)</b> kiriting:",
                                        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return EP_REP_KOD

    if text == "🎬 Short Qo'shish":
        await update.message.reply_text("🔢 Short qo'shmoqchi bo'lgan anime <b>botdagi kodini (raqamini)</b> kiriting:",
                                        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return SHORT_KOD

    if text == "🗑 Short O'chirish":
        conn = _db()
        rows = conn.execute(
            "SELECT s.id, s.anime_kod, s.sarlavha, a.title FROM anime_shorts s "
            "LEFT JOIN animelar a ON a.kod = s.anime_kod ORDER BY s.id DESC LIMIT 30"
        ).fetchall()
        conn.close()
        if not rows:
            await update.message.reply_text("😔 Hali short yo'q.")
            return ConversationHandler.END
        txt = "<b>🗑 Shortlar ro'yxati:</b>\n\n" + "\n".join(
            f"🆔 {r['id']} • Kod {r['anime_kod']} • {(r['title'] or '')} — {(r['sarlavha'] or '')}".strip()
            for r in rows)
        await update.message.reply_text(txt + "\n\nO'chirmoqchi bo'lgan shortning <b>ID raqamini</b> yuboring:",
                                        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return SHORT_DEL_ID

    if text == "📋 Anime Ro'yxati":
        await anime_list(update, context)
        return ConversationHandler.END

    if text == "📊 Statistika":
        await statistika(update, context)
        return ConversationHandler.END

    if text == "📢 Post Joylash":
        await update.message.reply_text("📝 Kanalga chiqarmoqchi bo'lgan anime <b>botdagi kodini (raqamini)</b> yuboring:",
                                        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return POST_KOD

    if text == "📣 Broadcast":
        await update.message.reply_text("📣 Barcha foydalanuvchilarga yuboriladigan <b>xabar matnini</b> yozing:",
                                        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return BC_TEXT

    if text == "🎁 Premium Berish":
        await update.message.reply_text("🎁 Premium bermoqchi bo'lgan foydalanuvchining <b>@nikini</b> yoki <b>ID raqamini</b> yuboring:",
                                        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return PREM_GIVE

    if text == "👑 Admin Qo'shish":
        if uid != SUPER_ADMIN_ID:
            return ConversationHandler.END
        await update.message.reply_text("🆔 Yangi adminning <b>ID raqamini</b> yuboring:",
                                        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return AD_ADD

    if text == "🗑 Admin O'chirish":
        if uid != SUPER_ADMIN_ID:
            return ConversationHandler.END
        await update.message.reply_text("🆔 O'chirmoqchi bo'lgan adminning <b>ID raqamini</b> yuboring:",
                                        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return AD_DEL

    if text == "📢 Kanallar":
        if uid != SUPER_ADMIN_ID:
            return ConversationHandler.END
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("➕ Yangi Kanal Qo'shish", callback_data="db_add_channel"),
            InlineKeyboardButton("❌ Kanalni O'chirish", callback_data="db_del_channel"),
        ], [
            InlineKeyboardButton("➕ Post Kanal Qo'shish", callback_data="db_add_post_channel"),
            InlineKeyboardButton("❌ Post Kanalni O'chirish", callback_data="db_del_post_channel"),
        ]])
        ch_list = "\n".join(f"🔗 @{c.lstrip('@')}" for c in get_channels()) or "—"
        pch_list = "\n".join(f"📤 @{c.lstrip('@')}" for c in get_post_channels()) or "—"
        await update.message.reply_text(
            f"<b>📢 Majburiy obuna kanallari:</b>\n\n{ch_list}\n\n"
            f"<b>📤 Post yuboriladigan kanallar:</b>\n\n{pch_list}\n\n"
            "Amalni tanlang:",
            parse_mode=ParseMode.HTML, reply_markup=kb)
        return ConversationHandler.END

    if text == "🚪 Foydalanuvchi Menyusi":
        await update.message.reply_text("<b>Foydalanuvchi menyusiga qaytdingiz:</b>",
                                        parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
        return ConversationHandler.END

    if text == "📦 Baza nusxasi":
        if uid != SUPER_ADMIN_ID:
            return ConversationHandler.END
        await update.message.reply_text("📦 Baza nusxasi tayyorlanmoqda...")
        try:
            size = os.path.getsize(DB_PATH)
            stamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
            with open(DB_PATH, "rb") as f:
                await context.bot.send_document(
                    chat_id=uid,
                    document=f,
                    filename=f"uzbek_animelar_backup_{stamp}.db",
                    caption=f"📦 Uzbek Animelar Olami baza nusxasi\n📅 {stamp}\n💾 Hajmi: {size / 1024:.0f} KB",
                )
            await update.message.reply_text("✅ Baza nusxasi yuborildi.")
        except Exception as e:
            log.warning("Backup yuborilmadi: %s", e)
            await update.message.reply_text("❌ Baza nusxasini yuborib bo'lmadi. Keyinroq urinib ko'ring.")
        return ConversationHandler.END

    return ConversationHandler.END


async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Oqim o'rtasida menyu/Orqaga bosilsa — eski oqim yopiladi."""
    text = (update.message.text or "").strip()
    uid = update.effective_user.id

    if BACK_BUTTON in text:
        if is_admin(uid):
            await update.message.reply_text("<b>👑 Admin Boshqaruv paneli:</b>",
                                            parse_mode=ParseMode.HTML, reply_markup=admin_keyboard())
        else:
            await update.message.reply_text("<b>Bosh sahifaga qaytdingiz:</b>",
                                            parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
        return ConversationHandler.END

    if text in ADMIN_BUTTONS and is_admin(uid):
        return await admin_action(update, context, text)
    if text in USER_BUTTONS:
        return await user_action(update, context, text)
    return None


async def user_text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await user_action(update, context, (update.message.text or "").strip())


async def admin_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await admin_action(update, context, (update.message.text or "").strip())

# ============================== START / ADMIN ==============================

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    uid = update.effective_user.id
    is_new = False
    try:
        conn = _db()
        uname = (update.effective_user.username or "").strip()
        fname = update.effective_user.first_name or ""
        existing = conn.execute("SELECT user_id FROM foydalanuvchilar WHERE user_id = ?", (uid,)).fetchone()
        is_new = not existing
        conn.execute("INSERT OR IGNORE INTO foydalanuvchilar (user_id, first_name, joined) VALUES (?, ?, ?)",
                     (uid, fname, datetime.now().strftime(DATETIME_FMT)))
        conn.execute("UPDATE foydalanuvchilar SET username = ?, first_name = ? WHERE user_id = ?",
                     (uname, fname, uid))
        conn.commit()
        conn.close()
    except Exception:
        pass
    # Referal: /start ref<user_id>
    if is_new and context.args and context.args[0].startswith("ref"):
        try:
            ref_id = int(context.args[0][3:])
            if ref_id != uid and ref_id > 0:
                conn = _db()
                conn.execute("UPDATE foydalanuvchilar SET referred_by = ? WHERE user_id = ?", (ref_id, uid))
                cnt = conn.execute("SELECT COUNT(*) c FROM foydalanuvchilar WHERE referred_by = ?", (ref_id,)).fetchone()["c"]
                rewarded = conn.execute("SELECT ref_rewarded FROM foydalanuvchilar WHERE user_id = ?", (ref_id,)).fetchone()
                conn.commit()
                conn.close()
                # 3 ta do'st = 1 oylik premium
                if cnt >= 3 and (not rewarded or not rewarded["ref_rewarded"]):
                    until = (datetime.now() + timedelta(days=30)).strftime(DATETIME_FMT)
                    conn = _db()
                    conn.execute("INSERT OR REPLACE INTO premium_users (user_id, tugash_vaqti) VALUES (?, ?)", (ref_id, until))
                    conn.execute("UPDATE foydalanuvchilar SET ref_rewarded = 1 WHERE user_id = ?", (ref_id,))
                    conn.commit()
                    conn.close()
                    try:
                        await context.bot.send_message(
                            ref_id,
                            "🎉 <b>Tabriklaymiz! 3 ta do'stingiz qo'shildi!</b>\n\n"
                            "🎁 Sizga <b>1 oylik Premium</b> sovg'a qilindi!\n"
                            f"📅 Muddat: {until}",
                            parse_mode=ParseMode.HTML)
                    except Exception:
                        pass
        except Exception:
            pass
    if not await check_subscription(uid, context):
        await update.message.reply_text(
            "<b>❌ Botdan foydalanish uchun homiy kanallarga a'zo bo'ling!</b>",
            parse_mode=ParseMode.HTML, reply_markup=sub_keyboard())
        return
    if context.args:  # deep-link: /start <kod>
        await send_anime_by_kod(update.message, context.args[0], context)
        return
    name = update.effective_user.first_name or "Do'st"
    await update.message.reply_text(
        f"Assalomu alaykum {name}, Xush kelibsiz! 👋")
    await update.message.reply_text("⬇️ Kerakli bo'limni tanlang:",
                                    reply_markup=main_keyboard())


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text("<b>👑 Admin Boshqaruv paneli:</b>",
                                    parse_mode=ParseMode.HTML, reply_markup=admin_keyboard())


# ============================== QIDIRUV ==============================

async def s_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return S_SEARCH
    await send_anime_by_kod(update.message, update.message.text.strip(), context)
    return S_SEARCH


async def ani_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🌐 Anime ma'lumot qidiruv — AniList jonli baza."""
    if not update.message.text:
        return ANI_INFO
    query = update.message.text.strip()
    wait = await update.message.reply_text("🔎 Qidirilmoqda...")
    try:
        data = await anilist(SEARCH_QUERY, {"search": query})
        media = (data.get("Page") or {}).get("media") or []
    except Exception as e:
        log.warning("AniList qidiruv xato: %s", e)
        await wait.edit_text("❌ Qidiruvda xatolik yuz berdi. Qaytadan urinib ko'ring.")
        return ANI_INFO
    if not media:
        await wait.edit_text("😔 Hech narsa topilmadi. Boshqa nom yozib ko'ring.")
        return ANI_INFO
    kb = [[InlineKeyboardButton(f"🎬 {al_title(m)} ({al_year(m)}) ⭐{al_score(m)}",
                                callback_data=f"ainfo_{m['id']}")] for m in media]
    await wait.edit_text("<b>🔎 Natijalar</b> — batafsil uchun bosing:",
                         parse_mode=ParseMode.HTML,
                         reply_markup=InlineKeyboardMarkup(kb))
    return ANI_INFO


# ============================== PREMIUM ==============================

async def buy_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Tarif tanlandi → chek kutish holatiga o'tish (entry point)."""
    q = update.callback_query
    await q.answer()
    months = 1 if q.data == "buy_1_month" else 2
    context.user_data["months"] = months
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("👤 Adminga yozish", url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}"),
    ]])
    await q.message.reply_text(
        "📥 To'lov chekini <b>adminga yuboring</b>:\n\n"
        "1️⃣ Chek rasmini shu yerga tashlang — biz adminga yetkazamiz\n"
        "2️⃣ Yoki pastdagi tugma orqali adminga to'g'ridan-to'g'ri yozing",
        parse_mode=ParseMode.HTML, reply_markup=kb)
    return P_CHECK


async def p_check_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        await update.message.reply_text("❌ Chekni rasm ko'rinishida yuboring!")
        return P_CHECK
    months = context.user_data.get("months", 1)
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Tasdiqlash ✅", callback_data=f"p_accept_{update.effective_user.id}_{months}"),
        InlineKeyboardButton("Rad etish ❌", callback_data=f"p_reject_{update.effective_user.id}"),
    ]])
    await context.bot.send_photo(
        chat_id=SUPER_ADMIN_ID,
        photo=update.message.photo[-1].file_id,
        caption=(f"💰 <b>Premium so'rovi!</b>\n👤 {update.effective_user.full_name}\n"
                 f"🆔 ID: <code>{update.effective_user.id}</code>\n📦 Tarif: {months} oylik"),
        parse_mode=ParseMode.HTML, reply_markup=kb,
    )
    await update.message.reply_text("✅ Chekingiz adminga yuborildi. Tez orada obunangiz faollashadi!",
                                    reply_markup=main_keyboard())
    return ConversationHandler.END


async def p_check_warn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Chekni rasm ko'rinishida yuboring!")
    return P_CHECK


async def ep_video_warn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Iltimos, faqat video yuboring!")
    return EP_VIDEO

# ============================== ADMIN: ANIME QO'SHISH ==============================

async def a_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return A_TITLE
    context.user_data["new_title"] = update.message.text.strip()
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Oddiy", callback_data="type_free"),
        InlineKeyboardButton("💎 Premium", callback_data="type_prem"),
    ]])
    await update.message.reply_text("Ushbu anime turini tanlang:", reply_markup=kb)
    return A_KIND


async def a_kind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data["new_premium"] = 1 if q.data == "type_prem" else 0
    await q.message.reply_text("🖼 Anime afishasi (rasm) ni yuklang:", reply_markup=back_keyboard())
    return A_VIDEO


def _next_kod(conn) -> int:
    """Eng kichik bo'sh anime kodini topadi (o'chirilganlarning o'rnini to'ldiradi)."""
    used = {r["kod"] for r in conn.execute("SELECT kod FROM animelar").fetchall()}
    k = 1
    while k in used:
        k += 1
    return k


async def a_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        return A_VIDEO
    # Eng katta o'lchamdagi rasmni olamiz
    fid = update.message.photo[-1].file_id
    conn = _db()
    kod = _next_kod(conn)
    conn.execute(
        "INSERT INTO animelar (kod, file_id, caption, title, is_premium) VALUES (?, ?, ?, ?, ?)",
        (kod, fid, update.message.caption or "",
         context.user_data.get("new_title", ""), context.user_data.get("new_premium", 0)),
    )
    conn.commit()
    conn.close()
    context.user_data["new_kod"] = kod
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🎬 Kino", callback_data="holat_kino"),
        InlineKeyboardButton("➡ Ongoing", callback_data="holat_ongoing"),
        InlineKeyboardButton("✅ Tugatigan", callback_data="holat_tugatigan"),
    ]])
    await update.message.reply_text(
        f"✅ Anime bazaga saqlandi! Kodi: <b>{kod}</b>\n\nUshbu anime holatini tanlang:",
        parse_mode=ParseMode.HTML, reply_markup=kb)
    return A_HOLAT


async def a_holat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    holat = q.data.split("_", 1)[1]  # kino / ongoing / tugatigan
    kod = context.user_data.get("new_kod")
    if kod:
        conn = _db()
        conn.execute("UPDATE animelar SET holat = ? WHERE kod = ?", (holat, kod))
        conn.commit()
        conn.close()
    holat_nom = {"kino": "🎬 Kino", "ongoing": "➡ Ongoing",
                 "tugatigan": "✅ Tugatigan"}.get(holat, holat)
    await q.message.reply_text(
        f"✅ Holat saqlandi: {holat_nom}\n🔑 Anime kodi: <b>{kod}</b>",
        parse_mode=ParseMode.HTML, reply_markup=admin_keyboard())
    return ConversationHandler.END


# ============================== ADMIN: QISM QO'SHISH ==============================

async def ep_kod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return EP_KOD
    kod = update.message.text.strip()
    conn = _db()
    anime = conn.execute("SELECT title FROM animelar WHERE kod = ?", (kod,)).fetchone()
    if not anime:
        conn.close()
        await update.message.reply_text("❌ Anime topilmadi!")
        return ConversationHandler.END
    max_qism = conn.execute(
        "SELECT MAX(qism_raqam) FROM serial_qismlar WHERE anime_kod = ?", (kod,)).fetchone()[0]
    conn.close()
    next_qism = (max_qism or 0) + 1
    context.user_data["ep_kod"] = kod
    context.user_data["ep_title"] = anime["title"]
    context.user_data["ep_next"] = next_qism
    await update.message.reply_text(
        f"🎬 <b>{anime['title']}</b>\n\n📹 Ushbu animening <b>{next_qism}-qism</b> videosini yuklang:",
        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
    return EP_VIDEO


def _check_file_valid(file_id) -> bool:
    """file_id Telegram'da shu bot uchun yaroqli ekanini tekshiradi.
    Katta fayllar (>20MB) ham yaroqli — ular MTProto orqali uzatiladi."""
    import json as _json
    import urllib.request as _urlreq
    from urllib.parse import quote
    try:
        with _urlreq.urlopen(
            "https://api.telegram.org/bot%s/getFile?file_id=%s" % (TOKEN, quote(file_id, safe="")),
            timeout=15,
        ) as resp:
            info = _json.loads(resp.read().decode())
        if info.get("ok"):
            return True
        # "file is too big" — file_id yaroqli, faqat Bot API orqali olib bo'lmaydi (MTProto ishlatamiz)
        return "too big" in info.get("description", "").lower()
    except Exception as e:
        # HTTP 400 "file is too big" ham yaroqli degani
        return "too big" in str(e).lower()


async def ep_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.video:
        await update.message.reply_text("❌ Iltimos, faqat video yuboring!")
        return EP_VIDEO
    fid = update.message.video.file_id
    kod = context.user_data["ep_kod"]
    qism = context.user_data["ep_next"]
    conn = _db()
    conn.execute("INSERT INTO serial_qismlar (anime_kod, qism_raqam, file_id, file_size) VALUES (?, ?, ?, ?)",
                 (kod, qism, fid, update.message.video.file_size or 0))
    conn.commit()
    conn.close()
    next_qism = qism + 1
    context.user_data["ep_next"] = next_qism
    await update.message.reply_text(
        f"✅ <b>{qism}-qism</b> saqlandi!\n\n"
        f"📹 <b>{next_qism}-qism</b> videosini yuboring\n"
        f"(tugatgan bo'lsangiz «{BACK_BUTTON}» ni bosing):",
        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
    return EP_VIDEO


# ============================== ADMIN: SHORT QO'SHISH / O'CHIRISH ==============================
# Admin botga joylangan anime uchun alohida qisqa video (short) yuklaydi.
# Shortlar bazada saqlanadi; admin ularni shu botda qo'shadi va o'chiradi.

async def short_kod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return SHORT_KOD
    kod = update.message.text.strip()
    conn = _db()
    anime = conn.execute("SELECT title FROM animelar WHERE kod = ?", (kod,)).fetchone()
    conn.close()
    if not anime:
        await update.message.reply_text("❌ Anime topilmadi!")
        return ConversationHandler.END
    context.user_data["short_kod"] = kod
    context.user_data["short_title"] = anime["title"]
    await update.message.reply_text(
        f"🎬 <b>{anime['title']}</b>\n\n📹 Short videosini yuboring (qisqa video):",
        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
    return SHORT_VIDEO


async def short_video_warn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Iltimos, faqat video yuboring!")
    return SHORT_VIDEO


async def short_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.video:
        await update.message.reply_text("❌ Iltimos, faqat video yuboring!")
        return SHORT_VIDEO
    context.user_data["short_file_id"] = update.message.video.file_id
    context.user_data["short_file_size"] = update.message.video.file_size or 0
    await update.message.reply_text(
        "✏️ Short uchun <b>sarlavha</b> yozing (o'tkazib yuborish uchun — yuboring):",
        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
    return SHORT_TITLE


async def short_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = (update.message.text or "").strip()
    # Admin kiritgan sarlavhadagi HTML teglarni tozalaymiz (XSS dan himoya; sayt baribir esc() qiladi)
    sarlavha = re.sub(r"<[^>]*>", "", raw)[:200] if raw != "—" else ""
    kod = context.user_data.get("short_kod")
    fid = context.user_data.get("short_file_id")
    fsize = context.user_data.get("short_file_size") or 0
    if not kod or not fid:
        await update.message.reply_text("❌ Ma'lumot topilmadi. Qaytadan boshlang.")
        return ConversationHandler.END
    conn = _db()
    cur = conn.execute(
        "INSERT INTO anime_shorts (anime_kod, file_id, file_size, sarlavha, yaratilgan) VALUES (?, ?, ?, ?, ?)",
        (kod, fid, fsize, sarlavha, datetime.now().strftime(DATETIME_FMT)))
    short_id = cur.lastrowid
    conn.commit()
    conn.close()
    await update.message.reply_text(
        f"✅ Short saqlandi! ID: <b>{short_id}</b>\n🎬 Anime: <b>{context.user_data.get('short_title', kod)}</b>",
        parse_mode=ParseMode.HTML, reply_markup=admin_keyboard())
    return ConversationHandler.END


async def short_del_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return SHORT_DEL_ID
    try:
        sid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ ID raqam bo'lishi kerak!")
        return ConversationHandler.END
    conn = _db()
    cur = conn.execute("DELETE FROM anime_shorts WHERE id = ?", (sid,))
    conn.commit()
    n = cur.rowcount
    conn.close()
    if n:
        await update.message.reply_text(f"✅ Short o'chirildi! ID: <b>{sid}</b>",
                                        parse_mode=ParseMode.HTML, reply_markup=admin_keyboard())
    else:
        await update.message.reply_text("❌ Bu ID bilan short topilmadi!")
    return ConversationHandler.END


# ============================== ADMIN: QISMNI ALMASHTIRISH ==============================
# (MP3->AAC migratsiya: mavjud qismning file_id ni yangi video bilan almashtiradi)

async def ep_rep_kod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return EP_REP_KOD
    kod = update.message.text.strip()
    conn = _db()
    anime = conn.execute("SELECT title FROM animelar WHERE kod = ?", (kod,)).fetchone()
    if not anime:
        conn.close()
        await update.message.reply_text("❌ Anime topilmadi!")
        return ConversationHandler.END
    conn.close()
    context.user_data["rep_kod"] = kod
    context.user_data["rep_title"] = anime["title"]
    await update.message.reply_text(
        f"🎬 <b>{anime['title']}</b>\n\n🔄 Almashtirmoqchi bo'lgan <b>qism raqamini</b> kiriting:",
        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
    return EP_REP_QISM


async def ep_rep_qism(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return EP_REP_QISM
    qism = update.message.text.strip()
    if not qism.isdigit():
        await update.message.reply_text("❌ Iltimos, faqat raqam kiriting!")
        return EP_REP_QISM
    kod = context.user_data["rep_kod"]
    conn = _db()
    row = conn.execute("SELECT id FROM serial_qismlar WHERE anime_kod = ? AND qism_raqam = ?",
                       (kod, int(qism))).fetchone()
    conn.close()
    if not row:
        await update.message.reply_text(f"❌ {qism}-qism topilmadi! Avval «🎞 Qism Qo'shish» orqali qo'shing.")
        return ConversationHandler.END
    context.user_data["rep_qism"] = int(qism)
    await update.message.reply_text(
        f"📹 <b>{context.user_data['rep_title']}</b> — <b>{qism}-qism</b> uchun yangi videoni yuboring:",
        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
    return EP_REP_VIDEO


async def ep_rep_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.video:
        await update.message.reply_text("❌ Iltimos, faqat video yuboring!")
        return EP_REP_VIDEO
    fid = update.message.video.file_id
    kod = context.user_data["rep_kod"]
    qism = context.user_data["rep_qism"]
    conn = _db()
    conn.execute("UPDATE serial_qismlar SET file_id = ?, file_size = ? WHERE anime_kod = ? AND qism_raqam = ?",
                 (fid, update.message.video.file_size or 0, kod, qism))
    conn.commit()
    conn.close()
    await update.message.reply_text(
        f"✅ <b>{qism}-qism</b> almashtirildi!\n\n"
        f"🔄 Yana bir qismni almashtirish uchun uning <b>raqamini</b> kiriting\n"
        f"(tugatgan bo'lsangiz «{BACK_BUTTON}» ni bosing):",
        parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
    return EP_REP_QISM


async def ep_rep_video_warn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Iltimos, faqat video yuboring!")
    return EP_REP_VIDEO


async def ep_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Qismlarni tozalash: kod -> tasdiq (HA) -> o'chirish."""
    text = (update.message.text or "").strip()
    if "clear_kod" not in context.user_data:
        kod = text
        conn = _db()
        anime = conn.execute("SELECT title FROM animelar WHERE kod = ?", (kod,)).fetchone()
        if not anime:
            conn.close()
            await update.message.reply_text("❌ Anime topilmadi!")
            return ConversationHandler.END
        cnt = conn.execute(
            "SELECT COUNT(*) c FROM serial_qismlar WHERE anime_kod = ?", (kod,)).fetchone()["c"]
        conn.close()
        if not cnt:
            await update.message.reply_text("ℹ️ Bu animeda qism yo'q — tozalashga hojat yo'q.",
                                            reply_markup=admin_keyboard())
            return ConversationHandler.END
        context.user_data["clear_kod"] = kod
        context.user_data["clear_title"] = anime["title"]
        await update.message.reply_text(
            f"⚠️ <b>{anime['title']}</b> — <b>{cnt} ta qism</b> topildi.\n\n"
            f"Barchasini o'chirish uchun <b>HA</b> deb yozing:",
            parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return EP_CLEAR
    if text.upper() == "HA":
        kod = context.user_data.pop("clear_kod")
        title = context.user_data.pop("clear_title", "")
        conn = _db()
        conn.execute("DELETE FROM serial_qismlar WHERE anime_kod = ?", (kod,))
        conn.commit()
        conn.close()
        await update.message.reply_text(
            f"✅ <b>{title}</b> ning barcha qismlari o'chirildi.\n\n"
            f"Endi «🎞 Qism Qo'shish» orqali qismlarni qayta yuklang.",
            parse_mode=ParseMode.HTML, reply_markup=admin_keyboard())
    else:
        context.user_data.pop("clear_kod", None)
        context.user_data.pop("clear_title", None)
        await update.message.reply_text("❌ Bekor qilindi.", reply_markup=admin_keyboard())
    return ConversationHandler.END

# ============================== ADMIN: RO'YXAT / STATISTIKA ==============================

async def anime_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = _db()
    rows = conn.execute(
        "SELECT kod, title, is_premium, holat FROM animelar ORDER BY kod").fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text("ℹ️ Hozircha anime qo'shilmagan.", reply_markup=admin_keyboard())
        return
    lines = []
    for r in rows:
        conn = _db()
        cnt = conn.execute("SELECT COUNT(*) c FROM serial_qismlar WHERE anime_kod = ?",
                           (r["kod"],)).fetchone()["c"]
        conn.close()
        prem = " 💎" if r["is_premium"] else ""
        holat = r["holat"] or "tugatigan"
        lines.append(f"<b>{r['kod']}</b>. {r['title']}{prem} — {cnt} qism ({holat})")
    await update.message.reply_text("📋 <b>Anime ro'yxati:</b>\n\n" + "\n".join(lines),
                                    parse_mode=ParseMode.HTML, reply_markup=admin_keyboard())


async def statistika(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = _db()
    n_anime = conn.execute("SELECT COUNT(*) c FROM animelar").fetchone()["c"]
    n_ep = conn.execute("SELECT COUNT(*) c FROM serial_qismlar").fetchone()["c"]
    n_prem = conn.execute("SELECT COUNT(*) c FROM premium_users").fetchone()["c"]
    n_admin = conn.execute("SELECT COUNT(*) c FROM adminlar").fetchone()["c"]
    try:
        n_users = conn.execute("SELECT COUNT(*) c FROM foydalanuvchilar").fetchone()["c"]
    except Exception:
        n_users = 0
    conn.close()
    await update.message.reply_text(
        f"📊 <b>Statistika:</b>\n\n"
        f"🎬 Animelar: <b>{n_anime}</b>\n"
        f"🎞 Qismlar: <b>{n_ep}</b>\n"
        f"👥 Foydalanuvchilar: <b>{n_users}</b>\n"
        f"💎 Premium: <b>{n_prem}</b>\n"
        f"👑 Adminlar: <b>{n_admin + 1}</b>",
        parse_mode=ParseMode.HTML, reply_markup=admin_keyboard())


# ============================== ADMIN: ANIME TAHRIRLASH ==============================

def _edit_menu(kod):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Nomini o'zgartirish", callback_data=f"edit:title:{kod}"),
         InlineKeyboardButton("🖼 Videosini o'zgartirish", callback_data=f"edit:video:{kod}")],
        [InlineKeyboardButton("💎 Premium almashtirish", callback_data=f"edit:prem:{kod}"),
         InlineKeyboardButton("🎭 Holatni o'zgartirish", callback_data=f"edit:holat:{kod}")],
        [InlineKeyboardButton("🗑 Animeni o'chirish", callback_data=f"edit:del:{kod}")],
    ])


async def edit_kod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kod = (update.message.text or "").strip()
    conn = _db()
    a = conn.execute("SELECT kod, title, is_premium, holat FROM animelar WHERE kod = ?",
                     (kod,)).fetchone()
    if not a:
        conn.close()
        await update.message.reply_text("❌ Anime topilmadi!")
        return ConversationHandler.END
    cnt = conn.execute("SELECT COUNT(*) c FROM serial_qismlar WHERE anime_kod = ?",
                       (kod,)).fetchone()["c"]
    conn.close()
    prem = "💎 Premium" if a["is_premium"] else "Oddiy"
    await update.message.reply_text(
        f"✏️ <b>{a['title']}</b>\n\n"
        f"🔢 Kod: {a['kod']} | {prem} | {a['holat'] or 'tugatigan'} | {cnt} qism\n\n"
        f"Nimani o'zgartirasiz?",
        parse_mode=ParseMode.HTML, reply_markup=_edit_menu(a["kod"]))
    return ConversationHandler.END


async def edit_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        return
    parts = (q.data or "").split(":")
    if len(parts) < 3 or parts[0] != "edit":
        return
    action, kod = parts[1], parts[2]
    conn = _db()
    a = conn.execute("SELECT kod, title, is_premium, holat FROM animelar WHERE kod = ?",
                     (kod,)).fetchone()

    if action == "title":
        conn.close()
        if not a:
            await q.message.reply_text("❌ Anime topilmadi!")
            return ConversationHandler.END
        context.user_data["edit_kod"] = kod
        await q.message.reply_text(f"📝 <b>{a['title']}</b> uchun <b>yangi nomni</b> yozing:",
                                   parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return EDIT_TITLE

    if action == "video":
        conn.close()
        if not a:
            await q.message.reply_text("❌ Anime topilmadi!")
            return
        context.user_data["edit_kod"] = kod
        await q.message.reply_text(f"🖼 <b>{a['title']}</b> uchun <b>yangi videoni</b> yuboring:",
                                   parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return EDIT_VIDEO

    if action == "prem":
        if not a:
            conn.close()
            await q.message.reply_text("❌ Anime topilmadi!")
            return
        new_prem = 0 if a["is_premium"] else 1
        conn.execute("UPDATE animelar SET is_premium = ? WHERE kod = ?", (new_prem, kod))
        conn.commit()
        conn.close()
        hol = "💎 Premium" if new_prem else "Oddiy"
        await q.message.edit_text(f"✅ <b>{a['title']}</b> endi: <b>{hol}</b>",
                                  parse_mode=ParseMode.HTML, reply_markup=_edit_menu(kod))
        return

    if action == "holat":
        conn.close()
        if not a:
            await q.message.reply_text("❌ Anime topilmadi!")
            return
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🎬 Kino", callback_data=f"edit:hset:{kod}:kino"),
            InlineKeyboardButton("➡ Ongoing", callback_data=f"edit:hset:{kod}:ongoing"),
            InlineKeyboardButton("✅ Tugatigan", callback_data=f"edit:hset:{kod}:tugatigan"),
        ]])
        await q.message.edit_text(f"🎭 <b>{a['title']}</b> — yangi holatni tanlang:",
                                  parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if action == "hset" and len(parts) == 4:
        new_holat = parts[3]
        if not a:
            conn.close()
            await q.message.reply_text("❌ Anime topilmadi!")
            return
        conn.execute("UPDATE animelar SET holat = ? WHERE kod = ?", (new_holat, kod))
        conn.commit()
        conn.close()
        await q.message.edit_text(f"✅ <b>{a['title']}</b> holati: <b>{new_holat}</b>",
                                  parse_mode=ParseMode.HTML, reply_markup=_edit_menu(kod))
        return

    if action == "del":
        conn.close()
        if not a:
            await q.message.reply_text("❌ Anime topilmadi!")
            return
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Ha, o'chirish", callback_data=f"edit:delok:{kod}"),
            InlineKeyboardButton("❌ Bekor qilish", callback_data=f"edit:delno:{kod}"),
        ]])
        await q.message.edit_text(
            f"⚠️ <b>{a['title']}</b> va uning barcha qismlari o'chiriladi!\n\nAniqmi?",
            parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if action == "delok":
        title = a["title"] if a else ""
        conn.execute("DELETE FROM serial_qismlar WHERE anime_kod = ?", (kod,))
        conn.execute("DELETE FROM animelar WHERE kod = ?", (kod,))
        conn.commit()
        conn.close()
        await q.message.edit_text(f"✅ <b>{title}</b> o'chirildi.", parse_mode=ParseMode.HTML)
        return

    if action == "delno":
        conn.close()
        await q.message.edit_text("❌ Bekor qilindi.", reply_markup=None)
        return

    conn.close()


async def edit_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kod = context.user_data.pop("edit_kod", None)
    if not kod or not update.message.text:
        return ConversationHandler.END
    new_title = update.message.text.strip()
    conn = _db()
    conn.execute("UPDATE animelar SET title = ? WHERE kod = ?", (new_title, kod))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"✅ Nom yangilandi: <b>{new_title}</b>",
                                    parse_mode=ParseMode.HTML, reply_markup=admin_keyboard())
    return ConversationHandler.END


async def edit_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kod = context.user_data.get("edit_kod")
    if not kod or not update.message.video:
        await update.message.reply_text("❌ Iltimos, faqat video yuboring!")
        return EDIT_VIDEO
    fid = update.message.video.file_id
    conn = _db()
    conn.execute("UPDATE animelar SET file_id = ? WHERE kod = ?", (fid, kod))
    conn.commit()
    conn.close()
    context.user_data.pop("edit_kod", None)
    await update.message.reply_text("✅ Video yangilandi.", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def edit_video_warn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Iltimos, faqat video yuboring!")
    return EDIT_VIDEO


# ============================== ADMIN: BROADCAST ==============================

async def bc_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text:
        return BC_TEXT
    context.user_data["bc_text"] = text
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yuborish", callback_data="bc:yes"),
        InlineKeyboardButton("❌ Bekor qilish", callback_data="bc:no"),
    ]])
    await update.message.reply_text(
        f"📣 <b>Xabar ko'rinishi:</b>\n\n{text}\n\nBarchaga yuborilsinmi?",
        parse_mode=ParseMode.HTML, reply_markup=kb)
    return ConversationHandler.END


async def bc_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        return
    if q.data == "bc:no":
        context.user_data.pop("bc_text", None)
        await q.message.edit_text("❌ Bekor qilindi.")
        return
    text = context.user_data.pop("bc_text", None)
    if not text:
        await q.message.edit_text("❌ Xabar matni topilmadi.")
        return
    conn = _db()
    users = [r["user_id"] for r in conn.execute("SELECT user_id FROM foydalanuvchilar").fetchall()]
    conn.close()
    ok, fail = 0, 0
    await q.message.edit_text("⏳ Yuborilmoqda...")
    for uid in users:
        try:
            await context.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML)
            ok += 1
        except Exception:
            fail += 1
    await q.message.edit_text(f"✅ Broadcast tugadi: <b>{ok}</b> yetkazildi, <b>{fail}</b> xato.",
                              parse_mode=ParseMode.HTML)


# ============================== ADMIN: PREMIUM BERISH ==============================

async def prem_give(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    target_id = None
    nick = ""
    conn = _db()
    if text.startswith("@"):
        nick = text[1:].strip().lower()
        row = conn.execute("SELECT user_id, first_name FROM foydalanuvchilar WHERE LOWER(username) = ?",
                           (nick,)).fetchone()
        if row:
            target_id = row["user_id"]
    elif text.isdigit():
        target_id = int(text)
        row = conn.execute("SELECT first_name, username FROM foydalanuvchilar WHERE user_id = ?",
                           (target_id,)).fetchone()
        if row:
            nick = row["username"] or ""
    conn.close()
    if not target_id:
        await update.message.reply_text(
            "❌ Foydalanuvchi topilmadi! U botni <b>/start</b> orqali ishga tushirgan bo'lishi kerak.",
            parse_mode=ParseMode.HTML, reply_markup=back_keyboard())
        return PREM_GIVE
    context.user_data["prem_target"] = target_id
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("1 oylik", callback_data=f"premgive:{target_id}:1"),
         InlineKeyboardButton("3 oylik", callback_data=f"premgive:{target_id}:3")],
        [InlineKeyboardButton("6 oylik", callback_data=f"premgive:{target_id}:6"),
         InlineKeyboardButton("12 oylik", callback_data=f"premgive:{target_id}:12")],
    ])
    who = f"@{nick}" if nick else f"ID {target_id}"
    await update.message.reply_text(
        f"🎁 <b>{who}</b> ga qancha muddatga Premium berilsin?",
        parse_mode=ParseMode.HTML, reply_markup=kb)
    return ConversationHandler.END


async def premgive_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        return
    parts = (q.data or "").split(":")
    if len(parts) != 3 or parts[0] != "premgive":
        return
    target_id, months = int(parts[1]), int(parts[2])
    until = (datetime.now() + timedelta(days=30 * months)).strftime(DATETIME_FMT)
    conn = _db()
    conn.execute("INSERT OR REPLACE INTO premium_users (user_id, tugash_vaqti) VALUES (?, ?)",
                 (target_id, until))
    conn.commit()
    conn.close()
    try:
        await context.bot.send_message(
            target_id,
            f"🎉 <b>Tabriklaymiz! Sizga {months} oylik Premium sovg'a qilindi!</b>\n📅 Muddat: {until}",
            parse_mode=ParseMode.HTML)
        sent = True
    except Exception:
        sent = False
    await q.message.edit_text(
        f"✅ <b>ID {target_id}</b> ga <b>{months} oylik</b> Premium berildi.\n📅 Muddat: {until}"
        + ("" if sent else "\n⚠️ Foydalanuvchiga xabar yuborilmadi."),
        parse_mode=ParseMode.HTML)


# ============================== ADMIN: POST / ADMIN / KANAL ==============================

async def post_kod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return POST_KOD
    kod = update.message.text.strip()
    conn = _db()
    anime = conn.execute("SELECT file_id, title, is_premium FROM animelar WHERE kod = ?", (kod,)).fetchone()
    if not anime:
        conn.close()
        await update.message.reply_text("❌ Bunday kodli anime topilmadi!")
        return ConversationHandler.END
    qism_soni = conn.execute(
        "SELECT COUNT(id) FROM serial_qismlar WHERE anime_kod = ?", (kod,)).fetchone()[0]
    conn.close()

    bot_username = (await context.bot.get_me()).username
    tag = " [💎 PREMIUM]" if anime["is_premium"] else ""
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🎬 Tomosha qilish", url=f"tg://resolve?domain={bot_username}&start={kod}")
    ]])
    caption = (f"🎬 <b>ANIME BARCHA QISMLARI JOYLANDI!{tag}</b> 🎉\n"
               f"━━━━━━━━━━━━━━━\n"
               f"📌 <b>Nomi:</b> {anime['title']}\n"
               f"🔢 <b>Jami qismlar:</b> {qism_soni} ta\n"
               f"🔑 <b>Bot kodi:</b> <code>{kod}</code>\n"
               f"🤖 <b>Bot:</b> @{bot_username}\n"
               f"━━━━━━━━━━━━━━━\n\n"
               "👇 Barcha qismlarni bitta joyda tomosha qilish uchun tugmani bosing:")
    targets = get_post_channels() or [POST_CHANNEL.lstrip("@")]
    ok, fails = [], []
    for ch in targets:
        try:
            await context.bot.send_photo(
                chat_id="@" + ch.lstrip("@"), photo=anime["file_id"],
                caption=caption, parse_mode=ParseMode.HTML, reply_markup=kb)
            ok.append("@" + ch.lstrip("@"))
        except Exception as e:
            fails.append(f"@{ch.lstrip('@')} — {e}")
    msg = ""
    if ok:
        msg += "✅ Post yuborildi: " + ", ".join(ok) + "\n"
    if fails:
        msg += "❌ Ketmadi (bot shu kanalda admin bo'lishi kerak):\n" + "\n".join(fails)
    await update.message.reply_text(msg.strip(), reply_markup=admin_keyboard())
    return ConversationHandler.END


async def ad_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return AD_ADD
    try:
        new_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ ID raqam xato!")
        return AD_ADD
    conn = _db()
    conn.execute("INSERT OR IGNORE INTO adminlar (user_id) VALUES (?)", (new_id,))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"✅ Yangi admin qo'shildi! ID: <code>{new_id}</code>",
                                    parse_mode=ParseMode.HTML, reply_markup=admin_keyboard())
    return ConversationHandler.END


async def ad_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return AD_DEL
    try:
        del_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ ID raqam xato!")
        return AD_DEL
    if del_id == SUPER_ADMIN_ID:
        await update.message.reply_text("⛔ Asosiy adminni o'chirib bo'lmaydi!",
                                        reply_markup=admin_keyboard())
        return ConversationHandler.END
    conn = _db()
    cur = conn.execute("DELETE FROM adminlar WHERE user_id = ?", (del_id,))
    conn.commit()
    conn.close()
    if cur.rowcount:
        await update.message.reply_text(f"🗑 Admin o'chirildi! ID: <code>{del_id}</code>",
                                        parse_mode=ParseMode.HTML, reply_markup=admin_keyboard())
    else:
        await update.message.reply_text("❌ Bunday ID'li admin topilmadi!",
                                        reply_markup=admin_keyboard())
    return ConversationHandler.END


async def db_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Kanal qo'shish/o'chirish tanlandi (entry point)."""
    q = update.callback_query
    await q.answer()
    if q.data == "db_add_channel":
        await q.message.reply_text("✍️ Yangi kanal username'ini yozing (boshiga @ qo'ymang):",
                                   reply_markup=back_keyboard())
        return CH_ADD
    if q.data == "db_add_post_channel":
        await q.message.reply_text("✍️ Post yuboriladigan kanal username'ini yozing (@ bilan yoki @ siz):\n\n"
                                   "⚠️ Avval botni shu kanalga admin qilib qo'shing!",
                                   reply_markup=back_keyboard())
        return PCH_ADD
    if q.data == "db_del_post_channel":
        await q.message.reply_text("✍️ Post ro'yxatidan o'chirmoqchi bo'lgan kanal username'ini yozing:",
                                   reply_markup=back_keyboard())
        return PCH_DEL
    await q.message.reply_text("✍️ O'chirmoqchi bo'lgan kanal username'ini yozing:",
                               reply_markup=back_keyboard())
    return CH_DEL


async def ch_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return CH_ADD
    ch = update.message.text.strip().lstrip("@")
    conn = _db()
    conn.execute("INSERT OR IGNORE INTO channels (username) VALUES (?)", (ch,))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"✅ @{ch} ro'yxatga qo'shildi!", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def ch_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return CH_DEL
    ch = update.message.text.strip().lstrip("@")
    conn = _db()
    conn.execute("DELETE FROM channels WHERE username = ?", (ch,))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"🗑 @{ch} o'chirildi!", reply_markup=admin_keyboard())
    return ConversationHandler.END


async def pch_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return PCH_ADD
    ch = update.message.text.strip().lstrip("@")
    conn = _db()
    conn.execute("INSERT OR IGNORE INTO post_channels (username) VALUES (?)", (ch,))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"✅ Post kanali qo'shildi: @{ch}\n\n"
                                    "⚠️ Bot shu kanalga admin bo'lishi kerak, aks holda post ketmaydi.",
                                    reply_markup=admin_keyboard())
    return ConversationHandler.END


async def pch_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.text:
        return PCH_DEL
    ch = update.message.text.strip().lstrip("@")
    conn = _db()
    conn.execute("DELETE FROM post_channels WHERE username = ?", (ch,))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"🗑 Post kanali o'chirildi: @{ch}", reply_markup=admin_keyboard())
    return ConversationHandler.END


# ============================== CALLBACK'LAR ==============================

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ""
    await q.answer()
    uid = q.from_user.id

    if data == "check_sub":
        if await check_subscription(uid, context):
            await q.message.delete()
            await q.message.reply_text("<b>Rahmat! Obuna tasdiqlandi. 🎉</b>",
                                       parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
        else:
            await q.answer("Siz hali barcha kanallarga a'zo bo'lmadingiz! ❌", show_alert=True)
        return

    if data.startswith("view_"):
        kod = data.split("_", 1)[1]
        await _display_anime(q.message.chat.id, uid, kod, context)
        return

    if data.startswith("ainfo_"):
        try:
            al_id = int(data.split("_", 1)[1])
            d = await anilist(DETAIL_QUERY, {"id": al_id})
            m = d["Media"]
        except Exception:
            await q.answer("❌ Ma'lumot olinmadi.", show_alert=True)
            return
        caption = al_card(m)
        kb_rows = []
        tr = m.get("trailer") or {}
        if tr.get("site") == "youtube" and tr.get("id"):
            kb_rows.append([InlineKeyboardButton(
                "🎬 Treylerni ko'rish", url=f"https://www.youtube.com/watch?v={tr['id']}")])
        if m.get("siteUrl"):
            kb_rows.append([InlineKeyboardButton("🌐 AniList'da ochish", url=m["siteUrl"])])
        try:
            await context.bot.send_photo(
                chat_id=q.message.chat.id, photo=m["coverImage"]["large"],
                caption=caption, parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(kb_rows) if kb_rows else None)
        except Exception:
            await q.message.reply_text(caption, parse_mode=ParseMode.HTML,
                                       reply_markup=InlineKeyboardMarkup(kb_rows) if kb_rows else None)
        return

    if data.startswith("p_") and uid == SUPER_ADMIN_ID:
        parts = data.split("_")
        action, target_id = parts[1], int(parts[2])
        if action == "accept":
            months = int(parts[3])
            until = (datetime.now() + timedelta(days=30 * months)).strftime(DATETIME_FMT)
            conn = _db()
            conn.execute("INSERT OR REPLACE INTO premium_users (user_id, tugash_vaqti) VALUES (?, ?)",
                         (target_id, until))
            conn.commit()
            conn.close()
            try:
                await context.bot.send_message(
                    target_id, f"🎉 <b>Premium obunangiz {months} oyga faollashtirildi!</b>\n📅 Muddat: {until}",
                    parse_mode=ParseMode.HTML)
            except Exception:
                pass
            try:
                await q.message.edit_caption(caption=q.message.caption + "\n\n✅ <b>Tasdiqlandi!</b>",
                                             parse_mode=ParseMode.HTML)
            except Exception:
                pass
        elif action == "reject":
            try:
                await context.bot.send_message(target_id, "❌ To'lovingiz tasdiqlanmadi.")
            except Exception:
                pass
            try:
                await q.message.edit_caption(caption=q.message.caption + "\n\n❌ <b>Rad etildi!</b>",
                                             parse_mode=ParseMode.HTML)
            except Exception:
                pass
        return

    if data.startswith("play_"):
        _, kod, qism = data.split("_")
        conn = _db()
        anime = conn.execute("SELECT is_premium FROM animelar WHERE kod = ?", (kod,)).fetchone()
        if anime and anime["is_premium"] and not is_premium_active(uid):
            conn.close()
            await q.answer("🔒 Premium obuna kerak!", show_alert=True)
            return
        row = conn.execute(
            "SELECT file_id FROM serial_qismlar WHERE anime_kod = ? AND qism_raqam = ?",
            (kod, qism)).fetchone()
        conn.close()
        if row:
            await context.bot.send_video(chat_id=q.message.chat.id, video=row["file_id"],
                                         caption=f"🎞 {qism}-qism")
        return

    if data.startswith("all_download_"):
        kod = data.split("_")[2]
        conn = _db()
        qismlar = conn.execute(
            "SELECT qism_raqam, file_id FROM serial_qismlar WHERE anime_kod = ? ORDER BY qism_raqam ASC",
            (kod,)).fetchall()
        conn.close()
        if not qismlar:
            await q.answer("Qismlar topilmadi! ❌", show_alert=True)
            return
        await q.answer("Barcha qismlar yuborilmoqda... 📥")
        import asyncio
        for r in qismlar:
            await context.bot.send_video(chat_id=q.message.chat.id, video=r["file_id"],
                                         caption=f"🎞 {r['qism_raqam']}-qism")
            await asyncio.sleep(0.5)
        return

    if data == "close_message":
        await q.message.delete()
        return


# ============================== XATO ==============================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Xatolik: %s", context.error, exc_info=context.error)
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text(
                "❌ Xatolik yuz berdi. /cancel ni bosing va qaytadan urinib ko'ring.")
    except Exception:
        pass


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Bekor qilindi.", reply_markup=main_keyboard())
    return ConversationHandler.END


# ============================== ISHGA TUSHIRISH ==============================

# ============================== MEDIA KO'CHIRISH (qabul) ==============================
_mig_state = {"posters": 0, "episodes": 0}


async def mig_receive(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Kanal orqali asosiy botdan kelgan fayllarni yangi bot nomiga yozish."""
    msg = update.channel_post
    if msg is None:
        return
    cap = msg.caption or ""
    conn = None
    try:
        if cap.startswith("MIGP|") and msg.photo:
            kod = cap.split("|", 1)[1].strip()
            conn = _db()
            conn.execute("UPDATE animelar SET file_id = ? WHERE kod = ?",
                         (msg.photo[-1].file_id, kod))
            conn.commit()
            _mig_state["posters"] += 1
        elif cap.startswith("MIG|") and msg.video:
            parts = cap.split("|")
            if len(parts) == 3:
                _, kod, qism = parts
                conn = _db()
                conn.execute(
                    "UPDATE serial_qismlar SET file_id = ? WHERE anime_kod = ? AND qism_raqam = ?",
                    (msg.video.file_id, kod.strip(), int(qism)))
                conn.commit()
                _mig_state["episodes"] += 1
        elif (msg.text or "").startswith("MIGDONE"):
            try:
                await context.bot.send_message(
                    SUPER_ADMIN_ID,
                    "✅ Media ko'chirish tugadi.\n"
                    f"Posterlar: {_mig_state['posters']}\n"
                    f"Qismlar: {_mig_state['episodes']}\n"
                    f"Asosiy bot xabari: {msg.text}")
            except Exception:
                pass
    except Exception as e:
        log.warning("mig_receive xato: %s", e)
    finally:
        if conn is not None:
            conn.close()


def build_app():
    init_db()
    builder = ApplicationBuilder().token(TOKEN)
    if PROXY_URL:
        builder = builder.proxy(PROXY_URL).get_updates_proxy(PROXY_URL)
    app = builder.build()

    menu_first = MessageHandler(filters.Regex(MENU_PATTERN), menu_router)
    txt = filters.TEXT & ~filters.COMMAND

    conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex(ADMIN_PATTERN), admin_entry),
            MessageHandler(filters.Regex(USER_PATTERN), user_text_router),
            CallbackQueryHandler(buy_cb, pattern="^buy_"),
            CallbackQueryHandler(db_cb, pattern="^db_"),
            CallbackQueryHandler(edit_cb, pattern="^edit:"),
            CallbackQueryHandler(bc_cb, pattern="^bc:"),
            CallbackQueryHandler(premgive_cb, pattern="^premgive:"),
        ],
        states={
            S_SEARCH: [menu_first, MessageHandler(txt, s_search)],
            ANI_INFO: [menu_first, MessageHandler(txt, ani_info)],
            P_CHECK: [menu_first,
                      MessageHandler(filters.PHOTO, p_check_photo),
                      MessageHandler(txt, p_check_warn)],
            A_TITLE: [menu_first, MessageHandler(txt, a_title)],
            A_KIND: [CallbackQueryHandler(a_kind, pattern="^type_"), menu_first],
            A_VIDEO: [menu_first, MessageHandler(filters.PHOTO, a_video)],
            A_HOLAT: [CallbackQueryHandler(a_holat, pattern="^holat_"), menu_first],
            EP_KOD: [menu_first, MessageHandler(txt, ep_kod)],
            EP_VIDEO: [menu_first, MessageHandler(filters.VIDEO, ep_video),
                       MessageHandler(txt, ep_video_warn)],
            EP_CLEAR: [menu_first, MessageHandler(txt, ep_clear)],
            EP_REP_KOD: [menu_first, MessageHandler(txt, ep_rep_kod)],
            EP_REP_QISM: [menu_first, MessageHandler(txt, ep_rep_qism)],
            EP_REP_VIDEO: [menu_first, MessageHandler(filters.VIDEO, ep_rep_video),
                           MessageHandler(txt, ep_rep_video_warn)],
            SHORT_KOD: [menu_first, MessageHandler(txt, short_kod)],
            SHORT_VIDEO: [menu_first, MessageHandler(filters.VIDEO, short_video),
                          MessageHandler(txt, short_video_warn)],
            SHORT_TITLE: [menu_first, MessageHandler(txt, short_title)],
            SHORT_DEL_ID: [menu_first, MessageHandler(txt, short_del_id)],
            EDIT_KOD: [menu_first, MessageHandler(txt, edit_kod)],
            EDIT_TITLE: [menu_first, MessageHandler(txt, edit_title)],
            EDIT_VIDEO: [menu_first, MessageHandler(filters.VIDEO, edit_video),
                         MessageHandler(txt, edit_video_warn)],
            BC_TEXT: [menu_first, MessageHandler(txt, bc_text)],
            PREM_GIVE: [menu_first, MessageHandler(txt, prem_give)],
            POST_KOD: [menu_first, MessageHandler(txt, post_kod)],
            AD_ADD: [menu_first, MessageHandler(txt, ad_add)],
            AD_DEL: [menu_first, MessageHandler(txt, ad_del)],
            CH_ADD: [menu_first, MessageHandler(txt, ch_add)],
            CH_DEL: [menu_first, MessageHandler(txt, ch_del)],
            PCH_ADD: [menu_first, MessageHandler(txt, pch_add)],
            PCH_DEL: [menu_first, MessageHandler(txt, pch_del)],
        },
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
        allow_reentry=True,
    )

    # /start va /admin suhbatdan tashqarida ham ishlaydi
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(conv)
    # Zayavkali kanallar: so'rovlarni avtomatik tasdiqlash
    app.add_handler(ChatJoinRequestHandler(join_request_handler))
    # Suhbat bo'lmaganda: user tugmalari va Orqaga
    app.add_handler(MessageHandler(filters.Regex("^.*Orqaga.*$"), menu_router))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POST, mig_receive))
    app.add_error_handler(error_handler)
    return app


def _pyro_thread_main():
    """Pyrogram'ni alohida oqimda ishga tushiradi."""
    global _pyro_loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _pyro_loop = loop
    try:
        loop.run_until_complete(_pyro_start())
        loop.run_forever()
    except Exception as e:
        log.warning("Pyrogram oqimi to'xtadi: %s", e)


def main():
    if not TOKEN:
        print("❌ BOT_TOKEN topilmadi.")
        return
    # MTProto (katta videolar uchun) — alohida oqimda
    import threading
    threading.Thread(target=_pyro_thread_main, daemon=True).start()
    app = build_app()
    print("Uzbek Animelar Olami (PTB) boti ishga tushdi...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
