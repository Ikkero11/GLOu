"""
bot.py — Анти-стресс v0.6.0
Изменения:
  - Добавлено отслеживание блокировок бота юзерами (is_blocked)
  - Изменена команда /admin_users (убран @username, добавлена пометка о блокировке)
  - Добавлена команда /admin_blocked
"""

import asyncio, csv, io, logging, os, random, re, sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile, CallbackQuery, FSInputFile,
    InlineKeyboardButton, KeyboardButton, Message,
    ReplyKeyboardMarkup, ReplyKeyboardRemove,
    ChatMemberUpdated  # ДОБАВЛЕНО
)
from aiogram.filters.chat_member_updated import ChatMemberUpdatedFilter, KICKED, MEMBER  # ДОБАВЛЕНО
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ================================================================
#  КОНФИГУРАЦИЯ
# ================================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE")
ADMIN_IDS = [7498442456, 1220845157]
VERSION = "0.7"
BOT_NAME = "Анти-стресс"
TIMEZONE = "Europe/Moscow"
MSK = pytz.timezone(TIMEZONE)

ZONE_GREEN = (8, 16)
ZONE_YELLOW = (17, 28)
ZONE_RED = (29, 40)

COOLDOWN_EXPRESS = 3600
COOLDOWN_BREATHING = 1800
COOLDOWN_DIAGNOSTIC = 28800  # 12 часов — 2 раза в сутки

POINTS_SURVEY = 15
POINTS_STREAK_BONUS = 10
POINTS_EXPRESS = 10
POINTS_BREATH_FIRST = 5
POINTS_BREATH_NEXT = 1
POINTS_DIAGNOSTIC = 25

TRIGGER_VALUE = 5
RED_STREAK_ALERT = 3

FACTS_FILE = "facts_day.txt"
QUOTES_FILE = "quotes_day.txt"
MOON_DIR = "moon_photos"
MORNING_DIR = "morning_images"
BREATHING_DIR = "breathing_images"
PRACTICE_DIR = "practice_images"
FACTS_TIME = "13:30"
QUOTE_TIME = "16:30"
WEEKLY_DAY = "fri"  # день еженедельной рассылки статистики
WEEKLY_TIME = "10:00"

DB_PATH = os.getenv("DB_PATH", "/data/antistress.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ================================================================
#  БАЗА ДАННЫХ
# ================================================================

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id           INTEGER PRIMARY KEY,
            username          TEXT,
            first_name        TEXT,
            gender            TEXT,
            points            INTEGER DEFAULT 0,
            streak            INTEGER DEFAULT 0,
            last_survey_date  TEXT,
            survey_time       TEXT DEFAULT '20:00',
            morning_time      TEXT,
            red_zone_streak   INTEGER DEFAULT 0,
            registered_at     TEXT DEFAULT (datetime('now')),
            is_blocked        INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS moods (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            score      INTEGER NOT NULL,
            zone       TEXT    NOT NULL,
            mood_type  TEXT    NOT NULL,
            created_at TEXT    DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );
        CREATE TABLE IF NOT EXISTS mood_details (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            mood_id      INTEGER NOT NULL,
            question_num INTEGER NOT NULL,
            answer       INTEGER NOT NULL,
            FOREIGN KEY (mood_id) REFERENCES moods(id)
        );
        CREATE TABLE IF NOT EXISTS daily_tasks (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id       INTEGER NOT NULL,
            task_date     TEXT    NOT NULL,
            task_type     TEXT    NOT NULL,
            points_earned INTEGER DEFAULT 0,
            done_at       TEXT    DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );
        CREATE TABLE IF NOT EXISTS diagnostics (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER NOT NULL,
            score        INTEGER NOT NULL,
            level        TEXT    NOT NULL,
            answers      TEXT    NOT NULL,
            created_at   TEXT    DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );
        CREATE TABLE IF NOT EXISTS weekly_sub (
            user_id  INTEGER PRIMARY KEY,
            active   INTEGER DEFAULT 1
        );
        """)

        # Безопасное добавление столбца is_blocked, если БД уже существовала до обновления
        try:
            conn.execute("ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass

    logger.info("БД готова: %s", DB_PATH)


def upsert_user(user_id, username, first_name):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO users (user_id, username, first_name) VALUES (?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username, first_name=excluded.first_name
        """, (user_id, username or "", first_name or ""))


def get_user(user_id):
    with get_conn() as conn:
        return conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()


def set_gender(user_id, gender):
    with get_conn() as conn:
        conn.execute("UPDATE users SET gender=? WHERE user_id=?", (gender, user_id))


def set_survey_time(user_id, t):
    with get_conn() as conn:
        conn.execute("UPDATE users SET survey_time=? WHERE user_id=?", (t, user_id))


def set_morning_time(user_id, t):
    with get_conn() as conn:
        conn.execute("UPDATE users SET morning_time=? WHERE user_id=?", (t, user_id))


def add_points(user_id, pts):
    with get_conn() as conn:
        conn.execute("UPDATE users SET points=points+? WHERE user_id=?", (pts, user_id))
        row = conn.execute("SELECT points FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row["points"] if row else 0


def set_points_value(user_id, pts):
    with get_conn() as conn:
        conn.execute("UPDATE users SET points=? WHERE user_id=?", (pts, user_id))


def get_all_users():
    with get_conn() as conn:
        return conn.execute("SELECT * FROM users").fetchall()


def get_top_position(user_id):
    """Возвращает позицию пользователя в рейтинге по очкам."""
    with get_conn() as conn:
        row = conn.execute("""
            SELECT COUNT(*) + 1 AS pos FROM users
            WHERE points > (SELECT points FROM users WHERE user_id=?)
        """, (user_id,)).fetchone()
        total = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        return (row["pos"] if row else 1), total


def get_users_by_survey_time(t):
    with get_conn() as conn:
        return conn.execute("SELECT * FROM users WHERE survey_time=?", (t,)).fetchall()


def get_users_by_morning_time(t):
    with get_conn() as conn:
        return conn.execute("SELECT * FROM users WHERE morning_time=?", (t,)).fetchall()


def save_mood(user_id, score, zone, mood_type, answers):
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO moods (user_id,score,zone,mood_type) VALUES (?,?,?,?)",
            (user_id, score, zone, mood_type)
        )
        mid = cur.lastrowid
        if answers:
            conn.executemany(
                "INSERT INTO mood_details (mood_id,question_num,answer) VALUES (?,?,?)",
                [(mid, i + 1, a) for i, a in enumerate(answers)]
            )
        return mid


def get_last_moods(user_id, mood_type, limit=7):
    with get_conn() as conn:
        return conn.execute("""
            SELECT score,zone,created_at FROM moods
            WHERE user_id=? AND mood_type=?
            ORDER BY created_at DESC LIMIT ?
        """, (user_id, mood_type, limit)).fetchall()


def get_last_mood_dt(user_id, mood_type):
    with get_conn() as conn:
        row = conn.execute("""
            SELECT created_at FROM moods
            WHERE user_id=? AND mood_type=?
            ORDER BY created_at DESC LIMIT 1
        """, (user_id, mood_type)).fetchone()
        return datetime.fromisoformat(row["created_at"]) if row else None


def update_streak(user_id):
    today = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    with get_conn() as conn:
        u = conn.execute(
            "SELECT last_survey_date,streak,red_zone_streak FROM users WHERE user_id=?",
            (user_id,)
        ).fetchone()
        last = u["last_survey_date"] if u else None
        streak = u["streak"] if u else 0
        if last == today:
            return streak
        streak = (streak + 1) if last == yesterday else 1
        row = conn.execute("""
            SELECT zone FROM moods WHERE user_id=? AND mood_type='main'
            ORDER BY created_at DESC LIMIT 1
        """, (user_id,)).fetchone()
        red = u["red_zone_streak"] if u else 0
        red = (red + 1) if (row and row["zone"] == "red") else 0
        conn.execute(
            "UPDATE users SET streak=?,last_survey_date=?,red_zone_streak=? WHERE user_id=?",
            (streak, today, red, user_id)
        )
        return streak


def get_red_streak(user_id):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT red_zone_streak FROM users WHERE user_id=?", (user_id,)
        ).fetchone()
        return row["red_zone_streak"] if row else 0


def set_streak(user_id, streak: int):
    """Устанавливает серию дней напрямую (для админа)."""
    today = date.today().isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET streak=?, last_survey_date=? WHERE user_id=?",
            (max(0, streak), today, user_id)
        )


def task_done_today(user_id, task_type):
    today = date.today().isoformat()
    with get_conn() as conn:
        return conn.execute("""
            SELECT id FROM daily_tasks
            WHERE user_id=? AND task_date=? AND task_type=? LIMIT 1
        """, (user_id, today, task_type)).fetchone() is not None


def log_task(user_id, task_type, pts):
    today = date.today().isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO daily_tasks (user_id,task_date,task_type,points_earned) VALUES (?,?,?,?)",
            (user_id, today, task_type, pts)
        )


def get_today_tasks(user_id):
    today = date.today().isoformat()
    with get_conn() as conn:
        return conn.execute("""
            SELECT task_type,points_earned FROM daily_tasks
            WHERE user_id=? AND task_date=? ORDER BY done_at
        """, (user_id, today)).fetchall()


def admin_general_stats():
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        active = conn.execute("""
            SELECT COUNT(DISTINCT user_id) AS c FROM moods
            WHERE created_at >= datetime('now','-7 days')
        """).fetchone()["c"]
        avg_sc = conn.execute("""
            SELECT ROUND(AVG(score),1) AS a FROM moods WHERE mood_type='main'
        """).fetchone()["a"]
        zones = conn.execute("""
            SELECT zone,COUNT(*) AS c FROM moods WHERE mood_type='main' GROUP BY zone
        """).fetchall()
        genders = conn.execute(
            "SELECT gender,COUNT(*) AS c FROM users GROUP BY gender"
        ).fetchall()
    return {
        "total": total, "active_7d": active, "avg_score": avg_sc,
        "zones": {r["zone"]: r["c"] for r in zones},
        "genders": {(r["gender"] or "не указан"): r["c"] for r in genders},
    }


def admin_all_users():
    with get_conn() as conn:
        return conn.execute("""
            SELECT user_id,username,first_name,gender,
                   points,streak,survey_time,morning_time,is_blocked
            FROM users ORDER BY points DESC
        """).fetchall()


def set_user_blocked(user_id: int, is_blocked: int):
    with get_conn() as conn:
        conn.execute("UPDATE users SET is_blocked=? WHERE user_id=?", (is_blocked, user_id))


def get_blocked_users():
    with get_conn() as conn:
        return conn.execute("""
            SELECT user_id, first_name, username 
            FROM users WHERE is_blocked=1
        """).fetchall()


def export_moods_csv(days=30):
    with get_conn() as conn:
        return conn.execute("""
            SELECT m.id,m.user_id,u.username,m.score,m.zone,m.mood_type,m.created_at
            FROM moods m LEFT JOIN users u ON u.user_id=m.user_id
            WHERE m.created_at >= datetime('now', ? || ' days')
            ORDER BY m.created_at DESC
        """, (f"-{days}",)).fetchall()


def save_diagnostic(user_id, score, level, answers: list):
    import json
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO diagnostics (user_id,score,level,answers) VALUES (?,?,?,?)",
            (user_id, score, level, json.dumps(answers, ensure_ascii=False))
        )


def get_last_diagnostic_dt(user_id):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT created_at FROM diagnostics WHERE user_id=? ORDER BY created_at DESC LIMIT 1",
            (user_id,)
        ).fetchone()
        return datetime.fromisoformat(row["created_at"]) if row else None


def get_diagnostic_count(user_id):
    with get_conn() as conn:
        return conn.execute(
            "SELECT COUNT(*) AS c FROM diagnostics WHERE user_id=?", (user_id,)
        ).fetchone()["c"]


def get_last_diagnostics(user_id, limit=5):
    with get_conn() as conn:
        return conn.execute(
            "SELECT score,level,created_at FROM diagnostics WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        ).fetchall()


def get_weekly_stats(user_id):
    """Статистика за последние 7 дней для еженедельного отчёта."""
    with get_conn() as conn:
        avg = conn.execute("""
            SELECT ROUND(AVG(score),1) AS a FROM moods
            WHERE user_id=? AND mood_type='main'
            AND created_at >= datetime('now','-7 days')
        """, (user_id,)).fetchone()["a"]
        avg_prev = conn.execute("""
            SELECT ROUND(AVG(score),1) AS a FROM moods
            WHERE user_id=? AND mood_type='main'
            AND created_at BETWEEN datetime('now','-14 days') AND datetime('now','-7 days')
        """, (user_id,)).fetchone()["a"]
        count = conn.execute("""
            SELECT COUNT(*) AS c FROM moods
            WHERE user_id=? AND mood_type='main'
            AND created_at >= datetime('now','-7 days')
        """, (user_id,)).fetchone()["c"]
        zones = conn.execute("""
            SELECT zone, COUNT(*) AS c FROM moods
            WHERE user_id=? AND mood_type='main'
            AND created_at >= datetime('now','-7 days')
            GROUP BY zone
        """, (user_id,)).fetchall()
    return {
        "avg": avg, "avg_prev": avg_prev, "count": count,
        "zones": {r["zone"]: r["c"] for r in zones}
    }


def get_weekly_sub_users():
    with get_conn() as conn:
        return conn.execute("""
            SELECT u.user_id, u.first_name FROM users u
            JOIN weekly_sub ws ON ws.user_id = u.user_id
            WHERE ws.active = 1
        """).fetchall()


def set_weekly_sub(user_id, active: bool):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO weekly_sub (user_id, active) VALUES (?,?)
            ON CONFLICT(user_id) DO UPDATE SET active=excluded.active
        """, (user_id, 1 if active else 0))


def get_weekly_sub_status(user_id):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT active FROM weekly_sub WHERE user_id=?", (user_id,)
        ).fetchone()
        return bool(row["active"]) if row else False


# ================================================================
#  ТЕКСТЫ
# ================================================================

WELCOME = (
    f"Привет! Я *{BOT_NAME}* 🤍\n\n"
    "Помогу отслеживать уровень стресса и находить баланс — "
    "каждый день, в твоём темпе.\n\n"
    "Сначала пара вопросов для старта 👇"
)

# Ссылки на документы (Teletype)
PRIVACY_URL = "https://teletype.in/@antists_bot/privacy"
AGREEMENT_URL = "https://teletype.in/@antists_bot/agreement"

ASK_AGREEMENT = (
    "📄 Перед началом ознакомься с документами:\n\n"
    "Нажимая «Принять», ты соглашаешься с условиями использования."
)


def agreement_kb():
    b = InlineKeyboardBuilder()
    b.button(text="📋 Пользовательское соглашение", url=AGREEMENT_URL)
    b.button(text="🔒 Политика конфиденциальности", url=PRIVACY_URL)
    b.button(text="✅ Принять и продолжить", callback_data="agreement:accept")
    b.adjust(1)
    return b.as_markup()


ASK_GENDER = (
    "👤 Укажи свой пол — только для анонимной статистики.\n"
    "_Никто кроме тебя не увидит эти данные._"
)
GENDER_SAVED = "Сохранил ✅\n\nХочешь прямо сейчас пройти пробный вечерний опрос?"
ASK_SURVEY_TIME = "⏰ В какое время тебе удобно проходить ежедневный опрос?\n\nНапиши время в формате *ЧЧ:ММ*, например `20:00`"
ASK_MORNING_TIME = "🌅 Хочешь получать утреннюю карточку?\n\nНапиши время в формате *ЧЧ:ММ* или /skip чтобы пропустить."
SETUP_DONE = "🎉 Всё готово! Ты в игре.\n\nПользуйся меню ниже 👇"
TIME_INVALID = "⚠️ Неверный формат. Попробуй ещё раз, например: `20:30`"
TIME_SAVED = "✅ Время сохранено: *{time}*"
TIME_MORNING_OFF = "✅ Утренняя рассылка отключена."
TIME_SETTINGS = "⏰ *Настройка времени*\n\n🌆 Вечерний опрос: *{survey_time}*\n🌅 Утренняя рассылка: *{morning_time}*"

HELP_TEXT = (
    f"ℹ️ *{BOT_NAME}* v{VERSION}\n\n"
    "*Главное меню:*\n"
    "📊 Статистика — очки, серия, задания, экспресс-тест, кабинет, диагностика\n"
    "🌿 Практики — дыхание с картинками, упражнения, фаза луны\n"
    "ℹ️ О боте — справка и настройка времени\n\n"
    "*Команды:*\n"
    "/start — запуск и регистрация\n"
    "/help — эта справка\n"
    "/cabinet — личный кабинет\n"
    "/start\\_diagnostic — глубокий опрос (20 вопросов)\n"
    "/menu\\_hide — скрыть кнопки меню\n"
    "/menu\\_show — показать кнопки меню\n\n"
    "*Как работает бот:*\n"
    "Каждый вечер в заданное время — опрос из 8 вопросов. "
    "По итогам ты получишь оценку уровня стресса (🟢🟡🔴) и очки.\n\n"
    "*Система очков:*\n"
    "• Вечерний опрос — 15 очков\n"
    "• Экспресс-тест — 10 очков\n"
    "• Глубокий опрос — 25 очков\n"
    "• Дыхательная практика — 5 очков\n"
    "• Бонус за серию дней — 10 очков"
)

ABOUT_TEXT = (
    f"🤍 *{BOT_NAME}* v{VERSION}\n\n"
    "Помогает отслеживать уровень стресса и заботиться о себе — "
    "каждый день, в привычном темпе.\n\n"
    "📋 *Что умеет бот:*\n"
    "• Ежедневный вечерний опрос — зона стресса 🟢🟡🔴\n"
    "• Экспресс-тест — 4 быстрых вопроса в любое время\n"
    "• Глубокий опрос — 20 вопросов, полная диагностика\n"
    "• Дыхательные практики с картинками — 5 техник\n"
    "• Упражнения по снижению стресса — 5 техник\n"
    "• Личный кабинет — история, прогресс, статистика\n"
    "• Факт о стрессе каждый день в 13:30\n"
    "• Цитата психолога каждый день в 16:30\n"
    "• Утренняя карточка в выбранное время\n"
    "• Еженедельный отчёт по пятницам (по подписке)\n\n"
    "🏆 *Система очков:*\n"
    "• Вечерний опрос — 15 очков\n"
    "• Экспресс-тест — 10 очков\n"
    "• Глубокий опрос — 25 очков\n"
    "• Дыхательная практика — 5 очков\n"
    "• Бонус за серию дней — 10 очков\n\n"
    "🌙 *О луне:*\n"
    "Фазы луны добавлены для интереса — "
    "научного влияния на уровень стресса они не имеют.\n\n"
    "_Нужна помощь? Напиши /help_"
)

SURVEY_QUESTIONS_POOL = [
    # Блок 1 — Тело
    [
        "🙆 *{n} — Тело*\n\nНасколько ты чувствуешь напряжение в теле прямо сейчас?\n\n_1 — совсем нет  |  5 — очень сильное_",
        "🙆 *{n} — Тело*\n\nЕсть ли зажатость в плечах, шее или спине сегодня?\n\n_1 — совсем нет  |  5 — очень сильная_",
        "🙆 *{n} — Тело*\n\nКак бы ты оценил физическое напряжение в теле за сегодня?\n\n_1 — полностью расслаблен  |  5 — очень напряжён_",
        "🙆 *{n} — Тело*\n\nОщущал ли ты сегодня головную боль или тяжесть в теле?\n\n_1 — совсем нет  |  5 — очень сильно_",
        "🙆 *{n} — Тело*\n\nЗамечал ли ты сегодня, что сжимаешь челюсть или кулаки?\n\n_1 — совсем нет  |  5 — очень часто_",
    ],
    # Блок 2 — Цифровой шум
    [
        "📱 *{n} — Цифровой шум*\n\nНасколько тебя утомляют уведомления, новости, соцсети сегодня?\n\n_1 — совсем нет  |  5 — очень сильно_",
        "📱 *{n} — Цифровой шум*\n\nСколько раз сегодня ты бездумно тянулся к телефону?\n\n_1 — почти не тянулся  |  5 — постоянно_",
        "📱 *{n} — Цифровой шум*\n\nЧувствуешь ли перегрузку от потока информации сегодня?\n\n_1 — совсем нет  |  5 — сильная перегрузка_",
        "📱 *{n} — Цифровой шум*\n\nРаздражали ли тебя сегодня сообщения или звонки?\n\n_1 — совсем нет  |  5 — очень сильно_",
        "📱 *{n} — Цифровой шум*\n\nБыло ли желание отключить все уведомления и побыть в тишине?\n\n_1 — совсем нет  |  5 — очень сильно_",
    ],
    # Блок 3 — Мысли
    [
        "🌀 *{n} — Мысли*\n\nКак часто сегодня крутились одни и те же тревожные мысли?\n\n_1 — совсем нет  |  5 — постоянно_",
        "🌀 *{n} — Мысли*\n\nТяжело ли было «отключить голову» сегодня?\n\n_1 — легко  |  5 — очень тяжело_",
        "🌀 *{n} — Мысли*\n\nВозникало ли ощущение, что мысли «идут по кругу»?\n\n_1 — совсем нет  |  5 — почти всё время_",
        "🌀 *{n} — Мысли*\n\nБыло ли трудно остановить поток беспокойных мыслей перед сном?\n\n_1 — совсем нет  |  5 — очень трудно_",
    ],
    # Блок 4 — Концентрация
    [
        "⚡ *{n} — Концентрация*\n\nКак сложно было сегодня сосредоточиться и удерживать внимание?\n\n_1 — легко  |  5 — очень тяжело_",
        "⚡ *{n} — Концентрация*\n\nОтвлекался ли ты сегодня больше обычного?\n\n_1 — совсем нет  |  5 — очень часто_",
        "⚡ *{n} — Концентрация*\n\nУдавалось ли удерживать фокус на задачах?\n\n_1 — без проблем  |  5 — совсем не удавалось_",
        "⚡ *{n} — Концентрация*\n\nБыло ли ощущение, что голова «не варит» несмотря на усилия?\n\n_1 — совсем нет  |  5 — весь день_",
    ],
    # Блок 5 — Эмоции
    [
        "💛 *{n} — Эмоции*\n\nНасколько сильными были негативные эмоции сегодня?\n\n_1 — совсем нет  |  5 — очень интенсивные_",
        "💛 *{n} — Эмоции*\n\nЧувствовал ли ты раздражительность или злость сегодня?\n\n_1 — совсем нет  |  5 — очень сильно_",
        "💛 *{n} — Эмоции*\n\nКак бы ты описал своё эмоциональное состояние за день?\n\n_1 — спокойное, ровное  |  5 — взволнованное, напряжённое_",
        "💛 *{n} — Эмоции*\n\nБыло ли ощущение подавленности или опустошённости?\n\n_1 — совсем нет  |  5 — очень сильное_",
        "💛 *{n} — Эмоции*\n\nСрывался ли ты сегодня на других из-за мелочей?\n\n_1 — совсем нет  |  5 — несколько раз_",
    ],
    # Блок 6 — Сон
    [
        "🌙 *{n} — Сон*\n\nКак ты оцениваешь свой прошлый сон?\n\n_1 — отлично  |  5 — очень плохо_",
        "🌙 *{n} — Сон*\n\nПросыпался ли ты отдохнувшим сегодня утром?\n\n_1 — да, хорошо  |  5 — совсем нет_",
        "🌙 *{n} — Сон*\n\nХватило ли тебе сна прошлой ночью?\n\n_1 — вполне  |  5 — совсем не хватило_",
        "🌙 *{n} — Сон*\n\nБыли ли мысли или тревоги которые мешали заснуть вчера?\n\n_1 — совсем нет  |  5 — долго не мог уснуть_",
        "🌙 *{n} — Сон*\n\nПросыпался ли ты ночью без причины?\n\n_1 — нет  |  5 — несколько раз_",
    ],
    # Блок 7 — Общение
    [
        "💬 *{n} — Общение*\n\nНасколько тебе сегодня хотелось избегать людей?\n\n_1 — совсем нет  |  5 — очень сильно_",
        "💬 *{n} — Общение*\n\nРаздражало ли тебя сегодня общение с окружающими?\n\n_1 — совсем нет  |  5 — очень сильно_",
        "💬 *{n} — Общение*\n\nЧувствовал ли ты желание побыть в одиночестве больше обычного?\n\n_1 — нет  |  5 — очень сильно_",
        "💬 *{n} — Общение*\n\nБыло ли тяжело поддерживать разговор или отвечать на сообщения?\n\n_1 — совсем нет  |  5 — очень тяжело_",
        "💬 *{n} — Общение*\n\nОщущал ли ты себя непонятым или одиноким сегодня?\n\n_1 — совсем нет  |  5 — очень сильно_",
    ],
    # Блок 8 — Аппетит (НОВЫЙ)
    [
        "🍽 *{n} — Аппетит*\n\nКак изменился твой аппетит сегодня по сравнению с обычным?\n\n_1 — всё как обычно  |  5 — сильно изменился_",
        "🍽 *{n} — Аппетит*\n\nЕл ли ты сегодня больше обычного из-за стресса или скуки?\n\n_1 — нет  |  5 — намного больше_",
        "🍽 *{n} — Аппетит*\n\nПропускал ли ты еду сегодня потому что не было аппетита?\n\n_1 — нет  |  5 — почти ничего не ел_",
        "🍽 *{n} — Аппетит*\n\nТянуло ли тебя сегодня к сладкому или вредной еде?\n\n_1 — совсем нет  |  5 — очень сильно_",
    ],
    # Блок 9 — Тревога без причины (НОВЫЙ)
    [
        "😰 *{n} — Тревога*\n\nВозникала ли у тебя тревога без конкретной причины сегодня?\n\n_1 — совсем нет  |  5 — очень часто_",
        "😰 *{n} — Тревога*\n\nБыло ли ощущение, что вот-вот что-то случится — хотя всё нормально?\n\n_1 — совсем нет  |  5 — почти весь день_",
        "😰 *{n} — Тревога*\n\nНасколько сильным было беспокойство о вещах которые не можешь контролировать?\n\n_1 — совсем нет  |  5 — очень сильное_",
        "😰 *{n} — Тревога*\n\nЧувствовал ли ты внутреннее беспокойство или нервозность без явного повода?\n\n_1 — совсем нет  |  5 — почти постоянно_",
    ],
    # Блок 10 — Смысл и мотивация (НОВЫЙ)
    [
        "🎯 *{n} — Смысл*\n\nЧувствовал ли ты смысл в том, что делал сегодня?\n\n_1 — да, всё было осмысленно  |  5 — совсем не чувствовал_",
        "🎯 *{n} — Смысл*\n\nБыло ли желание браться за дела или всё казалось бессмысленным?\n\n_1 — желание было  |  5 — ничего не хотелось_",
        "🎯 *{n} — Смысл*\n\nНасколько трудно было найти в себе мотивацию сегодня?\n\n_1 — легко  |  5 — очень трудно_",
        "🎯 *{n} — Смысл*\n\nОщущал ли ты усталость от жизни или нежелание что-либо делать?\n\n_1 — совсем нет  |  5 — очень сильно_",
    ],
    # Блок 11 — Откладывание дел (НОВЫЙ)
    [
        "📋 *{n} — Дела*\n\nОткладывал ли ты важные дела, хотя знал что нужно сделать?\n\n_1 — нет, всё сделал  |  5 — откладывал всё_",
        "📋 *{n} — Дела*\n\nНасколько трудно было начать делать то, что запланировал?\n\n_1 — легко  |  5 — не смог начать_",
        "📋 *{n} — Дела*\n\nБыло ли ощущение, что задачи накапливаются, а сил на них нет?\n\n_1 — совсем нет  |  5 — очень сильное_",
        "📋 *{n} — Дела*\n\nСколько запланированных дел ты не сделал сегодня без весомой причины?\n\n_1 — всё выполнил  |  5 — почти ничего_",
    ],
]


def get_random_survey_questions():
    """Случайно выбирает 8 блоков из 11, из каждого — 1 случайный вопрос.
    Нумерация подставляется динамически."""
    blocks = random.sample(SURVEY_QUESTIONS_POOL, 8)
    result = []
    for i, block in enumerate(blocks):
        q = random.choice(block)
        result.append(q.replace("{n}", f"Вопрос {i + 1} из 8"))
    return result


def get_random_express_questions():
    """Случайно выбирает 4 блока из общего пула, из каждого — 1 вопрос."""
    blocks = random.sample(SURVEY_QUESTIONS_POOL, 4)
    result = []
    for i, block in enumerate(blocks):
        q = random.choice(block)
        result.append(q.replace("{n}", f"Вопрос {i + 1} из 4"))
    return result


RESULT_GREEN = "🟢 *Зелёная зона — Баланс!*\n\nКрасавчик! Ты в хорошем состоянии сегодня 🙌\nСохраняй этот ритм — ты справляешься.\n\n_{score} баллов из 40_"
RESULT_YELLOW = "🟡 *Жёлтая зона — Умеренный стресс*\n\nЧувствуется нагрузка, но ты держишься 💪\nПопробуй технику дыхания — 5 минут могут изменить вечер.\n\n_{score} баллов из 40_"
RESULT_RED = "🔴 *Красная зона — Высокий стресс*\n\nЗвучит тяжело. Это нормально — бывает.\nПопробуй прямо сейчас: 🧘 *Дыхательная гимнастика*.\nЕсли так несколько дней — поговори с кем-то, кому доверяешь.\n\n_{score} баллов из 40_"

POINTS_ADDED = "\n\n✨ *+{pts} очков* начислено!"
STREAK_BONUS = "🔥 Серия {streak} дней подряд! Бонус *+10 очков*"

EXPRESS_START = "⚡ *Экспресс-тест* — 4 быстрых вопроса\n\nЗаймёт меньше минуты. Поехали 👇"
EXPRESS_RESULT = "⚡ *Результат экспресс-теста*\n\nСумма: *{score} из 20*\nСредний балл: *{avg}*\n\n_{hint}_\n\n✨ *+10 очков* начислено!"
EXPRESS_COOLDOWN = "⏳ Экспресс-тест можно проходить *раз в час*.\nСледующий доступен через *{minutes} мин.*"

BREATHING_MENU = "🧘 *Дыхательная гимнастика*\n\nВыбери упражнение — и дай себе пару минут тишины 🌿"
BREATHING_COOLDOWN = "🧘 Описание доступно, но очки получишь через *{minutes} мин.*\n\n{text}"
BREATHING_POINTS = "\n\n✨ *+{pts} очков* за практику!"

BREATHING_DATA = {
    "breath_square": {
        "image": "breath_square",
        "text": (
            "🔲 *Квадратное дыхание*\n\n"
            "Простая и мощная техника для быстрого снятия напряжения.\n\n"
            "1️⃣ Вдох — *4 секунды*\n"
            "2️⃣ Задержи дыхание — *4 секунды*\n"
            "3️⃣ Выдох — *4 секунды*\n"
            "4️⃣ Задержи дыхание — *4 секунды*\n\n"
            "Повтори 4–6 раз. Концентрируйся только на счёте 🎯"
        ),
    },
    "breath_478": {
        "image": "breath_478",
        "text": (
            "4️⃣ *Дыхание 4-7-8*\n\n"
            "Метод доктора Эндрю Вейла — расслабляет нервную систему за минуты.\n\n"
            "1️⃣ Вдох через нос — *4 секунды*\n"
            "2️⃣ Задержи дыхание — *7 секунд*\n"
            "3️⃣ Выдох через рот со звуком — *8 секунд*\n\n"
            "Повтори 3–4 раза. Можно делать лёжа 🛏"
        ),
    },
    "breath_diaphragm": {
        "image": "breath_diaphragm",
        "text": (
            "🫁 *Диафрагмальное дыхание*\n\n"
            "Самый естественный способ дышать — как в детстве.\n\n"
            "1️⃣ Положи руку на живот\n"
            "2️⃣ Вдохни носом — *живот поднимается*, грудь почти не двигается\n"
            "3️⃣ Выдыхай медленно через рот — *живот опускается*\n\n"
            "5 минут такого дыхания снижают кортизол 📉"
        ),
    },
    "breath_relax": {
        "image": "breath_relax",
        "text": (
            "😌 *Расслабляющее дыхание*\n\n"
            "Выдох длиннее вдоха — это сигнал телу «всё хорошо».\n\n"
            "1️⃣ Вдох — *4 секунды*\n"
            "2️⃣ Выдох — *6–8 секунд*\n\n"
            "Повтори 8–10 раз. Хорошо работает перед сном 🌙"
        ),
    },
    "breath_nostril": {
        "image": "breath_nostril",
        "text": (
            "👃 *Дыхание через ноздри (Нади Шодхана)*\n\n"
            "Балансирует левое и правое полушария мозга.\n\n"
            "1️⃣ Закрой правую ноздрю большим пальцем\n"
            "2️⃣ Вдохни через левую — *4 секунды*\n"
            "3️⃣ Закрой обе, задержи — *4 секунды*\n"
            "4️⃣ Открой правую, выдохни — *4 секунды*\n"
            "5️⃣ Вдохни через правую — *4 секунды*, затем смени ноздрю\n\n"
            "5 циклов = полная перезагрузка 🔄"
        ),
    },
}
BREATHING_TEXTS = {k: v["text"] for k, v in BREATHING_DATA.items()}

PRACTICES_DATA = {
    "practice_grounding": {
        "image": "practice_grounding",
        "text": (
            "🌍 *Техника заземления 5-4-3-2-1*\n\n"
            "Возвращает в «здесь и сейчас» за 2–3 минуты.\n\n"
            "Назови вслух или мысленно:\n"
            "👁 *5 вещей*, которые ты видишь\n"
            "✋ *4 вещи*, которые можешь потрогать\n"
            "👂 *3 звука*, которые слышишь\n"
            "👃 *2 запаха*, которые ощущаешь\n"
            "👅 *1 вкус*, который чувствуешь\n\n"
            "Фокусируйся на каждом ощущении — спешить не нужно 🌿"
        ),
    },
    "practice_cold_water": {
        "image": "practice_cold_water",
        "text": (
            "🧊 *Метод холодной воды*\n\n"
            "Аварийный «тормоз» тревоги — работает за 30–60 секунд.\n\n"
            "1️⃣ Подойди к раковине\n"
            "2️⃣ Смочи запястья холодной водой\n"
            "3️⃣ Или умойся — уделяя внимание ощущению холода\n\n"
            "Холод активирует рефлекс ныряния — сердечный ритм замедляется, "
            "тревога снижается. Это физиология, а не магия 🧬"
        ),
    },
    "practice_muscle_relax": {
        "image": "practice_muscle_relax",
        "text": (
            "💪 *Прогрессивная мышечная релаксация*\n\n"
            "Снимает накопившееся телесное напряжение за 10 минут.\n\n"
            "Для каждой группы мышц:\n"
            "1️⃣ Напряги — *7 секунд* (сильно, но без боли)\n"
            "2️⃣ Резко расслабь — *20 секунд*, чувствуй разницу\n\n"
            "Порядок: ступни → голени → бёдра → живот → руки → плечи → лицо\n\n"
            "_Метод Джекобсона — один из наиболее изученных способов снижения тревожности_ 🔬"
        ),
    },
    "practice_mind_dump": {
        "image": "practice_mind_dump",
        "text": (
            "📝 *Выгрузка мыслей*\n\n"
            "Освобождает «оперативную память» мозга.\n\n"
            "1️⃣ Возьми лист бумаги или открой заметки\n"
            "2️⃣ Поставь таймер на *3 минуты*\n"
            "3️⃣ Пиши всё, что тебя беспокоит — без цензуры, потоком\n"
            "4️⃣ Когда время выйдет — можно смять и выбросить\n\n"
            "Не нужно анализировать написанное. Сам акт выгрузки снижает тревогу ✍️"
        ),
    },
    "practice_mindful_walk": {
        "image": "practice_mindful_walk",
        "text": (
            "🌅 *Осознанная прогулка*\n\n"
            "Сочетание движения и присутствия снижает кортизол лучше, чем просто ходьба.\n\n"
            "1️⃣ Выйди на улицу — *5–10 минут* достаточно\n"
            "2️⃣ Убери телефон\n"
            "3️⃣ Фокусируйся только на ощущениях:\n"
            "   • Как чувствуется каждый шаг?\n"
            "   • Какой воздух — тёплый, холодный?\n"
            "   • Что видишь, что слышишь?\n\n"
            "_Если выйти нельзя — подойдёт медленная ходьба по комнате_ 🚶"
        ),
    },
}
PRACTICES_TEXTS = {k: v["text"] for k, v in PRACTICES_DATA.items()}

PRACTICES_MENU_TEXT = "🧠 *Практики снижения стресса*\n\nВыбери технику — каждая займёт не больше 10 минут 👇"

STATS_TEMPLATE = (
    "📊 *Твоя статистика*\n\n"
    "🏆 Очков: *{points}*\n"
    "🔥 Серия: *{streak} дн.*\n\n"
    "Последние опросы:\n{survey_list}\n\n"
    "Последние экспресс-тесты:\n{express_list}"
)
STATS_EMPTY = "пока нет данных"
ZONE_ICONS = {"green": "🟢", "yellow": "🟡", "red": "🔴"}

TASKS_TEMPLATE = (
    "📋 *Задания на сегодня*\n\n"
    "{survey_status} Вечерний опрос         (+15 очков)\n"
    "{breath_status} Дыхательная гимнастика (+5 очков)\n"
    "{express_status} Экспресс-тест          (+10 очков)\n\n"
    "Всего очков: *{points}*"
)
DONE_ICON = "✅"
TODO_ICON = "⬜"

MOON_PHASES = {
    "new": ("Новолуние", "5406872996801452883"),
    "waxing_crescent": ("Растущий серп", "5409106491464519356"),
    "waning_crescent": ("Убывающий серп", "5406978704536543831"),
    "first_quarter": ("Первая четверть", "5407127288930146376"),
    "waning_gibbous": ("Убывающая луна", "5406810608106509175"),
    "full": ("Полнолуние", "5406786659368869656"),
    "waxing_gibbous": ("Растущая луна", "5406989918696154956"),
    "last_quarter": ("Последняя четверть", "5406662354425386431"),
}


def moon_prem_tag(emoji_id: str) -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">🤩</tg-emoji>'


MOON_DISCLAIMER = "\n\nНаучных доказательств влияния фазы луны на уровень стресса не существует. Это просто красиво 🌌"

EVENING_PUSH = "🌆 *Время подвести итоги дня!*\n\nПройди короткий опрос — займёт меньше 2 минут. Нажми на кнопку ниже 👇"
SURVEY_START_BTN = "📝 Начать опрос"
FACT_PREFIX = "🧠 Факт о стрессе\n\n"
QUOTE_PREFIX = "💬 Цитата дня\n\n"

ADMIN_TRIGGER = "⚠️ Триггер!\nПользователь {uid} ({name}) поставил максимум 5 баллов в {q} вопросах.\nДата: {dt}"
ADMIN_RED = "🚨 Длительный стресс!\nПользователь {uid} ({name}) — {days} дня подряд в красной зоне."

ADMIN_HELP_TEXT = (
    f"🛠 Команды администратора — {BOT_NAME} v{VERSION}\n\n"
    "Статистика:\n"
    "/admin_stats — общая статистика бота\n"
    "/admin_users — список всех пользователей\n"
    "/admin_blocked — список тех, кто заблокировал бота\n"
    "/export_stats — экспорт настроений за 30 дней (CSV)\n\n"
    "Управление очками:\n"
    "/add_points user_id очки — начислить очки\n"
    "/set_points user_id очки — установить очки\n"
    "/set_streak user_id дни — установить серию дней\n\n"
    "Рассылки:\n"
    "/broadcast текст — рассылка всем пользователям\n"
    "/update_notify версия текст — оповещение с премиум-эмодзи\n"
    "/rat версия текст — то же самое (короткий алиас)\n"
    "  Пример: /rat 051 Новые вопросы и исправления\n"
    "  Версия без точек: 051 = v0.5.1\n\n"
    "Диагностика:\n"
    "/ping — проверка работы бота и ID\n"
    "/weekly_preview — предпросмотр пятничного отчёта\n\n"
    "Все команды доступны только администратору."
)

GROUP_STATS_TEXT = "📊 *Статистика*\n\nЗдесь ты можешь посмотреть результаты, задания на сегодня и быстро проверить состояние."
GROUP_RELAX_TEXT = "🌿 *Практики и релакс*\n\nДыхательные упражнения, научно обоснованные практики и немного астрономии 🌙"
GROUP_INFO_TEXT = "ℹ️ *Меню «О боте»*\n\nЗдесь ты найдёшь информацию о боте и настройку времени рассылок."

MORNING_CAPTIONS = [
    "🌅 Доброе утро! Сегодня — новый шанс быть в балансе 🌿",
    "☀️ Привет! Одна маленькая дыхательная практика — и день начнётся отлично 🧘",
    "🌸 Новый день — новые возможности. Ты справишься! 💪",
    "🌤 Сделай что-то маленькое для себя сегодня. Начни с дыхания ✨",
    "🌻 Доброе утро! Стресс временен, ты постоянен 🤍",
    "🌄 Ещё один день — ещё один шанс почувствовать себя лучше. Начнём? 🌿",
    "☕ Утро — лучшее время напомнить себе: ты справляешься. Даже если кажется иначе 🤍",
    "🌞 Три глубоких вдоха — и день уже чуть лучше. Попробуй прямо сейчас 🧘",
    "🌈 Доброе утро! Сегодня не нужно быть идеальным — достаточно просто быть 💛",
    "🍃 Новое утро — новый баланс. Один маленький шаг сегодня важнее большого плана на завтра ✨",
]

# ================================================================
#  КНОПКИ
# ================================================================

BTN_STATS_GROUP = "📊 Статистика"
BTN_RELAX_GROUP = "🌿 Практики"
BTN_INFO_GROUP = "ℹ️ О боте"

BTN_MY_STATS = "📊 Моя статистика"
BTN_TASKS = "📋 Мои задания"
BTN_EXPRESS = "⚡ Экспресс-тест"
BTN_CABINET = "👤 Личный кабинет"
BTN_DIAGNOSTIC = "🔬 Глубокий опрос"

BTN_BREATHING = "🧘 Дыхательная гимнастика"
BTN_PRACTICES = "🧠 Практики"
BTN_MOON = "🌙 Фаза луны"

BTN_ABOUT = "📖 О боте"
BTN_TIME = "⏰ Настроить время"

BTN_BACK = "← Главное меню"


# ================================================================
#  КЛАВИАТУРЫ
# ================================================================

def main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_STATS_GROUP)],
            [KeyboardButton(text=BTN_RELAX_GROUP)],
            [KeyboardButton(text=BTN_INFO_GROUP)],
        ],
        resize_keyboard=True, is_persistent=True,
    )


def stats_submenu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_TASKS), KeyboardButton(text=BTN_CABINET)],
            [KeyboardButton(text=BTN_EXPRESS), KeyboardButton(text=BTN_DIAGNOSTIC)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True, is_persistent=True,
    )


def relax_submenu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_BREATHING), KeyboardButton(text=BTN_PRACTICES)],
            [KeyboardButton(text=BTN_MOON)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True, is_persistent=True,
    )


def info_submenu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_ABOUT)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True, is_persistent=True,
    )


def gender_kb():
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="👦 Мужской", callback_data="gender:male"),
        InlineKeyboardButton(text="👧 Женский", callback_data="gender:female"),
        InlineKeyboardButton(text="🤷 Другое", callback_data="gender:other"),
    )
    return b.as_markup()


def trial_kb():
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="✅ Да, пройти сейчас", callback_data="trial:yes"),
        InlineKeyboardButton(text="⏭ Позже", callback_data="trial:no"),
    )
    return b.as_markup()


def likert_kb(prefix):
    b = InlineKeyboardBuilder()
    for i in range(1, 6):
        b.button(text=str(i), callback_data=f"{prefix}:{i}")
    b.adjust(5)
    return b.as_markup()


def breathing_kb():
    b = InlineKeyboardBuilder()
    for cb, label in [
        ("breath_square", "🔲 Квадратное дыхание"),
        ("breath_478", "4️⃣ Дыхание 4-7-8"),
        ("breath_diaphragm", "🫁 Диафрагмальное"),
        ("breath_relax", "😌 Расслабляющее"),
        ("breath_nostril", "👃 Через ноздри"),
    ]:
        b.button(text=label, callback_data=cb)
    b.adjust(1)
    return b.as_markup()


def practices_kb():
    b = InlineKeyboardBuilder()
    for cb, label in [
        ("practice_grounding", "🌍 Заземление 5-4-3-2-1"),
        ("practice_cold_water", "🧊 Холодная вода"),
        ("practice_muscle_relax", "💪 Мышечная релаксация"),
        ("practice_mind_dump", "📝 Выгрузка мыслей"),
        ("practice_mindful_walk", "🌅 Осознанная прогулка"),
    ]:
        b.button(text=label, callback_data=cb)
    b.adjust(1)
    return b.as_markup()


def time_settings_kb():
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="🌆 Время опроса", callback_data="time:survey"),
        InlineKeyboardButton(text="🌅 Утреннее время", callback_data="time:morning"),
    )
    b.row(
        InlineKeyboardButton(text="❌ Отключить утреннюю", callback_data="time:morning_off"),
    )
    return b.as_markup()


def survey_start_kb():
    b = InlineKeyboardBuilder()
    b.button(text=SURVEY_START_BTN, callback_data="survey:start_main")
    return b.as_markup()


# ================================================================
#  FSM
# ================================================================

class RegSt(StatesGroup):
    agreement = State()  # согласие с документами
    gender = State()
    trial = State()
    survey_time = State()
    morning_time = State()


class SurveySt(StatesGroup):
    main_q = State()
    express_q = State()


class TimeSt(StatesGroup):
    survey = State()
    morning = State()


class BroadcastSt(StatesGroup):
    waiting_text = State()


class DiagnosticSt(StatesGroup):
    question = State()


# ================================================================
#  УТИЛИТЫ
# ================================================================

TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})$")


def norm_time(raw):
    m = TIME_RE.match((raw or "").strip())
    if not m: return None
    h, mn = int(m.group(1)), int(m.group(2))
    return f"{h:02d}:{mn:02d}" if (0 <= h <= 23 and 0 <= mn <= 59) else None


def det_zone(score):
    if score <= ZONE_GREEN[1]:  return "green"
    if score <= ZONE_RED[0] - 1:  return "yellow"
    return "red"


def res_text(score, zone):
    return {"green": RESULT_GREEN, "yellow": RESULT_YELLOW, "red": RESULT_RED}[zone].format(score=score)


def expr_hint(score):
    if score <= 8:  return "Отличный баланс! Продолжай в том же духе 🌿"
    if score <= 14: return "Небольшое напряжение — попробуй дыхательную гимнастику 🧘"
    return "Высокая нагрузка. Сделай паузу и подыши 🫁"


def load_facts():
    if not os.path.exists(FACTS_FILE):
        return ["Стресс — нормальная реакция организма. Главное — научиться с ним работать."]
    with open(FACTS_FILE, encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]
    return lines or ["Краткий стресс (эустресс) может повышать продуктивность!"]


def load_quotes():
    if not os.path.exists(QUOTES_FILE):
        return ["Дышите. Вы уже справляетесь лучше, чем думаете."]
    with open(QUOTES_FILE, encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]
    return lines or ["Один маленький шаг сегодня лучше, чем идеальный план на завтра."]


def moon_phase_key():
    import ephem
    moon = ephem.Moon()
    moon.compute(date.today().strftime("%Y/%m/%d"))
    illum = moon.phase

    moon_tomorrow = ephem.Moon()
    tomorrow = (date.today() + timedelta(days=1)).strftime("%Y/%m/%d")
    moon_tomorrow.compute(tomorrow)
    waxing = moon_tomorrow.phase > moon.phase

    if illum < 2:
        return "new"
    elif illum < 45:
        return "waxing_crescent" if waxing else "waning_crescent"
    elif illum < 55:
        return "first_quarter" if waxing else "last_quarter"
    elif illum < 98:
        return "waxing_gibbous" if waxing else "waning_gibbous"
    else:
        return "full"


MOON_FILE_MAP = {
    "new": "new_moon",
    "waxing_crescent": "waxing_crescent",
    "first_quarter": "first_quarter",
    "waxing_gibbous": "waxing_gibbous",
    "full": "full_moon",
    "waning_gibbous": "waning_gibbous",
    "last_quarter": "third_quarter",
    "waning_crescent": "waning_crescent",
}


def moon_photo(phase_key):
    filename = MOON_FILE_MAP.get(phase_key, phase_key)
    for ext in ("jpg", "jpeg", "png", "webp"):
        p = os.path.join(MOON_DIR, f"{filename}.{ext}")
        if os.path.exists(p): return p
    return None


_last_morning_img = None


def rand_morning_img():
    global _last_morning_img
    if not os.path.isdir(MORNING_DIR):
        return None
    files = [
        os.path.join(MORNING_DIR, f)
        for f in os.listdir(MORNING_DIR)
        if f.lower().endswith((".jpg", ".jpeg", ".png", ".webp"))
    ]
    if not files:
        return None
    if len(files) == 1:
        return files[0]
    candidates = [f for f in files if f != _last_morning_img]
    chosen = random.choice(candidates)
    _last_morning_img = chosen
    return chosen


def fmt_moods(rows):
    if not rows: return STATS_EMPTY
    lines = []
    for r in rows:
        icon = ZONE_ICONS.get(r["zone"], "⚪")
        dt = r["created_at"][:10]
        lines.append(f"{icon} {r['score']} баллов — {dt}")
    return "\n".join(lines)


# ================================================================
#  РОУТЕР
# ================================================================

router = Router()
MD = "Markdown"


# ── Отслеживание блокировок ───────────────────────────────────

@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=KICKED))
async def bot_blocked_handler(event: ChatMemberUpdated):
    """Срабатывает, когда пользователь блокирует бота"""
    set_user_blocked(event.from_user.id, 1)
    logger.info(f"User {event.from_user.id} blocked the bot.")


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=MEMBER))
async def bot_unblocked_handler(event: ChatMemberUpdated):
    """Срабатывает, когда пользователь разблокирует бота (перезапускает)"""
    set_user_blocked(event.from_user.id, 0)
    logger.info(f"User {event.from_user.id} unblocked the bot.")


# ── Навигация ─────────────────────────────────────────────────

@router.message(F.text == BTN_STATS_GROUP)
async def open_stats(msg: Message):
    await msg.answer(GROUP_STATS_TEXT, parse_mode=MD, reply_markup=stats_submenu())


@router.message(F.text == BTN_RELAX_GROUP)
async def open_relax(msg: Message):
    await msg.answer(GROUP_RELAX_TEXT, parse_mode=MD, reply_markup=relax_submenu())


@router.message(F.text == BTN_INFO_GROUP)
async def open_info(msg: Message):
    await msg.answer(GROUP_INFO_TEXT, parse_mode=MD, reply_markup=info_submenu())


@router.message(F.text == BTN_BACK)
async def go_back(msg: Message):
    await msg.answer("🏠 Главное меню", reply_markup=main_menu())


@router.message(Command("menu_hide"))
async def cmd_menu_hide(msg: Message):
    await msg.answer(
        "Кнопки меню скрыты. Напиши /menu_show чтобы вернуть их.",
        reply_markup=ReplyKeyboardRemove()
    )


@router.message(Command("menu_show"))
async def cmd_menu_show(msg: Message):
    await msg.answer("Кнопки меню возвращены 👇", reply_markup=main_menu())


# ── /start ────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    upsert_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)
    set_user_blocked(msg.from_user.id, 0)
    u = get_user(msg.from_user.id)
    if u and u["gender"]:
        await msg.answer(
            f"С возвращением, {msg.from_user.first_name}! 👋\nВсё готово — пользуйся меню 👇",
            reply_markup=main_menu()
        )
        await state.clear()
        return
    # Новый пользователь — показываем соглашение
    await msg.answer(WELCOME, parse_mode=MD)
    await msg.answer(ASK_AGREEMENT, parse_mode=MD, reply_markup=agreement_kb())
    await state.set_state(RegSt.agreement)


@router.callback_query(RegSt.agreement, F.data == "agreement:accept")
async def cb_agreement(call: CallbackQuery, state: FSMContext):
    await call.message.edit_reply_markup()
    await call.message.answer(ASK_GENDER, parse_mode=MD, reply_markup=gender_kb())
    await state.set_state(RegSt.gender)
    await call.answer()


@router.message(Command("help"))
async def cmd_help(msg: Message):
    await msg.answer(HELP_TEXT, parse_mode=MD)


@router.callback_query(RegSt.gender, F.data.startswith("gender:"))
async def cb_gender(call: CallbackQuery, state: FSMContext):
    set_gender(call.from_user.id, call.data.split(":")[1])
    await call.message.edit_reply_markup()
    await call.message.answer(GENDER_SAVED, parse_mode=MD, reply_markup=trial_kb())
    await state.set_state(RegSt.trial)
    await call.answer()


@router.callback_query(RegSt.trial, F.data.startswith("trial:"))
async def cb_trial(call: CallbackQuery, state: FSMContext):
    await call.message.edit_reply_markup()
    await state.update_data(run_trial=(call.data.split(":")[1] == "yes"))
    await call.message.answer(ASK_SURVEY_TIME, parse_mode=MD)
    await state.set_state(RegSt.survey_time)
    await call.answer()


@router.message(RegSt.survey_time)
async def reg_survey_time(msg: Message, state: FSMContext):
    t = norm_time(msg.text)
    if not t:
        await msg.answer(TIME_INVALID, parse_mode=MD);
        return
    set_survey_time(msg.from_user.id, t)
    await msg.answer(TIME_SAVED.format(time=t), parse_mode=MD)
    await msg.answer(ASK_MORNING_TIME, parse_mode=MD)
    await state.set_state(RegSt.morning_time)


@router.message(RegSt.morning_time)
async def reg_morning_time(msg: Message, state: FSMContext):
    text = (msg.text or "").strip()
    data = await state.get_data()
    if text.lower() == "/skip":
        set_morning_time(msg.from_user.id, None)
    else:
        t = norm_time(text)
        if not t:
            await msg.answer(TIME_INVALID, parse_mode=MD);
            return
        set_morning_time(msg.from_user.id, t)
        await msg.answer(TIME_SAVED.format(time=t), parse_mode=MD)
    await msg.answer(SETUP_DONE, parse_mode=MD, reply_markup=main_menu())
    await state.clear()
    if data.get("run_trial"):
        await _start_survey(msg, state)


# ── Настройка времени ─────────────────────────────────────────

@router.message(F.text == BTN_TIME)
async def menu_time(msg: Message):
    u = get_user(msg.from_user.id)
    if not u: return
    await msg.answer(
        TIME_SETTINGS.format(
            survey_time=u["survey_time"],
            morning_time=u["morning_time"] or "отключена"
        ),
        parse_mode=MD, reply_markup=time_settings_kb()
    )


@router.callback_query(F.data == "time:survey")
async def cbt_survey(call: CallbackQuery, state: FSMContext):
    await call.message.answer("✏️ Введи новое время вечернего опроса (формат *ЧЧ:ММ*):", parse_mode=MD)
    await state.set_state(TimeSt.survey);
    await call.answer()


@router.callback_query(F.data == "time:morning")
async def cbt_morning(call: CallbackQuery, state: FSMContext):
    await call.message.answer("✏️ Введи время утренней рассылки (формат *ЧЧ:ММ*):", parse_mode=MD)
    await state.set_state(TimeSt.morning);
    await call.answer()


@router.callback_query(F.data == "time:morning_off")
async def cbt_morning_off(call: CallbackQuery):
    set_morning_time(call.from_user.id, None)
    await call.message.answer(TIME_MORNING_OFF);
    await call.answer()


@router.message(TimeSt.survey)
async def edit_survey(msg: Message, state: FSMContext):
    t = norm_time(msg.text)
    if not t:
        await msg.answer(TIME_INVALID, parse_mode=MD);
        return
    set_survey_time(msg.from_user.id, t)
    await msg.answer(TIME_SAVED.format(time=t), parse_mode=MD)
    await state.clear()


@router.message(TimeSt.morning)
async def edit_morning(msg: Message, state: FSMContext):
    t = norm_time(msg.text)
    if not t:
        await msg.answer(TIME_INVALID, parse_mode=MD);
        return
    set_morning_time(msg.from_user.id, t)
    await msg.answer(TIME_SAVED.format(time=t), parse_mode=MD)
    await state.clear()


# ── Основной опрос ────────────────────────────────────────────

async def _start_survey(msg: Message, state: FSMContext):
    questions = get_random_survey_questions()
    await state.set_state(SurveySt.main_q)
    await state.update_data(answers=[], questions=questions)
    await msg.answer(questions[0], parse_mode=MD, reply_markup=likert_kb("mq"))


@router.callback_query(F.data == "survey:start_main")
async def cb_survey_start(call: CallbackQuery, state: FSMContext):
    await call.message.edit_reply_markup()
    await _start_survey(call.message, state)
    await call.answer()


@router.callback_query(SurveySt.main_q, F.data.startswith("mq:"))
async def cb_main_q(call: CallbackQuery, state: FSMContext, bot: Bot):
    value = int(call.data.split(":")[1])
    data = await state.get_data()
    answers = data.get("answers", [])
    questions = data.get("questions", get_random_survey_questions())
    answers.append(value)
    await call.message.edit_reply_markup()

    if len(answers) < len(questions):
        await state.update_data(answers=answers)
        await call.message.answer(
            questions[len(answers)], parse_mode=MD, reply_markup=likert_kb("mq")
        )
        await call.answer();
        return

    uid = call.from_user.id
    score = sum(answers)
    zone = det_zone(score)
    save_mood(uid, score, zone, "main", answers)
    streak = update_streak(uid)
    add_points(uid, POINTS_SURVEY)
    log_task(uid, "survey", POINTS_SURVEY)

    text = res_text(score, zone) + POINTS_ADDED.format(pts=POINTS_SURVEY)
    if streak > 1:
        add_points(uid, POINTS_STREAK_BONUS)
        text += "\n" + STREAK_BONUS.format(streak=streak)

    await call.message.answer(text, parse_mode=MD)
    await state.clear()
    await call.answer()

    name = call.from_user.first_name or str(uid)
    dt = datetime.now().strftime("%d.%m.%Y %H:%M")

    if TRIGGER_VALUE in answers:
        bad_count = answers.count(TRIGGER_VALUE)
        try:
            await bot.send_message(
                ADMIN_IDS[0],  # отправляем первому админу
                ADMIN_TRIGGER.format(uid=uid, name=name, q=bad_count, dt=dt)
            )
        except Exception:
            pass

    rs = get_red_streak(uid)
    if rs >= RED_STREAK_ALERT:
        try:
            await bot.send_message(
                ADMIN_IDS[0],
                ADMIN_RED.format(uid=uid, name=name, days=rs)
            )
        except Exception:
            pass


# ── Экспресс-тест ─────────────────────────────────────────────

@router.message(F.text == BTN_EXPRESS)
async def menu_express(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    ldt = get_last_mood_dt(uid, "express")
    if ldt:
        diff = (datetime.now(pytz.utc) - ldt.replace(tzinfo=pytz.utc)).total_seconds()
        if diff < COOLDOWN_EXPRESS:
            mins = int((COOLDOWN_EXPRESS - diff) // 60) + 1
            await msg.answer(EXPRESS_COOLDOWN.format(minutes=mins), parse_mode=MD);
            return
    questions = get_random_express_questions()
    await state.set_state(SurveySt.express_q)
    await state.update_data(answers=[], questions=questions)
    await msg.answer(EXPRESS_START, parse_mode=MD)
    await msg.answer(questions[0], parse_mode=MD, reply_markup=likert_kb("eq"))


@router.callback_query(SurveySt.express_q, F.data.startswith("eq:"))
async def cb_express_q(call: CallbackQuery, state: FSMContext):
    value = int(call.data.split(":")[1])
    data = await state.get_data()
    answers = data.get("answers", [])
    questions = data.get("questions", get_random_express_questions())
    answers.append(value)
    await call.message.edit_reply_markup()

    if len(answers) < len(questions):
        await state.update_data(answers=answers)
        await call.message.answer(
            questions[len(answers)], parse_mode=MD, reply_markup=likert_kb("eq")
        )
        await call.answer();
        return

    uid = call.from_user.id
    score = sum(answers)
    avg = round(score / len(answers), 1)
    save_mood(uid, score, "yellow", "express", answers)
    add_points(uid, POINTS_EXPRESS)
    log_task(uid, "express", POINTS_EXPRESS)
    result_text = EXPRESS_RESULT.format(score=score, avg=avg, hint=expr_hint(score))
    await call.message.answer(result_text, parse_mode=MD)
    await state.clear();
    await call.answer()


# ── Дыхание ───────────────────────────────────────────────────

@router.message(F.text == BTN_BREATHING)
async def menu_breathing(msg: Message):
    await msg.answer(BREATHING_MENU, parse_mode=MD, reply_markup=breathing_kb())


@router.callback_query(F.data.startswith("breath_"))
async def cb_breath(call: CallbackQuery):
    data = BREATHING_DATA.get(call.data)
    if not data:
        await call.answer("Неизвестное упражнение");
        return
    text = data["text"]
    uid = call.from_user.id
    ldt = get_last_mood_dt(uid, "breathing")
    can = True
    if ldt:
        diff = (datetime.now(pytz.utc) - ldt.replace(tzinfo=pytz.utc)).total_seconds()
        can = diff >= COOLDOWN_BREATHING
    if can:
        pts = POINTS_BREATH_FIRST if not task_done_today(uid, "breathing") else POINTS_BREATH_NEXT
        add_points(uid, pts);
        log_task(uid, "breathing", pts)
        save_mood(uid, 0, "green", "breathing", [])
        caption = text + BREATHING_POINTS.format(pts=pts)
    else:
        diff = (datetime.now(pytz.utc) - ldt.replace(tzinfo=pytz.utc)).total_seconds()
        mins = int((COOLDOWN_BREATHING - diff) // 60) + 1
        caption = BREATHING_COOLDOWN.format(minutes=mins, text=text)

    img_name = data["image"]
    img_path = None
    for ext in ("jpg", "jpeg", "png", "webp"):
        p = os.path.join(BREATHING_DIR, f"{img_name}.{ext}")
        if os.path.exists(p):
            img_path = p
            break

    try:
        if img_path:
            await call.message.answer_photo(
                FSInputFile(img_path), caption=caption, parse_mode="Markdown"
            )
        else:
            await call.message.answer(caption, parse_mode="Markdown")
    except Exception:
        await call.message.answer(caption, parse_mode="Markdown")
    await call.answer()


# ── Практики ──────────────────────────────────────────────────

@router.message(F.text == BTN_PRACTICES)
async def menu_practices(msg: Message):
    await msg.answer(PRACTICES_MENU_TEXT, parse_mode=MD, reply_markup=practices_kb())


@router.callback_query(F.data.startswith("practice_"))
async def cb_practice(call: CallbackQuery):
    data = PRACTICES_DATA.get(call.data)
    if not data:
        await call.answer("Неизвестная практика");
        return
    text = data["text"]
    img_name = data["image"]
    img_path = None
    for ext in ("jpg", "jpeg", "png", "webp"):
        p = os.path.join(PRACTICE_DIR, f"{img_name}.{ext}")
        if os.path.exists(p):
            img_path = p
            break
    try:
        if img_path:
            await call.message.answer_photo(
                FSInputFile(img_path), caption=text, parse_mode="Markdown"
            )
        else:
            await call.message.answer(text, parse_mode=MD)
    except Exception:
        await call.message.answer(text, parse_mode=MD)
    await call.answer()


# ── Статистика ────────────────────────────────────────────────

@router.message(F.text == BTN_MY_STATS)
async def menu_stats(msg: Message):
    u = get_user(msg.from_user.id)
    if not u:
        await msg.answer("Сначала зарегистрируйся — нажми /start");
        return
    await msg.answer(
        STATS_TEMPLATE.format(
            points=u["points"],
            streak=u["streak"],
            survey_list=fmt_moods(get_last_moods(msg.from_user.id, "main", 7)),
            express_list=fmt_moods(get_last_moods(msg.from_user.id, "express", 7)),
        ),
        parse_mode=MD
    )


@router.message(F.text == BTN_TASKS)
async def menu_tasks(msg: Message):
    u = get_user(msg.from_user.id)
    if not u:
        await msg.answer("Сначала зарегистрируйся — нажми /start");
        return
    done = {t["task_type"] for t in get_today_tasks(msg.from_user.id)}
    await msg.answer(
        TASKS_TEMPLATE.format(
            survey_status=DONE_ICON if "survey" in done else TODO_ICON,
            breath_status=DONE_ICON if "breathing" in done else TODO_ICON,
            express_status=DONE_ICON if "express" in done else TODO_ICON,
            points=u["points"],
        ),
        parse_mode=MD
    )


# ── Фаза луны ─────────────────────────────────────────────────

@router.message(F.text == BTN_MOON)
async def menu_moon(msg: Message):
    key = moon_phase_key()
    if key is None:
        await msg.answer("🌙 Не удалось рассчитать фазу луны.\nПопробуй позже.")
        return

    name, emoji_id = MOON_PHASES.get(key, ("Неизвестная фаза", None))
    today_str = date.today().strftime("%d.%m.%Y")

    # Шаблон: <премиум-эмодзи> <фаза луны текстом> <дата>
    if emoji_id:
        prem = moon_prem_tag(emoji_id)
        caption = f"{prem} <b>{name}</b>  {today_str}"
    else:
        caption = f"🌙 <b>{name}</b>  {today_str}"

    if random.random() < 0.30:
        caption += MOON_DISCLAIMER

    photo = moon_photo(key)
    try:
        if photo:
            await msg.answer_photo(FSInputFile(photo), caption=caption, parse_mode="HTML")
        else:
            await msg.answer(
                caption + "\n\n(картинка не найдена в папке moon_photos)",
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error("moon send error: %s", e)
        await msg.answer(caption, parse_mode="HTML")


# ── О боте ─────────────────────────────────────────────────────

@router.message(F.text == BTN_ABOUT)
async def menu_about(msg: Message):
    await msg.answer(ABOUT_TEXT, parse_mode=MD)


# ── Личный кабинет ────────────────────────────────────────────

GENDER_LABELS = {"male": "👦 Мужской", "female": "👧 Женский", "other": "🤷 Другое"}


@router.message(F.text == BTN_CABINET)
@router.message(Command("cabinet"))
async def menu_cabinet(msg: Message):
    u = get_user(msg.from_user.id)
    if not u:
        await msg.answer("Сначала зарегистрируйся — нажми /start");
        return
    uid = msg.from_user.id
    diag_count = get_diagnostic_count(uid)
    last_diags = get_last_diagnostics(uid, 3)
    sub_active = get_weekly_sub_status(uid)
    sub_icon = "🔔" if sub_active else "🔕"
    pos, total = get_top_position(uid)
    gender_label = GENDER_LABELS.get(u["gender"] or "", "не указан")

    diag_lines = ""
    if last_diags:
        for d in last_diags:
            diag_lines += f"\n  • {d['level']} — {d['score']} б. ({d['created_at'][:10]})"
    else:
        diag_lines = "\n  пока нет"

    ws = get_weekly_stats(uid)
    avg_week = ws["avg"] or "—"

    text = (
        f"*👤 Личный кабинет*\n\n"
        f"🏆 Очков: *{u['points']}*\n"
        f"🔥 Серия: *{u['streak']} дн.*\n"
        f"📊 Средний балл за неделю: *{avg_week}*\n"
        f"👤 Пол: *{gender_label}*\n"
        f"⏰ Время опроса: *{u['survey_time']}*\n"
        f"🔬 Диагностик пройдено: *{diag_count}*\n"
        f"{sub_icon} Еженедельная статистика: *{'вкл' if sub_active else 'выкл'}*\n"
        f"📱 Ваша версия бота: *{VERSION}*"
    )

    b = InlineKeyboardBuilder()
    b.button(text="⏰ Изменить время опроса", callback_data="cabinet:time_survey")
    b.button(text="🌅 Изменить время рассылки", callback_data="cabinet:time_morning")
    b.button(text="📊 Статистика", callback_data="cabinet:stats")
    if sub_active:
        b.button(text="🔕 Отписаться от рассылки", callback_data="cabinet:unsub")
    else:
        b.button(text="🔔 Подписаться на рассылку", callback_data="cabinet:sub")
    b.adjust(1)
    await msg.answer(text, parse_mode=MD, reply_markup=b.as_markup())


@router.callback_query(F.data.in_({"cabinet:sub", "cabinet:unsub"}))
async def cb_cabinet_sub(call: CallbackQuery):
    active = call.data == "cabinet:sub"
    set_weekly_sub(call.from_user.id, active)
    icon = "🔔" if active else "🔕"
    status = "вкл" if active else "выкл"
    await call.answer(f"{icon} Еженедельная статистика {status}", show_alert=True)
    await call.message.delete()
    await menu_cabinet(call.message)


@router.callback_query(F.data == "cabinet:stats")
async def cb_cabinet_stats(call: CallbackQuery):
    uid = call.from_user.id

    # Последние опросы
    surveys = get_last_moods(uid, "main", 7)
    if surveys:
        survey_lines = "\n".join(
            f"{ZONE_ICONS.get(r['zone'], '⚪')} {r['score']} баллов — {r['created_at'][:10]}"
            for r in surveys
        )
    else:
        survey_lines = "пока нет данных"

    # Последние экспресс-тесты
    expresses = get_last_moods(uid, "express", 7)
    if expresses:
        express_lines = "\n".join(
            f"{ZONE_ICONS.get(r['zone'], '⚪')} {r['score']} баллов — {r['created_at'][:10]}"
            for r in expresses
        )
    else:
        express_lines = "пока нет данных"

    # Последние диагностики
    diags = get_last_diagnostics(uid, 3)
    if diags:
        diag_lines = "\n".join(
            f"{ZONE_ICONS.get(d['level'].split()[0].lower(), '⚪') if d['level'].startswith('🟡') or d['level'].startswith('🟢') or d['level'].startswith('🔴') or d['level'].startswith('🟠') else '📋'} {d['level']} — {d['score']} б. ({d['created_at'][:10]})"
            for d in diags
        )
    else:
        diag_lines = "пока нет данных"

    text = (
        f"*📊 Твоя статистика*\n\n"
        f"*Последние опросы:*\n{survey_lines}\n\n"
        f"*Последние экспресс-тесты:*\n{express_lines}\n\n"
        f"*Последние диагностики:*\n{diag_lines}"
    )
    b = InlineKeyboardBuilder()
    b.button(text="◀️ В главное меню", callback_data="cabinet:back_main")
    await call.message.edit_text(text, parse_mode=MD, reply_markup=b.as_markup())
    await call.answer()


@router.callback_query(F.data == "cabinet:back_main")
async def cb_cabinet_back(call: CallbackQuery):
    await call.message.delete()
    await call.answer()


@router.callback_query(F.data == "cabinet:diagnostic")
async def cb_cabinet_diagnostic(call: CallbackQuery, state: FSMContext):
    await call.message.edit_reply_markup()
    await _start_diagnostic(call.message, state)
    await call.answer()


@router.callback_query(F.data == "cabinet:gender")
async def cb_cabinet_gender(call: CallbackQuery):
    b = InlineKeyboardBuilder()
    b.button(text="👦 Мужской", callback_data="cgender:male")
    b.button(text="👧 Женский", callback_data="cgender:female")
    b.button(text="🤷 Другое", callback_data="cgender:other")
    b.adjust(3)
    await call.message.edit_text("👤 Выбери пол:", reply_markup=b.as_markup())
    await call.answer()


@router.callback_query(F.data.startswith("cgender:"))
async def cb_cabinet_gender_set(call: CallbackQuery):
    gender = call.data.split(":")[1]
    set_gender(call.from_user.id, gender)
    label = GENDER_LABELS.get(gender, gender)
    await call.answer(f"Пол обновлён: {label}", show_alert=True)
    await call.message.delete()
    await menu_cabinet(call.message)


@router.callback_query(F.data == "cabinet:time_survey")
async def cb_cabinet_time_survey(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text(
        "⏰ Введи новое время вечернего опроса (формат ЧЧ:ММ, например 20:00):"
    )
    await state.set_state(TimeSt.survey)
    await call.answer()


@router.callback_query(F.data == "cabinet:time_morning")
async def cb_cabinet_time_morning(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text(
        "🌅 Введи время утренней рассылки (формат ЧЧ:ММ) или /skip чтобы отключить:"
    )
    await state.set_state(TimeSt.morning)
    await call.answer()


# ── Глубокий опрос (диагностика) ──────────────────────────────

DIAGNOSTIC_QUESTIONS = [
    [
        "🌡 *Диагностика 1/20 — Самочувствие*\n\nКак бы ты оценил своё общее самочувствие сегодня?\n\n_1 — очень плохо  |  5 — отлично_",
        "🌡 *Диагностика 1/20 — Самочувствие*\n\nНасколько ты чувствуешь себя здоровым физически прямо сейчас?\n\n_1 — совсем нет  |  5 — полностью здоров_",
    ],
    [
        "⚡ *Диагностика 2/20 — Энергия*\n\nКак бы ты оценил свой уровень энергии сегодня?\n\n_1 — полностью истощён  |  5 — очень энергичен_",
        "⚡ *Диагностика 2/20 — Энергия*\n\nХватает ли тебе сил на повседневные дела без ощущения усталости?\n\n_1 — совсем не хватает  |  5 — хватает с избытком_",
    ],
    [
        "😴 *Диагностика 3/20 — Сон*\n\nКак ты оцениваешь качество сна за последнюю неделю в целом?\n\n_1 — очень плохо  |  5 — отлично_",
        "😴 *Диагностика 3/20 — Сон*\n\nПросыпаешься ли ты регулярно отдохнувшим?\n\n_1 — никогда  |  5 — всегда_",
    ],
    [
        "😰 *Диагностика 4/20 — Тревога*\n\nНасколько часто ты испытываешь тревогу или беспокойство?\n\n_1 — постоянно  |  5 — крайне редко_",
        "😰 *Диагностика 4/20 — Тревога*\n\nМешает ли тревога твоей повседневной жизни?\n\n_1 — сильно мешает  |  5 — совсем не мешает_",
    ],
    [
        "💛 *Диагностика 5/20 — Настроение*\n\nКаким было твоё настроение в среднем за последние 7 дней?\n\n_1 — очень плохим  |  5 — отличным_",
        "💛 *Диагностика 5/20 — Настроение*\n\nНасколько часто ты испытывал радость или удовольствие на этой неделе?\n\n_1 — никогда  |  5 — каждый день_",
    ],
    [
        "📚 *Диагностика 6/20 — Учёба/работа*\n\nНасколько сильный стресс ты испытываешь из-за учёбы или работы?\n\n_1 — очень сильный  |  5 — совсем нет_",
        "📚 *Диагностика 6/20 — Учёба/работа*\n\nУдаётся ли тебе справляться с учебной или рабочей нагрузкой?\n\n_1 — совсем не удаётся  |  5 — легко справляюсь_",
    ],
    [
        "👥 *Диагностика 7/20 — Общение*\n\nНасколько ты доволен своими отношениями с близкими людьми?\n\n_1 — очень недоволен  |  5 — полностью доволен_",
        "👥 *Диагностика 7/20 — Общение*\n\nЧувствуешь ли ты поддержку от окружающих людей?\n\n_1 — совсем нет  |  5 — постоянно чувствую_",
    ],
    [
        "🍽 *Диагностика 8/20 — Питание*\n\nКак бы ты оценил качество своего питания на этой неделе?\n\n_1 — очень плохо  |  5 — очень хорошо_",
        "🍽 *Диагностика 8/20 — Питание*\n\nПитаешься ли ты регулярно, без длительных пропусков еды?\n\n_1 — нет, часто пропускаю  |  5 — да, регулярно_",
    ],
    [
        "🏃 *Диагностика 9/20 — Активность*\n\nНасколько регулярно ты занимаешься физической активностью?\n\n_1 — совсем нет  |  5 — каждый день_",
        "🏃 *Диагностика 9/20 — Активность*\n\nПомогает ли движение тебе чувствовать себя лучше?\n\n_1 — нет  |  5 — очень помогает_",
    ],
    [
        "🎯 *Диагностика 10/20 — Концентрация*\n\nНасколько легко тебе сосредоточиться на важных задачах?\n\n_1 — очень трудно  |  5 — очень легко_",
        "🎯 *Диагностика 10/20 — Концентрация*\n\nЧасто ли ты замечаешь, что твои мысли блуждают во время работы?\n\n_1 — постоянно  |  5 — крайне редко_",
    ],
    [
        "😤 *Диагностика 11/20 — Раздражительность*\n\nКак часто ты раздражаешься или злишься без серьёзной причины?\n\n_1 — очень часто  |  5 — крайне редко_",
        "😤 *Диагностика 11/20 — Раздражительность*\n\nЛегко ли тебе сохранять спокойствие в напряжённых ситуациях?\n\n_1 — очень трудно  |  5 — легко_",
    ],
    [
        "🫂 *Диагностика 12/20 — Одиночество*\n\nНасколько часто ты чувствуешь себя одиноким?\n\n_1 — постоянно  |  5 — никогда_",
        "🫂 *Диагностика 12/20 — Одиночество*\n\nЕсть ли рядом человек, которому ты можешь доверять и с кем поговорить?\n\n_1 — нет  |  5 — да, есть_",
    ],
    [
        "🌟 *Диагностика 13/20 — Смысл*\n\nЧувствуешь ли ты смысл и цель в своей повседневной жизни?\n\n_1 — совсем нет  |  5 — очень ясно чувствую_",
        "🌟 *Диагностика 13/20 — Смысл*\n\nНасколько ты доволен тем, как проводишь своё время?\n\n_1 — совсем не доволен  |  5 — полностью доволен_",
    ],
    [
        "🪞 *Диагностика 14/20 — Самооценка*\n\nНасколько ты доволен собой в целом?\n\n_1 — совсем не доволен  |  5 — очень доволен_",
        "🪞 *Диагностика 14/20 — Самооценка*\n\nЧасто ли ты критикуешь себя за ошибки?\n\n_1 — постоянно критикую  |  5 — отношусь с пониманием_",
    ],
    [
        "🫀 *Диагностика 15/20 — Здоровье тела*\n\nБеспокоят ли тебя физические симптомы стресса (головная боль, напряжение, боли в спине)?\n\n_1 — постоянно беспокоят  |  5 — совсем нет_",
        "🫀 *Диагностика 15/20 — Здоровье тела*\n\nНасколько ты заботишься о своём физическом здоровье?\n\n_1 — совсем не забочусь  |  5 — очень тщательно_",
    ],
    [
        "📱 *Диагностика 16/20 — Цифровая нагрузка*\n\nНасколько часто использование телефона или соцсетей вызывает у тебя стресс?\n\n_1 — постоянно  |  5 — никогда_",
        "📱 *Диагностика 16/20 — Цифровая нагрузка*\n\nУдаётся ли тебе делать перерывы от экранов в течение дня?\n\n_1 — никогда  |  5 — регулярно_",
    ],
    [
        "🛋 *Диагностика 17/20 — Отдых*\n\nУдаётся ли тебе полноценно отдыхать и восстанавливаться?\n\n_1 — совсем не удаётся  |  5 — легко восстанавливаюсь_",
        "🛋 *Диагностика 17/20 — Отдых*\n\nЕсть ли у тебя хобби или занятия, которые помогают расслабиться?\n\n_1 — нет  |  5 — да, регулярно_",
    ],
    [
        "🔭 *Диагностика 18/20 — Будущее*\n\nНасколько оптимистично ты смотришь на своё будущее?\n\n_1 — очень пессимистично  |  5 — очень оптимистично_",
        "🔭 *Диагностика 18/20 — Будущее*\n\nВызывают ли мысли о будущем у тебя тревогу?\n\n_1 — постоянно вызывают  |  5 — совсем нет_",
    ],
    [
        "🛡 *Диагностика 19/20 — Стрессоустойчивость*\n\nНасколько хорошо ты справляешься со стрессом, когда он возникает?\n\n_1 — очень плохо  |  5 — отлично_",
        "🛡 *Диагностика 19/20 — Стрессоустойчивость*\n\nЕсть ли у тебя проверенные способы снять стресс (дыхание, спорт, общение)?\n\n_1 — нет  |  5 — да, и они помогают_",
    ],
    [
        "🌈 *Диагностика 20/20 — Удовлетворённость*\n\nНасколько ты в целом доволен своей жизнью прямо сейчас?\n\n_1 — совсем не доволен  |  5 — очень доволен_",
        "🌈 *Диагностика 20/20 — Удовлетворённость*\n\nЕсли бы ты мог изменить что-то одно в своей жизни — насколько это улучшило бы твоё состояние?\n\n_1 — изменило бы всё  |  5 — и так всё хорошо_",
    ],
]

TOTAL_DIAG = len(DIAGNOSTIC_QUESTIONS)


def get_diag_questions():
    return [random.choice(block) for block in DIAGNOSTIC_QUESTIONS]


def calc_diagnostic(score: int) -> tuple[str, str]:
    pct = score / (TOTAL_DIAG * 5) * 100
    if pct >= 75:
        return ("🟢 Высокий уровень благополучия",
                "Ты в хорошей форме! Продолжай поддерживать баланс — ежедневные опросы и практики помогут сохранить результат.")
    elif pct >= 50:
        return ("🟡 Умеренный уровень стресса",
                "Есть зоны для улучшения. Обрати внимание на сон, физическую активность и моменты отдыха. Дыхательные практики помогут снизить напряжение.")
    elif pct >= 30:
        return ("🟠 Повышенный стресс",
                "Стресс заметно влияет на твою жизнь. Рекомендую регулярно проходить вечерний опрос, использовать практики расслабления и по возможности поговорить с близкими.")
    else:
        return ("🔴 Высокий уровень стресса",
                "Ситуация требует внимания. Попробуй начать с малого — одна дыхательная практика в день. Если так продолжается больше недели — обратись к специалисту.")


async def _start_diagnostic(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    ldt = get_last_diagnostic_dt(uid)
    if ldt:
        diff = (datetime.now(pytz.utc) - ldt.replace(tzinfo=pytz.utc)).total_seconds()
        if diff < COOLDOWN_DIAGNOSTIC:
            mins = int((COOLDOWN_DIAGNOSTIC - diff) // 60) + 1
            await msg.answer(
                f"🔬 Глубокий опрос можно проходить *2 раза в сутки*.\n"
                f"Следующий доступен через *{mins} мин.*",
                parse_mode=MD
            )
            return
    questions = get_diag_questions()
    await state.set_state(DiagnosticSt.question)
    await state.update_data(d_answers=[], d_questions=questions)
    await msg.answer(
        "🔬 *Глубокий опрос* — 20 вопросов\n\n"
        "Это займёт около 5 минут. Отвечай честно — результат только для тебя.\n\n"
        "Поехали 👇",
        parse_mode=MD
    )
    await msg.answer(questions[0], parse_mode=MD, reply_markup=likert_kb("dq"))


@router.message(F.text == BTN_DIAGNOSTIC)
async def menu_diagnostic(msg: Message, state: FSMContext):
    await _start_diagnostic(msg, state)


@router.message(Command("start_diagnostic"))
async def cmd_diagnostic(msg: Message, state: FSMContext):
    await _start_diagnostic(msg, state)


@router.callback_query(DiagnosticSt.question, F.data.startswith("dq:"))
async def cb_diagnostic_q(call: CallbackQuery, state: FSMContext):
    value = int(call.data.split(":")[1])
    data = await state.get_data()
    answers = data.get("d_answers", [])
    questions = data.get("d_questions", get_diag_questions())
    answers.append(value)
    await call.message.edit_reply_markup()

    if len(answers) < len(questions):
        await state.update_data(d_answers=answers)
        await call.message.answer(
            questions[len(answers)], parse_mode=MD, reply_markup=likert_kb("dq")
        )
        await call.answer()
        return

    uid = call.from_user.id
    score = sum(answers)
    level, rec = calc_diagnostic(score)
    save_diagnostic(uid, score, level, answers)
    add_points(uid, POINTS_DIAGNOSTIC)
    log_task(uid, "diagnostic", POINTS_DIAGNOSTIC)

    await call.message.answer(
        f"🔬 *Результат диагностики*\n\n"
        f"{level}\n\n"
        f"Твой балл: *{score} из {TOTAL_DIAG * 5}*\n\n"
        f"💡 *Рекомендация:*\n{rec}\n\n"
        f"✨ *+{POINTS_DIAGNOSTIC} очков* начислено!",
        parse_mode=MD
    )
    await state.clear()
    await call.answer()


# ── Администратор ─────────────────────────────────────────────

def _adm(msg): return msg.from_user.id in ADMIN_IDS


@router.message(Command("ping"))
async def cmd_ping(msg: Message):
    await msg.answer(
        f"v{VERSION} — бот работает\n"
        f"Твой ID: {msg.from_user.id}\n"
        f"Ты админ: {_adm(msg)}",
        parse_mode=None
    )


@router.message(Command("admin"))
async def cmd_admin(msg: Message):
    if not _adm(msg): return
    await msg.answer(ADMIN_HELP_TEXT, parse_mode=None)


@router.message(Command("admin_stats"))
async def cmd_admin_stats(msg: Message):
    if not _adm(msg): return
    s = admin_general_stats()
    z = s["zones"]
    await msg.answer(
        f"📊 Общая статистика\n\n"
        f"👥 Всего: {s['total']}\n"
        f"📆 Активны за 7 дней: {s['active_7d']}\n"
        f"📈 Средний балл: {s['avg_score']}\n\n"
        f"Зоны: 🟢 {z.get('green', 0)}  🟡 {z.get('yellow', 0)}  🔴 {z.get('red', 0)}\n\n"
        f"Пол:\n" + "\n".join(f"  {k}: {v}" for k, v in s["genders"].items()),
        parse_mode=None
    )


@router.message(Command("admin_users"))
async def cmd_admin_users(msg: Message):
    if not _adm(msg): return
    users = admin_all_users()
    if not users:
        await msg.answer("Нет пользователей.", parse_mode=None);
        return
    lines = []
    for u in users:
        uid = u['user_id']
        name = u['first_name'] or str(uid)
        mention = f'<a href="tg://user?id={uid}">{name}</a>'

        # Если юзер заблокировал бота, пометим его значком
        blocked_tag = " 🚫 (Блок)" if u['is_blocked'] else ""

        lines.append(
            f"{mention}{blocked_tag}\n"
            f"🏆 {u['points']} оч  🔥 {u['streak']} дн  ⏰ {u['survey_time']}"
        )
    chunk = []
    for line in lines:
        chunk.append(line)
        if len("\n\n".join(chunk)) > 3800:
            await msg.answer("\n\n".join(chunk[:-1]), parse_mode="HTML")
            chunk = [chunk[-1]]
    if chunk:
        await msg.answer("\n\n".join(chunk), parse_mode="HTML")


@router.message(Command("admin_blocked"))
async def cmd_admin_blocked(msg: Message):
    if not _adm(msg): return
    users = get_blocked_users()
    if not users:
        await msg.answer("Нет пользователей, заблокировавших бота.", parse_mode=None)
        return

    lines = []
    for u in users:
        uid = u['user_id']
        name = u['first_name'] or str(uid)
        mention = f'<a href="tg://user?id={uid}">{name}</a>'
        lines.append(f"🚫 {mention} (ID: {uid})")

    chunk = []
    for line in lines:
        chunk.append(line)
        if len("\n".join(chunk)) > 3800:
            await msg.answer("<b>Заблокировали бота:</b>\n\n" + "\n".join(chunk[:-1]), parse_mode="HTML")
            chunk = [chunk[-1]]
    if chunk:
        await msg.answer("<b>Заблокировали бота:</b>\n\n" + "\n".join(chunk), parse_mode="HTML")


@router.message(Command("export_stats"))
async def cmd_export(msg: Message):
    if not _adm(msg): return
    rows = export_moods_csv(30)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id", "user_id", "username", "score", "zone", "type", "created_at"])
    for r in rows:
        w.writerow([r["id"], r["user_id"], r["username"], r["score"], r["zone"], r["mood_type"], r["created_at"]])
    await msg.answer_document(
        BufferedInputFile(buf.getvalue().encode("utf-8"), filename="stats_30d.csv"),
        caption="📂 Экспорт за 30 дней"
    )


@router.message(Command("bds"))
async def cmd_bds(msg: Message):
    """Секретная команда — отправляет файл базы данных."""
    if not _adm(msg): return
    if not os.path.exists(DB_PATH):
        await msg.answer("БД не найдена по пути: " + DB_PATH);
        return
    today = date.today().strftime("%Y-%m-%d")
    with open(DB_PATH, "rb") as f:
        data = f.read()
    await msg.answer_document(
        BufferedInputFile(data, filename=f"antistress_{today}.db"),
        caption=f"🗄 База данных на {today}\nРазмер: {len(data) // 1024} КБ"
    )


async def cmd_add_points(msg: Message):
    if not _adm(msg): return
    parts = (msg.text or "").split()
    if len(parts) != 3:
        await msg.answer("Формат: /add_points user_id points", parse_mode=None);
        return
    try:
        uid, pts = int(parts[1]), int(parts[2])
    except ValueError:
        await msg.answer("Неверные аргументы.", parse_mode=None);
        return
    total = add_points(uid, pts)
    await msg.answer(f"✅ Начислено {pts} очков пользователю {uid}. Итого: {total}", parse_mode=None)


@router.message(Command("set_points"))
async def cmd_set_points(msg: Message):
    if not _adm(msg): return
    parts = (msg.text or "").split()
    if len(parts) != 3:
        await msg.answer("Формат: /set_points user_id points", parse_mode=None);
        return
    try:
        uid, pts = int(parts[1]), int(parts[2])
    except ValueError:
        await msg.answer("Неверные аргументы.", parse_mode=None);
        return
    set_points_value(uid, pts)
    await msg.answer(f"✅ Очки пользователя {uid} установлены: {pts}", parse_mode=None)


@router.message(Command("set_streak"))
async def cmd_set_streak(msg: Message):
    if not _adm(msg): return
    parts = (msg.text or "").split()
    if len(parts) != 3:
        await msg.answer("Формат: /set_streak user_id дни");
        return
    try:
        uid, days = int(parts[1]), int(parts[2])
    except ValueError:
        await msg.answer("Неверные аргументы.");
        return
    set_streak(uid, days)
    u = get_user(uid)
    name = (u["first_name"] or str(uid)) if u else str(uid)
    await msg.answer(f"✅ Серия пользователя {name} ({uid}) установлена: {days} дн.")


@router.message(Command("weekly_preview"))
async def cmd_weekly_preview(msg: Message):
    if not _adm(msg): return
    uid = msg.from_user.id
    s = get_weekly_stats(uid)
    if not s["count"]:
        await msg.answer("Нет данных за последние 7 дней.");
        return
    avg = s["avg"] or 0
    avg_prev = s["avg_prev"] or 0
    z = s["zones"]
    diff = round(avg - avg_prev, 1) if avg_prev else None

    if diff is None:
        trend = "нет данных за прошлую неделю"
    elif diff < 0:
        trend = f"Стресс снизился на {abs(diff)} б. — хорошая динамика!"
    elif diff == 0:
        trend = "Уровень стресса не изменился"
    else:
        trend = f"Стресс вырос на {diff} б. — обрати внимание"

    red_count = z.get("red", 0)
    if red_count >= 3:
        rec = "Много красных зон — попробуй дыхательные практики и режим сна."
    elif z.get("green", 0) >= 4:
        rec = "Отличная неделя! Продолжай в том же духе."
    else:
        rec = "Небольшие ежедневные практики помогут улучшить динамику."

    await msg.answer(
        f"📊 Предпросмотр пятничного отчёта\n\n"
        f"Опросов: {s['count']}\n"
        f"Средний балл: {avg}\n"
        f"Зоны: 🟢 {z.get('green', 0)}  🟡 {z.get('yellow', 0)}  🔴 {z.get('red', 0)}\n\n"
        f"Динамика: {trend}\n\n"
        f"Рекомендация: {rec}"
    )


PREMIUM_EMOJI = {
    "⭐": "5287587057613429769",
    "О": "5188426394576636529",
    "Б": "5188619865673452995",
    "Н": "5188183870658334974",
    "В": "5188237553454564512",
    "Л": "5188275868857812979",
    "Е": "5188652859612219319",
    "И": "5188264366935394691",
    "v": "5188623602294999385",
    "V": "5188623602294999385",
    ".": "5188610760342787638",
    "0": "5188675532744576107",
    "1": "5188307557126524632",
    "2": "5188417224821460749",
    "3": "5188480700143129440",
    "4": "5188179579986005169",
    "5": "5188175671565764828",
    "6": "5188594168884119951",
    "7": "5188377912985799970",
    "8": "5188548801144573878",
    "9": "5188152972663608028",
}


def _prem_tag(char: str) -> str:
    ch = char.upper() if char not in ("v", ".") else char
    if ch not in PREMIUM_EMOJI and char not in PREMIUM_EMOJI:
        return char
    eid = PREMIUM_EMOJI.get(ch) or PREMIUM_EMOJI.get(char)
    if char.isdigit():
        ph = f"{char}️⃣"
    elif char in ("⭐",):
        ph = "⭐"
    elif char == ".":
        ph = "🔣"
    elif char.lower() == "v":
        ph = "🔠"
    else:
        ph = "🔤"
    return f'<tg-emoji emoji-id="{eid}">{ph}</tg-emoji>'


def build_update_message(version: str, body: str) -> str:
    line1 = _prem_tag("⭐")
    for ch in "ОБНОВЛЕНИЕ":
        line1 += _prem_tag(ch)
    line1 += _prem_tag("⭐")

    version_dots = ".".join(version)
    line2 = _prem_tag("⭐")
    line2 += _prem_tag("v")
    for ch in version_dots:
        line2 += _prem_tag(ch)
    line2 += _prem_tag("⭐")

    safe_body = body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    return f"{line1}\n{line2}\n\n{safe_body}"


_pending_updates: dict = {}


@router.message(Command("rat"))
@router.message(Command("update_notify"))
async def cmd_update_notify(msg: Message):
    if not _adm(msg): return
    parts = (msg.text or "").split(None, 2)
    if len(parts) < 3:
        await msg.answer(
            "Формат: /update_notify версия текст\n\n"
            "Пример: /update_notify 051 Новые вопросы и исправления\n"
            "Версия пишется без точек: 051 → v0.5.1"
        )
        return

    version = parts[1].strip()
    body = parts[2].strip()

    full_text = build_update_message(version, body)

    b = InlineKeyboardBuilder()
    b.button(text="✅ Отправить всем", callback_data="send_update")
    b.button(text="❌ Отмена", callback_data="cancel_update")
    b.adjust(2)

    _pending_updates[msg.from_user.id] = full_text

    await msg.answer(
        f"Предпросмотр:\n\n{full_text}\n\nОтправить всем пользователям?",
        parse_mode="HTML",
        reply_markup=b.as_markup()
    )


@router.callback_query(F.data == "send_update")
async def cb_send_update(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    text = _pending_updates.pop(call.from_user.id, None)
    if not text:
        await call.answer("Текст не найден. Попробуй ещё раз.", show_alert=True)
        return

    await call.message.edit_text("⏳ Отправляю...")
    await call.answer()

    users = get_all_users()
    sent, failed = 0, 0
    for u in users:
        try:
            await call.bot.send_message(u["user_id"], text, parse_mode="HTML")
            sent += 1
        except Exception:
            failed += 1

    await call.message.edit_text(
        f"✅ Рассылка обновления завершена\n\n"
        f"📤 Отправлено: {sent}\n"
        f"❌ Не доставлено: {failed}"
    )


@router.callback_query(F.data == "cancel_update")
async def cb_cancel_update(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    _pending_updates.pop(call.from_user.id, None)
    await call.message.edit_text("❌ Рассылка отменена.")
    await call.answer()


@router.message(Command("broadcast"))
async def cmd_broadcast(msg: Message, state: FSMContext):
    if not _adm(msg): return
    parts = (msg.text or "").split(None, 1)
    if len(parts) > 1 and parts[1].strip():
        await _do_broadcast(msg, parts[1].strip())
    else:
        await msg.answer(
            "✉️ Рассылка\n\nОтправь текст сообщения для рассылки всем пользователям.\n\n"
            "Для отмены напиши /cancel"
        )
        await state.set_state(BroadcastSt.waiting_text)


@router.message(BroadcastSt.waiting_text)
async def broadcast_text_received(msg: Message, state: FSMContext, bot: Bot):
    if not _adm(msg): return
    text = (msg.text or "").strip()
    if text.lower() in ("/cancel", "отмена"):
        await msg.answer("❌ Рассылка отменена.")
        await state.clear()
        return
    await state.clear()
    await _do_broadcast(msg, text, bot)


async def _do_broadcast(msg: Message, text: str, bot: Bot = None):
    _bot = bot or msg.bot
    users = get_all_users()
    sent = 0
    failed = 0
    broadcast_text = f"📢 Новое сообщение от администратора\n\n{text}"
    for u in users:
        try:
            await _bot.send_message(u["user_id"], broadcast_text, parse_mode=None)
            sent += 1
        except Exception:
            failed += 1
    await msg.answer(
        f"✅ Рассылка завершена\n\n"
        f"📤 Отправлено: {sent}\n"
        f"❌ Не доставлено: {failed}",
        parse_mode=None
    )


# ================================================================
#  ПЛАНИРОВЩИК
# ================================================================

async def job_facts(bot: Bot):
    fact = random.choice(load_facts())
    for u in get_all_users():
        try:
            await bot.send_message(u["user_id"], FACT_PREFIX + fact)
        except Exception:
            pass


async def job_quote(bot: Bot):
    quote = random.choice(load_quotes())
    for u in get_all_users():
        try:
            await bot.send_message(u["user_id"], QUOTE_PREFIX + quote)
        except Exception:
            pass


async def job_evening(bot: Bot):
    now = datetime.now(MSK).strftime("%H:%M")
    for u in get_users_by_survey_time(now):
        try:
            await bot.send_message(
                u["user_id"], EVENING_PUSH, parse_mode=MD,
                reply_markup=survey_start_kb()
            )
        except Exception:
            pass


_last_morning_cap_idx = None


async def job_morning(bot: Bot):
    global _last_morning_cap_idx
    now = datetime.now(MSK).strftime("%H:%M")
    img = rand_morning_img()
    indices = list(range(len(MORNING_CAPTIONS)))
    if _last_morning_cap_idx is not None and len(indices) > 1:
        indices = [i for i in indices if i != _last_morning_cap_idx]
    chosen_idx = random.choice(indices)
    _last_morning_cap_idx = chosen_idx
    cap = MORNING_CAPTIONS[chosen_idx]
    for u in get_users_by_morning_time(now):
        try:
            if img:
                await bot.send_photo(u["user_id"], FSInputFile(img), caption=cap)
            else:
                await bot.send_message(u["user_id"], cap)
        except Exception:
            pass


async def job_weekly_stats(bot: Bot):
    users = get_weekly_sub_users()
    for u in users:
        uid = u["user_id"]
        s = get_weekly_stats(uid)
        if not s["count"]:
            continue
        avg = s["avg"] or 0
        avg_prev = s["avg_prev"] or 0
        z = s["zones"]
        diff = round(avg - avg_prev, 1) if avg_prev else None

        if diff is None:
            trend = "нет данных за прошлую неделю"
        elif diff < 0:
            trend = f"🟢 Стресс снизился на {abs(diff)} б. — хорошая динамика!"
        elif diff == 0:
            trend = "🟡 Уровень стресса не изменился"
        else:
            trend = f"🔴 Стресс вырос на {diff} б. — обрати внимание"

        red_count = z.get("red", 0)
        if red_count >= 3:
            rec = "Много красных зон — попробуй регулярные дыхательные практики и режим сна."
        elif z.get("green", 0) >= 4:
            rec = "Отличная неделя! Продолжай в том же духе."
        else:
            rec = "Средние результаты — небольшие ежедневные практики помогут улучшить динамику."

        text = (
            f"📊 *Еженедельный отчёт*\n\n"
            f"Опросов пройдено: *{s['count']}*\n"
            f"Средний балл: *{avg}*\n"
            f"Зоны: 🟢 {z.get('green', 0)}  🟡 {z.get('yellow', 0)}  🔴 {z.get('red', 0)}\n\n"
            f"Динамика: {trend}\n\n"
            f"💡 {rec}\n\n"
            f"_Хорошей недели!_ 🤍"
        )
        try:
            await bot.send_message(uid, text, parse_mode="Markdown")
        except Exception:
            pass


def setup_scheduler(bot: Bot):
    s = AsyncIOScheduler(timezone=MSK)
    fh, fm = map(int, FACTS_TIME.split(":"))
    qh, qm = map(int, QUOTE_TIME.split(":"))
    wh, wm = map(int, WEEKLY_TIME.split(":"))
    s.add_job(job_facts, "cron", hour=fh, minute=fm, kwargs={"bot": bot})
    s.add_job(job_quote, "cron", hour=qh, minute=qm, kwargs={"bot": bot})
    s.add_job(job_weekly_stats, "cron", day_of_week=WEEKLY_DAY,
              hour=wh, minute=wm, kwargs={"bot": bot})
    s.add_job(job_evening, "cron", minute="*", kwargs={"bot": bot})
    s.add_job(job_morning, "cron", minute="*", kwargs={"bot": bot})
    return s


# ================================================================
#  ЗАПУСК
# ================================================================

async def main():
    init_db()
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=None),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    sched = setup_scheduler(bot)
    sched.start()
    logger.info("Бот %s v%s запущен, БД: %s", BOT_NAME, VERSION, DB_PATH)

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        sched.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())