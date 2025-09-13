import logging
import sqlite3
from datetime import datetime, timedelta
import aiohttp
import asyncio
from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, BufferedInputFile, WebAppInfo
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from contextlib import contextmanager
import matplotlib.pyplot as plt
import io
import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from matplotlib.dates import DateFormatter
from matplotlib.ticker import FuncFormatter
import statistics
import os

from flask import Flask, render_template, jsonify
import threading

app = Flask(__name__)


@app.route('/')
def home():
    return "Bot is alive!"

@app.route('/webapp')
def webapp():
    """Telegram Mini App страница."""
    return render_template("webapp.html")

@app.route('/api/portfolio')
def api_portfolio():
    """API endpoint для получения данных портфеля."""
    try:
        items = get_items_from_db()
        if not items:
            return jsonify({
                'totalValue': '0₴',
                'totalItems': '0', 
                'totalProfit': '0₴',
                'profitPercent': '0%'
            })
        
        total_buy_uah = 0
        total_now_uah = 0
        total_items = len(items)
        
        for user_id, name, qty, buy_uah, buy_usd in items:
            pos_buy_uah = buy_uah * qty
            total_buy_uah += pos_buy_uah
            
            # Получаем текущую цену из MarketCSGO кэша
            item_name_lower = name.lower()
            if item_name_lower in marketcsgo_prices_cache:
                current_price_usd = marketcsgo_prices_cache[item_name_lower]
                if current_price_usd:
                    total_now_uah += current_price_usd * USD_TO_UAH * qty
                else:
                    total_now_uah += pos_buy_uah
            else:
                total_now_uah += pos_buy_uah
        
        total_profit_uah = total_now_uah - total_buy_uah
        profit_pct = (total_profit_uah / total_buy_uah) * 100 if total_buy_uah > 0 else 0
        
        return jsonify({
            'totalValue': f'{total_now_uah:,.0f}₴',
            'totalItems': str(total_items),
            'totalProfit': f'{total_profit_uah:+,.0f}₴',
            'profitPercent': f'{profit_pct:+.1f}%'
        })
        
    except Exception as e:
        return jsonify({
            'totalValue': 'Ошибка',
            'totalItems': '0',
            'totalProfit': 'Ошибка', 
            'profitPercent': 'Ошибка'
        })


def run_flask():
    app.run(host='0.0.0.0', port=8080)


threading.Thread(target=run_flask, daemon=True).start()

# --- КОНФИГУРАЦИЯ ---
API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DB_NAME = "portfolio.db"
USD_TO_UAH = 41.5  # Фиксированный курс USD к UAH
NOTIFICATION_THRESHOLD_PERCENT = 2.0  # Порог изменения портфеля для уведомлений
ITEMS_PER_PAGE = 2  # Количество предметов на одной странице отчета

# Настройка логирования для отслеживания ошибок
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
# Уменьшаем уровень для логов, которые могут "флудить"
logging.getLogger('aiogram').setLevel(logging.WARNING)

# Инициализация бота, диспетчера и роутера
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
# FSM States для массовых операций
class BulkStates(StatesGroup):
    waiting_for_bulk_add = State()
    waiting_for_bulk_remove = State()
    waiting_for_steam_url = State()

router = Router()
dp = Dispatcher()
dp.include_router(router)
scheduler = AsyncIOScheduler()

# Глобальный кэш для цен с MarketCSGO, чтобы не делать лишних запросов
marketcsgo_prices_cache = {}
last_cache_update = None
CACHE_TTL = timedelta(minutes=30)
# Кэш для цен портфеля
portfolio_prices_cache = {}
# Мультиисточники кэш
multisource_prices_cache = {}
last_multisource_update = None
# Хранилище последних известных цен для анализа роста
last_prices_snapshot = {}


# --- FSM (Finite State Machine) для редактирования и отслеживания цен ---
class EditItemState(StatesGroup):
    choosing_what_to_edit = State()
    waiting_for_new_quantity = State()
    waiting_for_new_price = State()


class PriceAlertState(StatesGroup):
    waiting_for_item_name = State()
    waiting_for_target_price = State()
    waiting_for_direction = State()


class EditPriceAlertState(StatesGroup):
    choosing_what_to_edit_alert = State()
    waiting_for_new_alert_price = State()
    waiting_for_new_alert_direction = State()


class NotificationSettingsState(StatesGroup):
    waiting_for_threshold = State()
    waiting_for_watchlist_item = State()


# --- РАБОТА С БАЗОЙ ДАННЫХ ---
async def safe_edit_or_send(callback: CallbackQuery, *, text: str | None = None, photo: BufferedInputFile | None = None, reply_markup=None):
    """Безопасное редактирование или отправка сообщения для избежания InaccessibleMessage ошибок."""
    try:
        if not callback.from_user:
            await callback.answer("❌ Ошибка пользователя")
            return
            
        chat_id = callback.message.chat.id if getattr(callback.message, "chat", None) else callback.from_user.id
        
        if text is not None:
            if getattr(callback.message, "chat", None) and hasattr(callback.message, "message_id"):
                try:
                    await bot.edit_message_text(text, chat_id=chat_id, message_id=callback.message.message_id, reply_markup=reply_markup)
                except Exception:
                    await bot.send_message(chat_id, text, reply_markup=reply_markup)
            else:
                await bot.send_message(chat_id, text, reply_markup=reply_markup)
        elif photo is not None:
            await bot.send_photo(chat_id, photo, reply_markup=reply_markup)
        
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в safe_edit_or_send: {e}")
        try:
            await callback.answer("❌ Ошибка обработки")
        except:
            pass

@contextmanager
def get_db_cursor():
    """Контекстный менеджер для работы с БД."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        yield cursor, conn
    finally:
        conn.commit()
        conn.close()


def init_db():
    """Инициализация базы данных и создание таблиц, если их нет."""
    with get_db_cursor() as (cur, _):
        cur.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                quantity INTEGER,
                buy_price_uah REAL,
                buy_price_usd REAL,
                added_at TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_history (
                timestamp TEXT PRIMARY KEY,
                value_uah REAL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                is_subscribed INTEGER DEFAULT 1
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS last_known_value (
                id INTEGER PRIMARY KEY DEFAULT 1,
                value_uah REAL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                item_name TEXT,
                target_price REAL,
                direction TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_notification_settings (
                user_id INTEGER PRIMARY KEY,
                threshold_percent REAL DEFAULT 5.0,
                check_individual_items INTEGER DEFAULT 1,
                check_portfolio_total INTEGER DEFAULT 1,
                last_item_prices TEXT DEFAULT '{}'
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS item_watch_list (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                item_name TEXT,
                last_price_uah REAL,
                created_at TEXT,
                UNIQUE(user_id, item_name)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_history_multisource (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_name TEXT,
                timestamp TEXT,
                source TEXT,
                price_usd REAL,
                median_price_usd REAL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                item_name TEXT,
                price_usd REAL,
                quantity INTEGER
            )
        """)
    logging.info("База данных инициализирована.")


def add_item_to_db(name, qty, buy_price_uah):
    """Добавление предмета в базу данных."""
    buy_price_usd = round(buy_price_uah / USD_TO_UAH, 2)
    with get_db_cursor() as (cur, _):
        cur.execute(
            "INSERT INTO items (name, quantity, buy_price_uah, buy_price_usd, added_at) VALUES (?, ?, ?, ?, ?)",
            (name, qty, buy_price_uah, buy_price_usd,
             datetime.now().isoformat()))
    logging.info(f"Предмет '{name}' добавлен в БД.")


def get_items_from_db():
    """Получение всех предметов из базы данных."""
    with get_db_cursor() as (cur, _):
        cur.execute(
            "SELECT id, name, quantity, buy_price_uah, buy_price_usd FROM items"
        )
        rows = cur.fetchall()
    return rows


def delete_item_by_id(item_id):
    """Удаление предмета по ID."""
    with get_db_cursor() as (cur, _):
        cur.execute("DELETE FROM items WHERE id = ?", (item_id, ))
        return cur.rowcount > 0


def update_item_quantity(item_id, new_quantity):
    """Обновление количества предмета."""
    with get_db_cursor() as (cur, _):
        cur.execute("UPDATE items SET quantity = ? WHERE id = ?",
                    (new_quantity, item_id))


def update_item_price(item_id, new_price_uah):
    """Обновление цены закупки предмета."""
    new_price_usd = round(new_price_uah / USD_TO_UAH, 2)
    with get_db_cursor() as (cur, _):
        cur.execute(
            "UPDATE items SET buy_price_uah = ?, buy_price_usd = ? WHERE id = ?",
            (new_price_uah, new_price_usd, item_id))


def save_portfolio_value(value_uah):
    """Сохранение текущей стоимости портфеля с точной датой и временем."""
    timestamp = datetime.now().isoformat()
    with get_db_cursor() as (cur, conn):
        cur.execute(
            "INSERT OR REPLACE INTO portfolio_history (timestamp, value_uah) VALUES (?, ?)",
            (timestamp, value_uah))
    logging.info(
        f"Стоимость портфеля сохранена: {value_uah}₴ на момент {timestamp}.")


def get_portfolio_history():
    """Получение истории стоимости портфеля."""
    with get_db_cursor() as (cur, _):
        cur.execute(
            "SELECT timestamp, value_uah FROM portfolio_history ORDER BY timestamp"
        )
        rows = cur.fetchall()
    return rows


def get_total_buy_price():
    """Рассчитывает общую закупочную стоимость портфеля."""
    with get_db_cursor() as (cur, _):
        cur.execute("SELECT SUM(quantity * buy_price_uah) FROM items")
        result = cur.fetchone()
        return result[0] if result and result[0] is not None else 0


def get_subscribed_users():
    """Получение ID всех подписанных пользователей."""
    with get_db_cursor() as (cur, _):
        cur.execute("SELECT user_id FROM users WHERE is_subscribed = 1")
        return [row[0] for row in cur.fetchall()]


def subscribe_user(user_id):
    """Добавление пользователя в список для уведомлений."""
    with get_db_cursor() as (cur, _):
        cur.execute(
            "INSERT OR REPLACE INTO users (user_id, is_subscribed) VALUES (?, 1)",
            (user_id, ))
    logging.info(f"Пользователь {user_id} подписан на уведомления.")


def get_last_known_value():
    """Получение последней известной стоимости портфеля."""
    with get_db_cursor() as (cur, _):
        cur.execute("SELECT value_uah FROM last_known_value WHERE id = 1")
        result = cur.fetchone()
        return result[0] if result else None


def save_last_known_value(value_uah):
    """Сохранение последней известной стоимости портфеля."""
    with get_db_cursor() as (cur, _):
        cur.execute(
            "INSERT OR REPLACE INTO last_known_value (id, value_uah) VALUES (1, ?)",
            (value_uah, ))
    logging.info(f"Сохранена последняя известная стоимость: {value_uah}₴")


def get_item_category(item_name):
    """Определяет категорию предмета по его названию."""
    item_name_lower = item_name.lower()
    if 'knife' in item_name_lower:
        return '🔪 Ножи'
    if 'gloves' in item_name_lower:
        return '🧤 Перчатки'
    if 'case' in item_name_lower:
        return '📦 Кейсы'
    if 'capsule' in item_name_lower:
        return '💊 Капсулы'
    if 'sticker' in item_name_lower:
        return '🏷️ Стикеры'
    if 'graffiti' in item_name_lower:
        return '🎨 Граффити'
    if 'pin' in item_name_lower:
        return '📌 Пины'
    if 'music kit' in item_name_lower:
        return '🎵 Музыкальные наборы'
    if 'patch' in item_name_lower:
        return '🧵 Нашивки'
    return '🔫 Оружие'


def add_price_alert_to_db(user_id, item_name, target_price, direction):
    """Добавляет уведомление о цене в базу данных."""
    with get_db_cursor() as (cur, _):
        cur.execute(
            "INSERT INTO price_alerts (user_id, item_name, target_price, direction) VALUES (?, ?, ?, ?)",
            (user_id, item_name, target_price, direction))


def get_price_alerts_by_user(user_id):
    """Получает все активные уведомления о ценах для конкретного пользователя."""
    with get_db_cursor() as (cur, _):
        cur.execute(
            "SELECT id, item_name, target_price, direction FROM price_alerts WHERE user_id = ?",
            (user_id, ))
        return cur.fetchall()


def get_all_price_alerts():
    """Получает все активные уведомления о ценах."""
    with get_db_cursor() as (cur, _):
        cur.execute(
            "SELECT id, user_id, item_name, target_price, direction FROM price_alerts"
        )
        return cur.fetchall()


def delete_price_alert(alert_id):
    """Удаляет уведомление о цене по ID."""
    with get_db_cursor() as (cur, _):
        cur.execute("DELETE FROM price_alerts WHERE id = ?", (alert_id, ))
        return cur.rowcount > 0


def update_price_alert(alert_id, new_price=None, new_direction=None):
    """Обновляет уведомление о цене."""
    with get_db_cursor() as (cur, _):
        if new_price is not None:
            cur.execute(
                "UPDATE price_alerts SET target_price = ? WHERE id = ?",
                (new_price, alert_id))
        if new_direction is not None:
            cur.execute("UPDATE price_alerts SET direction = ? WHERE id = ?",
                        (new_direction, alert_id))


# --- API ПОЛУЧЕНИЯ ЦЕН ---

# --- МУЛЬТИИСТОЧНИКИ ДЛЯ ЦЕН ---
async def fetch_skinport_price(item_name):
    """Получение цены с Skinport API."""
    try:
        # Публичный API Skinport без авторизации
        url = f"https://api.skinport.com/v1/items?app_id=730&currency=USD&tradable=0"
        headers = {'User-Agent': 'CS-Portfolio-Bot/1.0'}
        
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status == 200:
                    data = await response.json()
                    for item in data:
                        if item.get('market_hash_name', '').lower() == item_name.lower():
                            min_price = item.get('min_price')
                            return float(min_price) if min_price else None
                    return None
                else:
                    logging.warning(f"Skinport API error: {response.status}")
                    return None
    except Exception as e:
        logging.warning(f"Ошибка получения цены с Skinport для {item_name}: {e}")
        return None

async def fetch_buff_price(item_name):
    """Симуляция получения цены с BUFF (API требует сложную авторизацию)."""
    try:
        # Публичные данные BUFF через прокси API (если доступно)
        # В реальности потребуется полноценная интеграция с API
        return None  # Временно отключено
    except Exception as e:
        logging.warning(f"Ошибка получения цены с BUFF для {item_name}: {e}")
        return None

async def fetch_csmoney_price(item_name):
    """Получение цены с CS.Money через публичные данные."""
    try:
        # CS.Money имеет ограниченное API, используем fallback
        return None  # Временно отключено - требует специальную интеграцию
    except Exception as e:
        logging.warning(f"Ошибка получения цены с CS.Money для {item_name}: {e}")
        return None

async def fetch_multisource_prices(item_name):
    """Получение цен из всех доступных источников и расчет медианы."""
    try:
        # Получаем цены из всех источников параллельно
        marketcsgo_task = asyncio.create_task(fetch_marketcsgo_single_price(item_name))
        skinport_task = asyncio.create_task(fetch_skinport_price(item_name))
        steam_task = asyncio.create_task(get_steam_price(item_name))
        buff_task = asyncio.create_task(fetch_buff_price(item_name))
        csmoney_task = asyncio.create_task(fetch_csmoney_price(item_name))
        
        tasks = [marketcsgo_task, skinport_task, steam_task, buff_task, csmoney_task]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обрабатываем результаты
        prices = []
        source_prices = {}
        
        # MarketCSGO
        marketcsgo_price = results[0] if not isinstance(results[0], Exception) else None
        if marketcsgo_price:
            prices.append(marketcsgo_price)
            source_prices['marketcsgo'] = marketcsgo_price
        
        # Skinport
        skinport_price = results[1] if not isinstance(results[1], Exception) else None
        if skinport_price:
            prices.append(skinport_price)
            source_prices['skinport'] = skinport_price
        
        # Steam (исключаем из медианы по требованию)
        steam_price = results[2] if not isinstance(results[2], Exception) else None
        if steam_price:
            source_prices['steam'] = steam_price
        
        # BUFF
        buff_price = results[3] if not isinstance(results[3], Exception) else None
        if buff_price:
            prices.append(buff_price)
            source_prices['buff'] = buff_price
        
        # CS.Money
        csmoney_price = results[4] if not isinstance(results[4], Exception) else None
        if csmoney_price:
            prices.append(csmoney_price)
            source_prices['csmoney'] = csmoney_price
        
        # Рассчитываем медиану из доступных цен (кроме Steam)
        median_price = None
        if len(prices) >= 2:
            median_price = statistics.median(prices)
        elif len(prices) == 1:
            median_price = prices[0]
        
        # Сохраняем данные в базу
        timestamp = datetime.now().isoformat()
        with get_db_cursor() as (cur, _):
            for source, price in source_prices.items():
                cur.execute(
                    "INSERT INTO price_history_multisource (item_name, timestamp, source, price_usd, median_price_usd) VALUES (?, ?, ?, ?, ?)",
                    (item_name, timestamp, source, price, median_price)
                )
        
        return {
            'median': median_price,
            'sources': source_prices,
            'steam': steam_price  # Steam отдельно для совместимости
        }
        
    except Exception as e:
        logging.error(f"Ошибка при получении мультиисточников цен для {item_name}: {e}")
        return None

async def fetch_marketcsgo_single_price(item_name):
    """Получение цены одного предмета с MarketCSGO."""
    await fetch_marketcsgo_prices()
    return marketcsgo_prices_cache.get(item_name.lower())

async def fetch_marketcsgo_prices():
    """Асинхронно получает цены с MarketCSGO и кэширует их."""
    global marketcsgo_prices_cache, last_cache_update

    if last_cache_update and datetime.now() - last_cache_update < CACHE_TTL:
        logging.debug("Используется кэш MarketCSGO.")
        return

    url = "https://market.csgo.com/api/v2/prices/USD.json"
    headers = {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    prices = {}
                    if data and data.get("success"):
                        items_data = data.get("items")
                        if isinstance(items_data, dict):
                            prices = {
                                item["market_hash_name"].lower():
                                float(item["price"])
                                for item in items_data.values() if
                                "market_hash_name" in item and "price" in item
                            }
                        elif isinstance(items_data, list):
                            prices = {
                                item["market_hash_name"].lower():
                                float(item["price"])
                                for item in items_data if
                                "market_hash_name" in item and "price" in item
                            }

                    marketcsgo_prices_cache = prices
                    last_cache_update = datetime.now()
                    logging.info(
                        f"Кэш MarketCSGO обновлён. Получено {len(prices)} цен."
                    )
                    return
                logging.error(
                    f"Ошибка при запросе к MarketCSGO: {response.status}")
        except aiohttp.ClientError as e:
            logging.error(f"Ошибка соединения при запросе к MarketCSGO: {e}")


async def get_steam_price(name):
    """Получает цену предмета со Steam Community Market."""
    from urllib.parse import quote
    url = f"https://steamcommunity.com/market/priceoverview/?currency=1&appid=730&market_hash_name={quote(name)}"
    headers = {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data and data.get("success"):
                price_str = data.get("median_price") or data.get(
                    "lowest_price")
                if price_str:
                    cleaned_price = price_str.replace("$",
                                                      "").replace(",",
                                                                  "").strip()
                    try:
                        return float(cleaned_price)
                    except ValueError:
                        logging.error(
                            f"Не удалось преобразовать цену '{price_str}' в число."
                        )
                        return None
        logging.error(f"Ошибка при запросе к Steam: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка соединения при запросу к Steam: {e}")
    return None

# --- ФУНКЦИИ ДЛЯ АНАЛИЗА ПОРТФЕЛЯ ---
def save_portfolio_snapshot():
    """Сохранение снимка портфеля для анализа изменений."""
    items = get_items_from_db()
    timestamp = datetime.now().isoformat()
    
    with get_db_cursor() as (cur, _):
        for item_id, name, qty, buy_uah, buy_usd in items:
            # Пытаемся получить текущую цену из кэша
            if name in multisource_prices_cache:
                price_data = multisource_prices_cache[name]
                current_price = price_data.get('median') or price_data.get('sources', {}).get('marketcsgo')
                
                if current_price:
                    cur.execute(
                        "INSERT INTO portfolio_snapshots (timestamp, item_name, price_usd, quantity) VALUES (?, ?, ?, ?)",
                        (timestamp, name, current_price, qty)
                    )

def get_biggest_price_changes(limit=3):
    """Находит предметы с наибольшими изменениями цен."""
    try:
        with get_db_cursor() as (cur, _):
            # Получаем последние два снимка для каждого предмета
            cur.execute("""
                WITH latest_snapshots AS (
                    SELECT item_name, price_usd, timestamp,
                           ROW_NUMBER() OVER (PARTITION BY item_name ORDER BY timestamp DESC) as rn
                    FROM portfolio_snapshots
                ),
                current_prices AS (
                    SELECT item_name, price_usd as current_price, timestamp as current_time
                    FROM latest_snapshots WHERE rn = 1
                ),
                previous_prices AS (
                    SELECT item_name, price_usd as previous_price, timestamp as previous_time
                    FROM latest_snapshots WHERE rn = 2
                )
                SELECT cp.item_name,
                       cp.current_price,
                       pp.previous_price,
                       ((cp.current_price - pp.previous_price) / pp.previous_price * 100) as change_pct,
                       (cp.current_price - pp.previous_price) as change_abs
                FROM current_prices cp
                JOIN previous_prices pp ON cp.item_name = pp.item_name
                WHERE pp.previous_price > 0
                ORDER BY change_pct DESC
                LIMIT ?
            """, (limit,))
            
            return cur.fetchall()
    except Exception as e:
        logging.error(f"Ошибка при получении изменений цен: {e}")
        return []

def get_top_gainers_and_losers():
    """Получает топ растущих и падающих предметов за последние сутки."""
    try:
        with get_db_cursor() as (cur, _):
            # Получаем изменения за последние 24 часа
            yesterday = (datetime.now() - timedelta(days=1)).isoformat()
            
            cur.execute("""
                WITH price_changes AS (
                    SELECT 
                        ps1.item_name,
                        ps1.price_usd as current_price,
                        ps2.price_usd as old_price,
                        ps1.quantity,
                        ((ps1.price_usd - ps2.price_usd) / ps2.price_usd * 100) as change_pct,
                        (ps1.price_usd - ps2.price_usd) * ps1.quantity as profit_loss_usd
                    FROM portfolio_snapshots ps1
                    JOIN portfolio_snapshots ps2 ON ps1.item_name = ps2.item_name
                    WHERE ps1.timestamp > ps2.timestamp 
                    AND ps2.timestamp >= ?
                    AND ps2.price_usd > 0
                )
                SELECT * FROM price_changes 
                ORDER BY change_pct DESC
            """, (yesterday,))
            
            return cur.fetchall()
    except Exception as e:
        logging.error(f"Ошибка при получении топ изменений: {e}")
        return []


async def get_current_prices_and_steam(item_name):
    """
    Получает текущие цены предмета с MarketCSGO и Steam Market.
    """
    await fetch_marketcsgo_prices()

    marketcsgo_price = marketcsgo_prices_cache.get(item_name.lower())

    steam_price = await get_steam_price(item_name)

    return marketcsgo_price, steam_price


# --- КЛАВИАТУРА ---
def get_main_keyboard():
    """Возвращает клавиатуру с основными кнопками."""
    kb = [[
        KeyboardButton(text="➕ Добавить"),
        KeyboardButton(text="📊 Портфель")
    ], [KeyboardButton(text="🗑️ Удалить"),
        KeyboardButton(text="📈 График")],
          [
              KeyboardButton(text="🔔 Отслеживать цену"),
              KeyboardButton(text="🔔 Мои отслеживания")
          ], [
              KeyboardButton(text="📊 Топ изменений"),
              KeyboardButton(text="🎯 Анализ трендов")
          ], [
              KeyboardButton(text="💹 Прогнозы"),
              KeyboardButton(text="📋 Детальная статистика")
          ], [
              KeyboardButton(text="🔍 Поиск предметов"),
              KeyboardButton(text="📤 Экспорт Excel"),
              KeyboardButton(text="🤖 AI Советы")
          ], [
              KeyboardButton(text="🛒 Массовые операции"),
              KeyboardButton(text="⚡ Краш детектор"),
              KeyboardButton(text="🎮 Steam импорт")
          ], [
              KeyboardButton(text="⚙️ Настройки"),
              KeyboardButton(text="📱 Mini App")
          ]]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


# --- ОБРАБОТЧИКИ СООБЩЕНИЙ ---
@router.message(Command("start"))
async def start_cmd(message: Message):
    """Обработчик команды /start."""
    subscribe_user(message.from_user.id)
    await message.answer(
        "👋 Привет! Я бот для отслеживания твоего CS2 портфеля 💼\n\n"
        "Я буду отправлять тебе уведомления о его изменении. Используй кнопки ниже, чтобы управлять им.",
        reply_markup=get_main_keyboard())


@router.message(Command("subscribe"))
async def subscribe_cmd(message: Message):
    """Обработчик команды /subscribe."""
    subscribe_user(message.from_user.id)
    await message.answer("✅ Ты подписан на уведомления!")


@router.message(F.text == "➕ Добавить")
async def add_item_cmd(message: Message):
    """Обработчик кнопки 'Добавить предмет'."""
    await message.answer(
        "📝 Напиши в формате:\n\n"
        "<code>Название, Количество, Цена_закупки_в_₴</code>\n\n"
        "**Пример:**\n<code>AK-47 | Redline (Field-Tested), 2, 1500</code>")


@router.message(F.text.regexp(r"^.+, *\d+, *\d+(\.\d+)?$"))
async def add_item_parse(message: Message):
    """Парсинг и добавление предмета после ввода."""
    if not message.text:
        return
    try:
        parts = [p.strip() for p in message.text.split(",")]
        if len(parts) != 3:
            await message.answer(
                "❌ Неверный формат ввода. Убедись, что есть 3 части (название, количество, цена)."
            )
            return

        name, qty_str, price_str = parts
        qty = int(qty_str)
        price = float(price_str)

        if qty <= 0 or price <= 0:
            await message.answer("⚠️ Количество и цена должны быть больше 0.")
            return

        add_item_to_db(name, qty, price)
        await message.answer(f"✅ Предмет <b>{name}</b> добавлен в портфель!",
                             reply_markup=get_main_keyboard())
    except (ValueError, IndexError):
        await message.answer(
            "❌ Неверный формат ввода. Используй формат:\n<code>Название, Количество, Цена_в_₴</code>"
        )
    except Exception as e:
        logging.error(f"Ошибка при добавлении предмета: {e}")
        await message.answer("❌ Произошла ошибка. Попробуй еще раз.")


@router.message(F.text == "📊 Портфель")
async def portfolio_cmd(message: Message):
    """Обработчик кнопки 'Портфель'. Начинает отчет с первой страницы, отправляя новое сообщение."""
    # Очищаем кэш, чтобы при следующем нажатии цены обновились
    global portfolio_prices_cache, multisource_prices_cache
    portfolio_prices_cache = {}
    multisource_prices_cache = {}
    await generate_portfolio_report(message, page=0)

@router.message(F.text == "📊 Топ изменений")
async def top_changes_cmd(message: Message):
    """Показывает топ растущих/падающих предметов."""
    try:
        changes = get_top_gainers_and_losers()
        
        if not changes:
            await message.answer("📊 Недостаточно данных для анализа изменений. Подождите накопления истории цен.")
            return
        
        text = "<b>📊 Топ изменений за последние сутки:</b>\n\n"
        
        gainers = [item for item in changes if item[4] > 0][:3]  # change_pct > 0
        losers = [item for item in changes if item[4] < 0][-3:]  # change_pct < 0
        
        if gainers:
            text += "<b>🟢 Лидеры роста:</b>\n"
            for item_name, current, old, qty, change_pct, profit in gainers:
                profit_uah = profit * USD_TO_UAH
                text += f"• <b>{item_name}</b>\n"
                text += f"  📈 {change_pct:+.2f}% | Прибыль: {profit_uah:,.0f}₴\n"
                text += f"  💰 {old:.2f}$ → {current:.2f}$ (x{qty})\n\n"
        
        if losers:
            text += "<b>🔴 Лидеры падения:</b>\n"
            for item_name, current, old, qty, change_pct, profit in losers:
                profit_uah = profit * USD_TO_UAH
                text += f"• <b>{item_name}</b>\n"
                text += f"  📉 {change_pct:+.2f}% | Убыток: {profit_uah:,.0f}₴\n"
                text += f"  💰 {old:.2f}$ → {current:.2f}$ (x{qty})\n\n"
        
        await message.answer(text, reply_markup=get_main_keyboard())
        
    except Exception as e:
        logging.error(f"Ошибка в топ изменениях: {e}")
        await message.answer("❌ Ошибка при получении данных об изменениях.")

@router.message(F.text == "🎯 Анализ трендов")  
async def trend_analysis_cmd(message: Message):
    """Анализ трендов портфеля."""
    try:
        # Получаем историю портфеля за последние 30 дней
        with get_db_cursor() as (cur, _):
            thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
            cur.execute(
                "SELECT timestamp, value_uah FROM portfolio_history WHERE timestamp >= ? ORDER BY timestamp",
                (thirty_days_ago,)
            )
            history = cur.fetchall()
        
        if len(history) < 2:
            await message.answer("📈 Недостаточно данных для анализа трендов. Нужна история минимум за 2 дня.")
            return
        
        # Анализируем тренд
        values = [float(row[1]) for row in history]
        dates = [datetime.fromisoformat(row[0]) for row in history]
        
        # Простой анализ тренда
        first_week_avg = sum(values[:7]) / len(values[:7]) if len(values) >= 7 else values[0]
        last_week_avg = sum(values[-7:]) / len(values[-7:]) if len(values) >= 7 else values[-1]
        
        trend_pct = ((last_week_avg - first_week_avg) / first_week_avg) * 100 if first_week_avg > 0 else 0
        trend_emoji = "📈" if trend_pct > 0 else "📉" if trend_pct < 0 else "➡️"
        
        # Максимум и минимум
        max_value = max(values)
        min_value = min(values)
        current_value = values[-1]
        
        volatility = ((max_value - min_value) / min_value) * 100 if min_value > 0 else 0
        
        text = f"<b>🎯 Анализ трендов портфеля (30 дней):</b>\n\n"
        text += f"<b>Общий тренд:</b> {trend_emoji} {trend_pct:+.2f}%\n"
        text += f"<b>Текущая стоимость:</b> {current_value:,.0f}₴\n"
        text += f"<b>Максимум:</b> {max_value:,.0f}₴\n"
        text += f"<b>Минимум:</b> {min_value:,.0f}₴\n"
        text += f"<b>Волатильность:</b> {volatility:.1f}%\n\n"
        
        # Прогноз
        if trend_pct > 5:
            text += "🟢 <b>Прогноз:</b> Портфель показывает уверенный рост"
        elif trend_pct > 0:
            text += "🟡 <b>Прогноз:</b> Портфель растет умеренными темпами"
        elif trend_pct > -5:
            text += "🟡 <b>Прогноз:</b> Портфель в стагнации"
        else:
            text += "🔴 <b>Прогноз:</b> Портфель показывает негативную динамику"
        
        await message.answer(text, reply_markup=get_main_keyboard())
        
    except Exception as e:
        logging.error(f"Ошибка в анализе трендов: {e}")
        await message.answer("❌ Ошибка при анализе трендов.")

@router.message(F.text == "💹 Прогнозы")
async def predictions_cmd(message: Message):
    """Прогнозы и рекомендации."""
    try:
        # Простой анализ для прогнозов
        items = get_items_from_db()
        if not items:
            await message.answer("❌ Портфель пуст.")
            return
        
        text = "<b>💹 Прогнозы и рекомендации:</b>\n\n"
        text += "<b>🎯 На основе анализа рынка:</b>\n"
        text += "• Ножи: Стабильный рост 📈\n"
        text += "• Перчатки: Высокий потенциал 🚀\n"
        text += "• Кейсы: Осторожность ⚠️\n"
        text += "• AK-47/M4A4 скины: Рекомендуется к покупке ✅\n\n"
        
        text += "<b>📊 Рекомендации по портфелю:</b>\n"
        # Анализируем категории в портфеле
        category_analysis = {}
        for _, name, qty, buy_uah, buy_usd in items:
            category = get_item_category(name)
            if category not in category_analysis:
                category_analysis[category] = 0
            category_analysis[category] += buy_uah * qty
        
        total_value = sum(category_analysis.values())
        
        for category, value in category_analysis.items():
            percentage = (value / total_value) * 100 if total_value > 0 else 0
            if percentage > 40:
                text += f"⚠️ Высокая концентрация в {category} ({percentage:.1f}%)\n"
            elif percentage > 60:
                text += f"🔴 Критическая концентрация в {category} ({percentage:.1f}%)\n"
        
        text += "\n<b>💡 Общие советы:</b>\n"
        text += "• Диверсифицируйте портфель по категориям\n"
        text += "• Следите за обновлениями игры\n"
        text += "• Устанавливайте стоп-лоссы\n"
        text += "• Покупайте на падениях рынка"
        
        await message.answer(text, reply_markup=get_main_keyboard())
        
    except Exception as e:
        logging.error(f"Ошибка в прогнозах: {e}")
        await message.answer("❌ Ошибка при формировании прогнозов.")

@router.message(F.text == "📋 Детальная статистика")
async def detailed_stats_cmd(message: Message):
    """Детальная статистика портфеля."""
    try:
        items = get_items_from_db()
        if not items:
            await message.answer("❌ Портфель пуст.")
            return
        
        # Детальный анализ
        total_items = len(items)
        total_quantity = sum(item[2] for item in items)
        total_buy_value = sum(item[3] * item[2] for item in items)
        
        # Анализ по датам добавления
        with get_db_cursor() as (cur, _):
            cur.execute("SELECT added_at FROM items ORDER BY added_at")
            dates = [datetime.fromisoformat(row[0]) for row in cur.fetchall()]
        
        if dates:
            days_active = (datetime.now() - min(dates)).days
            avg_items_per_week = (total_items / max(days_active / 7, 1))
        else:
            days_active = 0
            avg_items_per_week = 0
        
        text = f"<b>📋 Детальная статистика портфеля:</b>\n\n"
        text += f"<b>📊 Основные показатели:</b>\n"
        text += f"• Уникальных предметов: {total_items}\n"
        text += f"• Общее количество: {total_quantity}\n"
        text += f"• Инвестировано: {total_buy_value:,.0f}₴\n"
        text += f"• Дней активности: {days_active}\n"
        text += f"• Среднее пополнений/неделю: {avg_items_per_week:.1f}\n\n"
        
        # Топ дорогих позиций
        expensive_items = sorted(items, key=lambda x: x[3] * x[2], reverse=True)[:3]
        text += f"<b>💎 Самые дорогие позиции:</b>\n"
        for i, (_, name, qty, buy_uah, _) in enumerate(expensive_items, 1):
            total_pos = buy_uah * qty
            text += f"{i}. {name}: {total_pos:,.0f}₴\n"
        
        text += f"\n<b>🏷️ Анализ по категориям:</b>\n"
        category_stats = {}
        for _, name, qty, buy_uah, _ in items:
            category = get_item_category(name)
            if category not in category_stats:
                category_stats[category] = {'count': 0, 'value': 0}
            category_stats[category]['count'] += qty
            category_stats[category]['value'] += buy_uah * qty
        
        for category, stats in sorted(category_stats.items(), key=lambda x: x[1]['value'], reverse=True):
            percentage = (stats['value'] / total_buy_value) * 100 if total_buy_value > 0 else 0
            text += f"• {category}: {stats['count']} шт. ({percentage:.1f}%)\n"
        
        await message.answer(text, reply_markup=get_main_keyboard())
        
    except Exception as e:
        logging.error(f"Ошибка в детальной статистике: {e}")
        await message.answer("❌ Ошибка при формировании статистики.")

@router.message(Command("cancel"))
async def cancel_handler(message: Message, state: FSMContext):
    """Отмена текущего состояния."""
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("❌ Нет активных операций для отмены.")
        return
    
    await state.clear()
    await message.answer("✅ Операция отменена.", reply_markup=get_main_keyboard())

@router.message(F.text == "🔍 Поиск предметов")
async def search_items_cmd(message: Message):
    """Поиск предметов по названию."""
    await message.answer(
        "🔍 <b>Поиск предметов в портфеле</b>\n\n"
        "Отправь название предмета или его часть для поиска.\n"
        "Например: <code>AK-47</code> или <code>Knife</code>",
        reply_markup=get_main_keyboard()
    )

# Обработчик поиска предметов
@router.message(F.text.regexp(r"^[A-Za-z0-9\s\-\|()]+$"))
async def search_handler(message: Message):
    """Обработчик поиска предметов в портфеле."""
    if not message.text:
        return
    query = message.text.strip().lower()
    
    # Игнорируем команды-кнопки
    if query in ["➕ добавить", "📊 портфель", "🗑️ удалить", "📈 график", 
                 "🔔 отслеживать цену", "🔔 мои отслеживания", "📊 топ изменений",
                 "🎯 анализ трендов", "💹 прогнозы", "📋 детальная статистика",
                 "🔍 поиск предметов", "⚙️ настройки"]:
        return
    
    items = get_items_from_db()
    if not items:
        await message.answer("❌ Портфель пуст.")
        return
    
    # Поиск совпадений
    found_items = []
    for item_id, name, qty, buy_uah, buy_usd in items:
        if query in name.lower():
            found_items.append((item_id, name, qty, buy_uah, buy_usd))
    
    if not found_items:
        await message.answer(f"🔍 По запросу '<b>{message.text}</b>' ничего не найдено.")
        return
    
    # Формируем ответ
    text = f"🔍 <b>Найдено по запросу '{message.text}':</b>\n\n"
    
    for item_id, name, qty, buy_uah, buy_usd in found_items[:10]:  # Максимум 10 результатов
        total_buy = buy_uah * qty
        text += f"• <b>{name}</b>\n"
        text += f"  📦 Количество: {qty}\n"
        text += f"  💰 Общая закупка: {total_buy:,.0f}₴\n\n"
    
    if len(found_items) > 10:
        text += f"... и еще {len(found_items) - 10} предметов"
    
    await message.answer(text, reply_markup=get_main_keyboard())

# Новые callback handlers
@router.callback_query(lambda c: (c.data or "").startswith("refresh_portfolio_"))
async def refresh_portfolio_callback(callback: CallbackQuery):
    """Обновление портфеля."""
    if not callback.data:
        return
    page = int(callback.data.split("_")[-1])
    global portfolio_prices_cache, multisource_prices_cache
    portfolio_prices_cache = {}
    multisource_prices_cache = {}
    
    await safe_edit_or_send(callback, text="⏳ Обновляю портфель...")
    await generate_portfolio_report(callback.message, page=page)
    await callback.answer("✅ Портфель обновлён!")

@router.callback_query(lambda c: c.data == "show_chart")
async def show_chart_callback(callback: CallbackQuery):
    """Показать график портфеля."""
    try:
        chart_buffer = await generate_portfolio_chart()
        if chart_buffer:
            chart_file = BufferedInputFile(chart_buffer.getvalue(), filename="portfolio_chart.png")
            await safe_edit_or_send(callback, photo=chart_file)
        else:
            await callback.answer("❌ Недостаточно данных для построения графика")
    except Exception as e:
        logging.error(f"Ошибка при показе графика: {e}")
        await callback.answer("❌ Ошибка при построении графика")


# ===== НОВЫЕ ФУНКЦИИ =====

@router.message(F.text == "📤 Экспорт Excel")
async def export_excel_cmd(message: Message):
    """Экспорт портфеля в Excel файл."""
    await message.answer("⏳ Создаю Excel файл с твоим портфелем...")
    
    items = get_items_from_db()
    if not items:
        await message.answer("❌ Портфель пуст. Нечего экспортировать.")
        return
    
    try:
        import pandas as pd
        from datetime import datetime
        import io
        
        # Создаем данные для экспорта
        data = []
        total_buy_uah = 0
        total_now_uah = 0
        
        for user_id, name, qty, buy_uah, buy_usd in items:
            # Получаем текущие цены
            result = await get_current_prices_and_steam(name)
            current_price_usd, steam_price_usd = result
            current_price_uah = current_price_usd * USD_TO_UAH if current_price_usd else buy_uah
            
            pos_buy_uah = buy_uah * qty
            pos_now_uah = current_price_uah * qty
            profit_uah = pos_now_uah - pos_buy_uah
            profit_pct = (profit_uah / pos_buy_uah) * 100 if pos_buy_uah > 0 else 0
            
            total_buy_uah += pos_buy_uah
            total_now_uah += pos_now_uah
            
            data.append({
                'Предмет': name,
                'Количество': qty,
                'Цена покупки (₴)': buy_uah,
                'Цена покупки ($)': buy_usd,
                'Текущая цена (₴)': round(current_price_uah, 2),
                'Текущая цена ($)': round(current_price_usd, 2) if current_price_usd else 0,
                'Steam цена ($)': round(steam_price_usd, 2) if steam_price_usd else 0,
                'Инвестировано (₴)': pos_buy_uah,
                'Текущая стоимость (₴)': round(pos_now_uah, 2),
                'Прибыль/Убыток (₴)': round(profit_uah, 2),
                'Прибыль/Убыток (%)': round(profit_pct, 2),
                'Категория': get_item_category(name),
                'Дата экспорта': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # Создаем DataFrame
        df = pd.DataFrame(data)
        
        # Создаем Excel файл в памяти
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
            # Основной лист с портфелем
            df.to_excel(writer, sheet_name='Портфель', index=False)
            
            # Лист со статистикой
            stats_data = [{
                'Показатель': 'Общая сумма инвестиций',
                'Значение (₴)': total_buy_uah,
                'Значение ($)': round(total_buy_uah / USD_TO_UAH, 2)
            }, {
                'Показатель': 'Текущая стоимость',
                'Значение (₴)': round(total_now_uah, 2),
                'Значение ($)': round(total_now_uah / USD_TO_UAH, 2)
            }, {
                'Показатель': 'Общая прибыль/убыток',
                'Значение (₴)': round(total_now_uah - total_buy_uah, 2),
                'Значение ($)': round((total_now_uah - total_buy_uah) / USD_TO_UAH, 2)
            }]
            
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='Статистика', index=False)
        
        excel_buffer.seek(0)
        
        # Отправляем файл пользователю
        await message.answer_document(
            document=BufferedInputFile(
                excel_buffer.getvalue(),
                filename=f"portfolio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            ),
            caption=f"📊 <b>Экспорт портфеля завершен!</b>\n\n"
                   f"📈 Общая стоимость: {total_now_uah:,.0f}₴\n"
                   f"💰 Инвестировано: {total_buy_uah:,.0f}₴\n"
                   f"💹 Прибыль: {total_now_uah - total_buy_uah:+,.0f}₴"
        )
        
    except Exception as e:
        await message.answer(f"❌ Ошибка при создании Excel: {str(e)}")

@router.message(F.text == "🤖 AI Советы")
async def ai_advice_cmd(message: Message):
    """AI анализ портфеля с рекомендациями.""" 
    await message.answer("🧠 Анализирую твой портфель с помощью AI...")
    
    items = get_items_from_db()
    if not items:
        await message.answer("❌ Портфель пуст. AI нечего анализировать.")
        return
    
    try:
        import os
        from openai import OpenAI
        
        # Проверяем API ключ
        if not os.environ.get("OPENAI_API_KEY"):
            await message.answer("❌ OpenAI API ключ не настроен. Обратись к администратору.")
            return
        
        # Using GPT-4 as the most reliable model for production use
        client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        
        # Собираем данные портфеля для анализа
        portfolio_data = []
        total_value = 0
        
        for user_id, name, qty, buy_uah, buy_usd in items[:10]:  # Ограничиваем для экономии токенов
            result = await get_current_prices_and_steam(name)
            current_price_usd, steam_price_usd = result
            
            if current_price_usd:
                current_value = current_price_usd * USD_TO_UAH * qty
                profit_pct = ((current_price_usd * USD_TO_UAH - buy_uah) / buy_uah) * 100
                total_value += current_value
                
                portfolio_data.append({
                    'name': name,
                    'quantity': qty,
                    'buy_price_uah': buy_uah,
                    'current_price_usd': current_price_usd,
                    'profit_percent': round(profit_pct, 2),
                    'category': get_item_category(name),
                    'value_uah': round(current_value, 2)
                })
        
        # Создаем промпт для AI
        prompt = f"""Проанализируй CS:GO/CS2 портфель игрока и дай профессиональные рекомендации.

ДАННЫЕ ПОРТФЕЛЯ:
{portfolio_data}

Общая стоимость портфеля: {total_value:,.0f} ₴

ЗАДАЧИ:
1. Оцени диверсификацию портфеля
2. Определи самые рисковые позиции
3. Найди возможности для улучшения
4. Дай конкретные рекомендации по покупке/продаже
5. Оцени общую стратегию инвестиций

Отвечай на русском языке, будь конкретным и профессиональным."""

        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Ты профессиональный аналитик рынка CS:GO скинов с 10+ летним опытом торговли. Даешь конкретные, практичные советы."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1000,
            temperature=0.7
        )
        
        ai_advice = response.choices[0].message.content
        
        # Форматируем и отправляем ответ
        final_message = f"🤖 <b>AI Анализ твоего портфеля</b>\n\n{ai_advice}\n\n"
        final_message += f"💡 <i>Анализ выполнен GPT-4 на основе {len(portfolio_data)} предметов</i>"
        
        await message.answer(final_message, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка AI анализа: {str(e)}")

@router.message(F.text == "⚡ Краш детектор")
async def crash_detector_cmd(message: Message):
    """Детектор рыночных крашей и возможностей."""
    await message.answer("⚡ Анализирую рынок на предмет крашей и возможностей...")
    
    try:
        # Получаем данные о ценах за последние 24 часа
        current_time = datetime.now()
        yesterday = current_time - timedelta(days=1)
        
        with get_db_cursor() as (cur, _):
            # Находим предметы с большими изменениями цен
            cur.execute('''
                SELECT DISTINCT item_name, 
                       AVG(CASE WHEN timestamp > ? THEN median_price_usd END) as current_avg,
                       AVG(CASE WHEN timestamp <= ? THEN median_price_usd END) as prev_avg
                FROM price_history_multisource 
                WHERE timestamp > ? AND median_price_usd > 0
                GROUP BY item_name 
                HAVING current_avg IS NOT NULL AND prev_avg IS NOT NULL
                ORDER BY (current_avg - prev_avg) / prev_avg
            ''', (yesterday.isoformat(), yesterday.isoformat(), (current_time - timedelta(days=2)).isoformat()))
            
            results = cur.fetchall()
        
        if not results:
            await message.answer("📊 Недостаточно данных для анализа крашей.")
            return
        
        # Анализируем результаты
        crashes = []  # Сильные падения
        pumps = []    # Сильный рост
        
        for item_name, current_avg, prev_avg in results:
            if current_avg and prev_avg:
                change_pct = ((current_avg - prev_avg) / prev_avg) * 100
                
                if change_pct <= -15:  # Падение больше 15%
                    crashes.append((item_name, change_pct, current_avg))
                elif change_pct >= 20:  # Рост больше 20%
                    pumps.append((item_name, change_pct, current_avg))
        
        # Формируем отчет
        report = "⚡ <b>Детектор рыночных изменений</b>\n\n"
        
        if crashes:
            report += "📉 <b>КРАШЫ (возможности для покупки):</b>\n"
            for item, change, price in crashes[:5]:
                report += f"• {item[:40]}\n  💸 {change:+.1f}% (${price:.2f})\n\n"
        
        if pumps:
            report += "📈 <b>ПАМПЫ (возможности для продажи):</b>\n"
            for item, change, price in pumps[:5]:
                report += f"• {item[:40]}\n  🚀 {change:+.1f}% (${price:.2f})\n\n"
        
        if not crashes and not pumps:
            report += "📊 Рынок стабилен. Значительных изменений не обнаружено."
        
        report += f"🕐 Анализ за последние 24 часа"
        
        await message.answer(report)
        
    except Exception as e:
        await message.answer(f"❌ Ошибка анализа рынка: {str(e)}")

@router.message(F.text == "📱 Mini App")
async def mini_app_cmd(message: Message):
    """Запуск Telegram Mini App."""
    # Создаем веб-приложение кнопку
    webapp = WebAppInfo(url=f"https://{os.environ.get('REPLIT_DEV_DOMAIN', 'localhost:5000')}/webapp")
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Открыть Mini App", web_app=webapp)]
    ])
    
    await message.answer(
        "📱 <b>Telegram Mini App</b>\n\n"
        "🎯 <b>Расширенные возможности:</b>\n"
        "• Интерактивные графики\n"
        "• Детальная аналитика\n"
        "• Сравнение портфелей\n"
        "• Расширенные фильтры\n"
        "• Экспорт в разных форматах\n\n"
        "👆 Нажми кнопку ниже для запуска:",
        reply_markup=kb
    )

# ===== ОБРАБОТЧИКИ CALLBACK ДЛЯ МАССОВЫХ ОПЕРАЦИЙ =====

@router.callback_query(lambda c: c.data == "bulk_add")
async def bulk_add_callback(callback: CallbackQuery, state: FSMContext):
    """Массовое добавление предметов."""
    await state.set_state(BulkStates.waiting_for_bulk_add)
    await callback.message.edit_text(
        "🛒 <b>Массовое добавление</b>\n\n"
        "📝 Отправь список предметов в формате:\n"
        "<code>Название предмета | Количество | Цена в ₴</code>\n\n"
        "📋 <b>Пример:</b>\n"
        "<code>AK-47 Redline (Field-Tested) | 1 | 2500\n"
        "AWP Dragon Lore (Factory New) | 1 | 45000</code>\n\n"
        "⚠️ <i>По одному предмету на строку</i>\n"
        "🚫 Отправь /cancel для отмены",
        parse_mode="HTML"
    )

@router.message(StateFilter(BulkStates.waiting_for_bulk_add))
async def process_bulk_add(message: Message, state: FSMContext):
    """Обработка массового добавления предметов."""
    try:
        lines = message.text.strip().split('\n')
        added_items = []
        errors = []
        
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
                
            try:
                parts = [p.strip() for p in line.split('|')]
                if len(parts) != 3:
                    errors.append(f"Строка {line_num}: Неверный формат")
                    continue
                
                item_name, quantity_str, price_str = parts
                quantity = int(quantity_str)
                price_uah = float(price_str)
                price_usd = price_uah / USD_TO_UAH
                
                if quantity <= 0 or price_uah <= 0:
                    errors.append(f"Строка {line_num}: Неверные числовые значения")
                    continue
                
                # Добавляем в базу
                with get_db_cursor() as (cur, conn):
                    cur.execute(
                        "INSERT INTO portfolio (user_id, item_name, quantity, buy_price_uah, buy_price_usd) VALUES (?, ?, ?, ?, ?)",
                        (message.from_user.id, item_name, quantity, price_uah, price_usd)
                    )
                
                added_items.append(f"• {item_name} x{quantity} ({price_uah:,.0f}₴)")
                
            except ValueError:
                errors.append(f"Строка {line_num}: Ошибка в числах")
            except Exception as e:
                errors.append(f"Строка {line_num}: {str(e)}")
        
        # Очищаем кэш
        global portfolio_prices_cache, multisource_prices_cache
        portfolio_prices_cache = {}
        multisource_prices_cache = {}
        
        # Формируем ответ
        result = "🛒 <b>Результат массового добавления</b>\n\n"
        
        if added_items:
            result += f"✅ <b>Добавлено {len(added_items)} предметов:</b>\n"
            result += '\n'.join(added_items[:10])  # Показываем первые 10
            if len(added_items) > 10:
                result += f"\n... и еще {len(added_items) - 10}"
        
        if errors:
            result += f"\n\n❌ <b>Ошибки ({len(errors)}):</b>\n"
            result += '\n'.join(errors[:5])  # Показываем первые 5 ошибок
            if len(errors) > 5:
                result += f"\n... и еще {len(errors) - 5}"
        
        await message.answer(result, parse_mode="HTML")
        await state.clear()
        
    except Exception as e:
        await message.answer(f"❌ Ошибка обработки: {str(e)}")
        await state.clear()

@router.callback_query(lambda c: c.data == "bulk_remove")
async def bulk_remove_callback(callback: CallbackQuery):
    """Массовое удаление предметов."""
    await callback.message.edit_text(
        "🗑 <b>Массовое удаление</b>\n\n"
        "📝 Отправь список предметов для удаления:\n"
        "<code>Название предмета</code>\n\n"
        "📋 <b>Пример:</b>\n"
        "<code>AK-47 Redline (Field-Tested)\n"
        "AWP Dragon Lore (Factory New)</code>\n\n"
        "⚠️ <i>По одному предмету на строку</i>",
        parse_mode="HTML"
    )

@router.callback_query(lambda c: c.data == "bulk_update_prices")
async def bulk_update_prices_callback(callback: CallbackQuery):
    """Принудительное обновление всех цен в портфеле."""
    await callback.message.edit_text("⏳ Обновляю все цены в портфеле...")
    
    try:
        global portfolio_prices_cache, multisource_prices_cache
        portfolio_prices_cache = {}
        multisource_prices_cache = {}
        
        items = get_items_from_db()
        if not items:
            await callback.message.edit_text("❌ Портфель пуст.")
            return
        
        updated_count = 0
        for user_id, name, qty, buy_uah, buy_usd in items:
            try:
                # Принудительно обновляем цены для каждого предмета
                result = await get_current_prices_and_steam(name)
                if result and result[0]:  # Если получили цену
                    updated_count += 1
            except:
                continue
        
        await callback.message.edit_text(
            f"✅ <b>Цены обновлены!</b>\n\n"
            f"📊 Обновлено предметов: <b>{updated_count}</b> из {len(items)}\n"
            f"🕐 Время обновления: {datetime.now().strftime('%H:%M:%S')}"
        )
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка обновления цен: {str(e)}")

@router.callback_query(lambda c: c.data == "bulk_clear")
async def bulk_clear_callback(callback: CallbackQuery):
    """Подтверждение очистки портфеля."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, очистить", callback_data="confirm_clear"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_clear")
        ]
    ])
    
    await callback.message.edit_text(
        "⚠️ <b>ВНИМАНИЕ!</b>\n\n"
        "Ты хочешь полностью очистить портфель?\n"
        "Все данные будут удалены без возможности восстановления!\n\n"
        "🤔 Уверен?",
        reply_markup=kb
    )

@router.callback_query(lambda c: c.data == "confirm_clear")
async def confirm_clear_callback(callback: CallbackQuery):
    """Подтвержденная очистка портфеля."""
    try:
        with get_db_cursor() as (cur, conn):
            cur.execute("DELETE FROM portfolio WHERE user_id = ?", (callback.from_user.id,))
            deleted_count = cur.rowcount
        
        global portfolio_prices_cache, multisource_prices_cache
        portfolio_prices_cache = {}
        multisource_prices_cache = {}
        
        await callback.message.edit_text(
            f"🗑 <b>Портфель очищен!</b>\n\n"
            f"❌ Удалено предметов: <b>{deleted_count}</b>\n"
            f"✨ Теперь можешь начать заново!"
        )
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка очистки портфеля: {str(e)}")

@router.callback_query(lambda c: c.data == "cancel_clear")
async def cancel_clear_callback(callback: CallbackQuery):
    """Отмена очистки портфеля."""
    await callback.message.edit_text(
        "✅ <b>Отменено!</b>\n\n"
        "🛡 Твой портфель в безопасности."
    )

@router.message(F.text == "⚙️ Настройки уведомлений")
async def notification_settings_cmd(message: Message):
    """Обработчик кнопки 'Настройки уведомлений'."""
    user_id = message.from_user.id
    threshold, check_items, check_portfolio, last_prices = get_user_notification_settings(
        user_id)

    # Получаем количество предметов в списке отслеживания
    watchlist = get_user_watchlist(user_id)
    watchlist_count = len(watchlist)

    text = (f"⚙️ <b>Настройки уведомлений</b>\n\n"
            f"🎯 Порог изменения: <b>{threshold}%</b>\n"
            f"💼 Отслеживание портфеля: {'✅' if check_items else '❌'}\n"
            f"📊 Общие уведомления: {'✅' if check_portfolio else '❌'}\n"
            f"👁️ Предметов в отслеживании: <b>{watchlist_count}</b>\n\n"
            f"📋 Выбери что хочешь настроить:")

    kb = [[
        InlineKeyboardButton(text="🎯 Изменить порог (%)",
                             callback_data="set_threshold")
    ],
          [
              InlineKeyboardButton(
                  text="💼 Отслеживание портфеля",
                  callback_data=f"toggle_items_{not check_items}")
          ],
          [
              InlineKeyboardButton(
                  text="📊 Общие уведомления",
                  callback_data=f"toggle_portfolio_{not check_portfolio}")
          ],
          [
              InlineKeyboardButton(text="👁️ Управление списком",
                                   callback_data="manage_watchlist")
          ],
          [
              InlineKeyboardButton(text="🔄 Обновить",
                                   callback_data="refresh_settings")
          ]]
    markup = InlineKeyboardMarkup(inline_keyboard=kb)

    await message.answer(text, reply_markup=markup)


@router.callback_query(lambda c: (c.data or "").startswith("page_"))
async def change_page(callback: CallbackQuery):
    """Обработчик для кнопок пагинации отчета. Редактирует текущее сообщение."""
    if not callback.data:
        return
    page = int(callback.data.split("_")[1])
    await generate_portfolio_report(callback.message, page=page)
    await callback.answer()


async def generate_portfolio_report_enhanced(message: Message, page: int):
    """Улучшенная генерация отчета по портфелю с мультиисточниками и анализом роста."""
    global portfolio_prices_cache, multisource_prices_cache
    
    items = get_items_from_db()
    if not items:
        await message.answer(
            "❌ Портфель пуст. Добавь предметы, используя кнопку '➕ Добавить'.")
        return
    
    # Прогресс бар для пользователя
    progress_msg = await message.answer("⏳ Обновляю цены из мультиисточников...")
    
    # Если кэш пуст, делаем запросы к мультиисточникам
    if not multisource_prices_cache:
        await fetch_marketcsgo_prices()
        
        # Получаем цены из мультиисточников для всех предметов
        for _, name, qty, buy_uah, buy_usd in items:
            try:
                multisource_data = await fetch_multisource_prices(name)
                if multisource_data:
                    multisource_prices_cache[name] = multisource_data
                else:
                    # Fallback к старому методу
                    marketcsgo_price, steam_price = await get_current_prices_and_steam(name)
                    multisource_prices_cache[name] = {
                        'median': marketcsgo_price,
                        'sources': {'marketcsgo': marketcsgo_price},
                        'steam': steam_price
                    }
            except Exception as e:
                logging.error(f"Ошибка получения цен для {name}: {e}")
                # Fallback
                multisource_prices_cache[name] = {
                    'median': None,
                    'sources': {},
                    'steam': None
                }
    
    await progress_msg.edit_text("📊 Анализирую изменения цен...")
    
    # Сохраняем снимок портфеля для анализа
    save_portfolio_snapshot()
    
    # Находим предмет с наибольшим ростом
    biggest_changes = get_biggest_price_changes(3)
    biggest_gainer = None
    if biggest_changes:
        biggest_gainer = biggest_changes[0]  # Первый элемент - максимальный рост
    
    await progress_msg.edit_text("📈 Формирую отчет...")
    
    # Считаем общую статистику по всему портфелю
    total_buy_uah = 0
    total_now_uah = 0.0
    category_stats = {}
    multisource_count = 0
    
    for _, name, qty, buy_uah, buy_usd in items:
        pos_buy_uah = buy_uah * qty
        total_buy_uah += pos_buy_uah
        
        price_data = multisource_prices_cache.get(name, {})
        median_price = price_data.get('median')
        sources_count = len(price_data.get('sources', {}))
        
        if median_price:
            current_price_uah = median_price * USD_TO_UAH
            total_now_uah += current_price_uah * qty
            if sources_count > 1:
                multisource_count += 1
        else:
            total_now_uah += pos_buy_uah
        
        category = get_item_category(name)
        if category not in category_stats:
            category_stats[category] = {'buy_uah': 0, 'now_uah': 0}
        
        category_stats[category]['buy_uah'] += pos_buy_uah
        category_stats[category]['now_uah'] += (
            median_price * USD_TO_UAH * qty if median_price else pos_buy_uah
        )
    
    # Формирование итогового отчета (сводная часть)
    total_profit_uah = total_now_uah - total_buy_uah
    total_profit_usd = total_profit_uah / USD_TO_UAH
    total_profit_pct = (total_profit_uah / total_buy_uah) * 100 if total_buy_uah > 0 else 0
    total_buy_usd = total_buy_uah / USD_TO_UAH
    total_now_usd = total_now_uah / USD_TO_UAH
    
    total_profit_emoji = "🟢" if total_profit_uah >= 0 else "🔴"
    
    final_report = (
        f"<b>📊 Твой портфель (мульти-источники):</b>\n"
        f"<b>💰 Общая закупка:</b> {total_buy_uah:,.2f}₴ | {total_buy_usd:,.2f}$\n"
        f"<b>📈 Текущая стоимость:</b> {total_now_uah:,.2f}₴ | {total_now_usd:,.2f}$\n"
        f"<b>🚀 Общий профит:</b> {total_profit_emoji} {total_profit_uah:,.2f}₴ ({total_profit_usd:,.2f}$) ({total_profit_pct:+.2f}%)\n"
        f"<b>📡 Источников данных:</b> {multisource_count} предметов с множественными источниками\n"
    )
    
    # Показываем лидера роста если есть
    if biggest_gainer:
        gainer_name, current_price, prev_price, change_pct, change_abs = biggest_gainer
        change_abs_uah = change_abs * USD_TO_UAH
        final_report += f"\n<b>🏆 Лидер роста:</b>\n"
        final_report += f"📈 <b>{gainer_name}</b> {change_pct:+.2f}% (+{change_abs_uah:,.0f}₴)\n"
    
    final_report += "\n--- <b>Статистика по категориям</b> ---\n"
    for category, values in category_stats.items():
        cat_profit = values['now_uah'] - values['buy_uah']
        cat_profit_pct = (cat_profit / values['buy_uah']) * 100 if values['buy_uah'] > 0 else 0
        cat_profit_emoji = "🟢" if cat_profit >= 0 else "🔴"
        final_report += f"{category}: {values['now_uah']:,.2f}₴ ({cat_profit_emoji} {cat_profit_pct:+.2f}%)\n"
    
    final_report += "\n--- <b>Детализация по предметам</b> ---\n"
    
    # Пагинация
    total_pages = (len(items) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    start_index = page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE
    items_on_page = items[start_index:end_index]
    
    item_reports = []
    for _, name, qty, buy_uah, buy_usd in items_on_page:
        report_lines = []
        report_lines.append(f"<b>{name}</b>")
        report_lines.append(f"📦 Количество: {qty}")
        
        price_data = multisource_prices_cache.get(name, {})
        if not price_data:
            report_lines.append("❌ Нет данных о ценах. Попробуйте обновить портфель.")
            item_reports.append("\n".join(report_lines))
            continue
        
        median_price = price_data.get('median')
        sources = price_data.get('sources', {})
        steam_price = price_data.get('steam')
        
        # Общая стоимость закупки
        buy_total_uah = buy_uah * qty
        buy_total_usd = buy_usd * qty
        report_lines.append(f"💰 Общая закупка: {buy_total_uah:,.2f}₴ | {buy_total_usd:,.2f}$")
        
        # Текущая общая стоимость
        if median_price:
            current_total_uah = (median_price * USD_TO_UAH) * qty
            current_total_usd = median_price * qty
            report_lines.append(f"📈 Текущая стоимость: {current_total_uah:,.2f}₴ | {current_total_usd:,.2f}$")
            
            # Профит
            profit_uah = current_total_uah - buy_total_uah
            profit_usd = current_total_usd - buy_total_usd
            profit_pct = (profit_uah / buy_total_uah) * 100 if buy_total_uah > 0 else 0
            profit_emoji = "🟢" if profit_uah >= 0 else "🔴"
            
            report_lines.append(f"🚀 Профит: {profit_emoji} {profit_uah:,.2f}₴ ({profit_usd:,.2f}$) ({profit_pct:+.2f}%)")
            
            # Показываем источники
            if len(sources) > 1:
                sources_text = ", ".join([f"{src}: {price:.2f}$" for src, price in sources.items()])
                report_lines.append(f"📡 Источники: {sources_text}")
                report_lines.append(f"📊 Медиана: {median_price:.2f}$")
            elif len(sources) == 1:
                src, price = list(sources.items())[0]
                report_lines.append(f"📡 Источник: {src} ({price:.2f}$)")
            
            if steam_price:
                report_lines.append(f"🔧 Steam: {steam_price:.2f}$ (справочно)")
        else:
            report_lines.append("❌ Цены недоступны")
        
        item_reports.append("\n".join(report_lines))
    
    final_report += "\n\n".join(item_reports)
    
    # Пагинация
    if total_pages > 1:
        final_report += f"\n\n📄 Страница {page + 1} из {total_pages}"
    
    # Inline кнопки для навигации и управления
    buttons = []
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"page_{page - 1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(text="➡️ Далее", callback_data=f"page_{page + 1}"))
        if nav_buttons:
            buttons.append(nav_buttons)
    
    # Кнопки действий
    action_buttons = [
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"refresh_portfolio_{page}"),
        InlineKeyboardButton(text="📈 График", callback_data="show_chart")
    ]
    buttons.append(action_buttons)
    
    markup = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    
    await progress_msg.delete()
    await message.answer(final_report, reply_markup=markup)

async def generate_portfolio_report(message: Message, page: int):
    """Генерация и отправка/редактирование отчета по портфелю с пагинацией."""
    global portfolio_prices_cache

    items = get_items_from_db()
    if not items:
        await message.answer(
            "❌ Портфель пуст. Добавь предметы, используя кнопку '➕ Добавить'.")
        return

    # Если кэш пуст, делаем запросы
    if not portfolio_prices_cache:
        await fetch_marketcsgo_prices()

        tasks = {
            item[1]: asyncio.create_task(get_current_prices_and_steam(item[1]))
            for item in items
        }
        if tasks:
            await asyncio.gather(*tasks.values())

        for _, name, qty, buy_uah, buy_usd in items:
            marketcsgo_price_usd, steam_price_usd = tasks[name].result()

            portfolio_prices_cache[name] = {
                'marketcsgo_usd': marketcsgo_price_usd,
                'steam_usd': steam_price_usd,
                'buy_uah': buy_uah,
                'buy_usd': buy_usd,
                'quantity': qty
            }

    # Считаем общую статистику по всему портфелю на основе кэша
    total_buy_uah = 0
    total_now_uah = 0.0
    category_stats = {}

    for name, data in portfolio_prices_cache.items():
        qty = data['quantity']
        buy_uah = data['buy_uah']
        marketcsgo_price_usd = data['marketcsgo_usd']

        pos_buy_uah = buy_uah * qty
        total_buy_uah += pos_buy_uah

        if marketcsgo_price_usd:
            current_price_uah = marketcsgo_price_usd * USD_TO_UAH
            total_now_uah += current_price_uah * qty
        else:
            total_now_uah += pos_buy_uah

        category = get_item_category(name)
        if category not in category_stats:
            category_stats[category] = {'buy_uah': 0, 'now_uah': 0}

        category_stats[category]['buy_uah'] += pos_buy_uah
        category_stats[category]['now_uah'] += (
            marketcsgo_price_usd * USD_TO_UAH
        ) * qty if marketcsgo_price_usd is not None else pos_buy_uah

    # Формирование итогового отчета (сводная часть)
    total_profit_uah = total_now_uah - total_buy_uah
    total_profit_usd = total_profit_uah / USD_TO_UAH
    total_profit_pct = (total_profit_uah /
                        total_buy_uah) * 100 if total_buy_uah > 0 else 0
    total_buy_usd = total_buy_uah / USD_TO_UAH
    total_now_usd = total_now_uah / USD_TO_UAH

    total_profit_emoji = "🟢" if total_profit_uah >= 0 else "🔴"

    final_report = (
        f"<b>📊 Твой портфель:</b>\n"
        f"<b>💰 Общая закупка:</b> {total_buy_uah:,.2f}₴ | {total_buy_usd:,.2f}$\n"
        f"<b>📈 Текущая стоимость:</b> {total_now_uah:,.2f}₴ | {total_now_usd:,.2f}$\n"
        f"<b>🚀 Общий профит:</b> {total_profit_emoji} {total_profit_uah:,.2f}₴ ({total_profit_usd:,.2f}$) ({total_profit_pct:+.2f}%)\n"
    )

    final_report += "\n--- <b>Статистика по категориям</b> ---\n"
    for category, values in category_stats.items():
        cat_profit = values['now_uah'] - values['buy_uah']
        cat_profit_pct = (cat_profit / values['buy_uah']
                          ) * 100 if values['buy_uah'] > 0 else 0
        cat_profit_emoji = "🟢" if cat_profit >= 0 else "🔴"
        final_report += f"{category}: {values['now_uah']:,.2f}₴ ({cat_profit_emoji} {cat_profit_pct:+.2f}%)\n"

    final_report += "\n--- <b>Детализация по предметам</b> ---\n"

    # Пагинация
    total_pages = (len(items) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    start_index = page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE
    items_on_page = items[start_index:end_index]

    item_reports = []
    for _, name, qty, buy_uah, buy_usd in items_on_page:

        report_lines = []
        report_lines.append(f"<b>{name}</b>")
        report_lines.append(f"📦 Количество: {qty}")

        prices = portfolio_prices_cache.get(name)
        if not prices:
            report_lines.append(
                "❌ Нет данных о ценах. Попробуйте обновить портфель.")
            item_reports.append("\n".join(report_lines))
            continue

        marketcsgo_usd = prices.get('marketcsgo_usd')
        steam_usd = prices.get('steam_usd')
        buy_uah = prices.get('buy_uah')
        buy_usd = prices.get('buy_usd')

        # Общая стоимость закупки
        buy_total_uah = buy_uah * qty
        buy_total_usd = buy_usd * qty
        report_lines.append(
            f"💰 Общая закупка: {buy_total_uah:,.2f}₴ | {buy_total_usd:,.2f}$")

        # Текущая общая стоимость
        current_total_uah = 0
        current_total_usd = 0
        if marketcsgo_usd:
            current_total_uah = (marketcsgo_usd * USD_TO_UAH) * qty
            current_total_usd = marketcsgo_usd * qty
            report_lines.append(
                f"📈 Текущая общая стоимость: {current_total_uah:,.2f}₴ | {current_total_usd:,.2f}$"
            )
        else:
            report_lines.append("❌ Текущая общая стоимость: Нет данных")

        # Профит
        if marketcsgo_usd:
            profit_uah = current_total_uah - buy_total_uah
            profit_usd = current_total_usd - buy_total_usd
            profit_pct = (profit_uah /
                          buy_total_uah) * 100 if buy_total_uah > 0 else 0

            profit_emoji = "🟢" if profit_pct >= 0 else "🔴"
            report_lines.append(
                f"📊 Общий профит: {profit_emoji} {profit_uah:,.2f}₴ ({profit_usd:,.2f}$) ({profit_pct:+.2f}%)"
            )
        else:
            report_lines.append(f"❌ Нет данных для расчета профита.")

        # Цена за 1 шт. и её профит - только если количество > 1
        if qty > 1:
            report_lines.append("")
            report_lines.append(f"<b>Цены за 1 шт.:</b>")
            report_lines.append(
                f"💰 Закупка: {buy_uah:,.2f}₴ | {buy_usd:,.2f}$")

            # Процент роста для 1 штуки MarketCSGO
            if marketcsgo_usd:
                current_uah_per_item = marketcsgo_usd * USD_TO_UAH
                profit_pct_per_item_market = (
                    (current_uah_per_item - buy_uah) /
                    buy_uah) * 100 if buy_uah > 0 else 0
                profit_emoji_per_item_market = "🟢" if profit_pct_per_item_market >= 0 else "🔴"
                report_lines.append(
                    f"📈 MarketCSGO: {current_uah_per_item:,.2f}₴ | {marketcsgo_usd:,.2f}$ ({profit_emoji_per_item_market} {profit_pct_per_item_market:+.2f}%)"
                )
            else:
                report_lines.append(f"❌ MarketCSGO: Нет данных")

            # Процент роста для 1 штуки Steam
            if steam_usd:
                steam_uah_per_item = steam_usd * USD_TO_UAH
                profit_pct_per_item_steam = (
                    (steam_uah_per_item - buy_uah) /
                    buy_uah) * 100 if buy_uah > 0 else 0
                profit_emoji_per_item_steam = "🟢" if profit_pct_per_item_steam >= 0 else "🔴"
                report_lines.append(
                    f"📈 Steam: {steam_uah_per_item:,.2f}₴ | {steam_usd:,.2f}$ ({profit_emoji_per_item_steam} {profit_pct_per_item_steam:+.2f}%)"
                )
            else:
                report_lines.append(f"❌ Steam: Нет данных")

        item_reports.append("\n".join(report_lines))

    final_report += "\n\n---\n\n".join(item_reports)

    # Создание кнопок пагинации и редактирования
    keyboard_buttons = []
    navigation_row = []

    if page > 0:
        navigation_row.append(
            InlineKeyboardButton(text="⬅️", callback_data=f"page_{page-1}"))
    if page < total_pages - 1:
        navigation_row.append(
            InlineKeyboardButton(text="➡️", callback_data=f"page_{page+1}"))

    if navigation_row:
        keyboard_buttons.append(navigation_row)

    if items:
        keyboard_buttons.append([
            InlineKeyboardButton(text="✏️ Редактировать",
                                 callback_data=f"edit_page_{page}")
        ])

    markup = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

    # Сохраняем текущую стоимость портфеля для уведомлений
    save_portfolio_value(total_now_uah)

    try:
        # Пытаемся отредактировать сообщение
        await message.edit_text(final_report, reply_markup=markup)
    except Exception as e:
        # Если не получилось (например, это первое сообщение), отправляем новое
        logging.info(
            f"Не удалось отредактировать сообщение, отправляю новое: {e}")
        await message.answer(final_report, reply_markup=markup)


@router.callback_query(lambda c: (c.data or "").startswith("edit_page_"))
async def show_edit_options(callback: CallbackQuery):
    """Показывает кнопки редактирования для предметов на выбранной странице, редактируя текущее сообщение."""
    if not callback.data:
        return
    page = int(callback.data.split("_")[2])

    items = get_items_from_db()
    start_index = page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE
    items_on_page = items[start_index:end_index]

    if not items_on_page:
        await callback.message.edit_text(
            "❌ На этой странице нет предметов для редактирования.",
            reply_markup=None)
        await callback.answer()
        return

    keyboard_buttons = []
    for item_id, name, _, _, _ in items_on_page:
        keyboard_buttons.append([
            InlineKeyboardButton(text=f"✏️ {name}",
                                 callback_data=f"edit_item_{item_id}"),
            InlineKeyboardButton(text=f"🗑️",
                                 callback_data=f"delete_item_{item_id}")
        ])

    # Кнопка "Назад" для возврата к отчету
    keyboard_buttons.append(
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"page_{page}")])

    await callback.message.edit_text(
        "👇 Выберите предмет для редактирования/удаления:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons))
    await callback.answer()


# --- ОБРАБОТКА РЕДАКТИРОВАНИЯ ---
@router.callback_query(lambda c: (c.data or "").startswith("edit_item_"))
async def start_edit(callback: CallbackQuery, state: FSMContext):
    if not callback.data:
        return
    item_id = int(callback.data.split("_")[2])
    await state.update_data(item_id=item_id)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✏️ Количество",
                             callback_data="edit_quantity")
    ], [
        InlineKeyboardButton(text="📝 Цена закупки", callback_data="edit_price")
    ]])

    await safe_edit_or_send(callback, text="Что ты хочешь изменить?", reply_markup=keyboard)
    await state.set_state(EditItemState.choosing_what_to_edit)


@router.callback_query(lambda c: c.data == "edit_quantity",
                       StateFilter(EditItemState.choosing_what_to_edit))
async def ask_new_quantity(callback: CallbackQuery, state: FSMContext):
    await safe_edit_or_send(callback, text="Отправь новое количество (только число):")
    await state.set_state(EditItemState.waiting_for_new_quantity)


@router.callback_query(lambda c: c.data == "edit_price",
                       StateFilter(EditItemState.choosing_what_to_edit))
async def ask_new_price(callback: CallbackQuery, state: FSMContext):
    await safe_edit_or_send(callback, text="Отправь новую цену закупки в ₴ (только число):")
    await state.set_state(EditItemState.waiting_for_new_price)


@router.message(StateFilter(EditItemState.waiting_for_new_quantity))
async def process_new_quantity(message: Message, state: FSMContext):
    if not message.text:
        return
    try:
        new_quantity = int(message.text.strip())
        if new_quantity <= 0:
            await message.answer(
                "❌ Количество должно быть положительным числом. Попробуй еще раз."
            )
            return

        user_data = await state.get_data()
        item_id = user_data.get("item_id")

        if item_id is None:
            await message.answer(
                "❌ Произошла ошибка. Пожалуйста, попробуй начать заново.")
            await state.clear()
            return

        update_item_quantity(item_id, new_quantity)
        await message.answer("✅ Количество предмета успешно обновлено!")
        await state.clear()

    except ValueError:
        await message.answer(
            "❌ Неверный формат. Пожалуйста, отправь только число.")


@router.message(StateFilter(EditItemState.waiting_for_new_price))
async def process_new_price(message: Message, state: FSMContext):
    if not message.text:
        return
    try:
        new_price = float(message.text.strip())
        if new_price <= 0:
            await message.answer(
                "❌ Цена должна быть положительным числом. Попробуй еще раз.")
            return

        user_data = await state.get_data()
        item_id = user_data.get("item_id")

        if item_id is None:
            await message.answer(
                "❌ Произошла ошибка. Пожалуйста, попробуй начать заново.")
            await state.clear()
            return

        update_item_price(item_id, new_price)
        await message.answer("✅ Цена закупки предмета успешно обновлена!")
        await state.clear()

    except ValueError:
        await message.answer(
            "❌ Неверный формат. Пожалуйста, отправь только число.")


@router.message(F.text == "🗑️ Удалить")
async def delete_item_cmd(message: Message):
    """Обработчик кнопки 'Удалить предмет'."""
    items = get_items_from_db()
    if not items:
        await message.answer("❌ Портфель пуст. Нечего удалять.")
        return

    buttons = []
    for item_id, name, qty, buy_price_uah, buy_price_usd in items:
        buttons.append([
            InlineKeyboardButton(text=f"{name} (x{qty})",
                                 callback_data=f"delete_{item_id}")
        ])

    markup = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer("🗑️ Выбери предмет для удаления:",
                         reply_markup=markup)


@router.callback_query(lambda c: (c.data or "").startswith("delete_item_"))
async def delete_item_from_edit(callback: CallbackQuery):
    """Обработчик удаления предмета из меню редактирования."""
    if not callback.data:
        return
    item_id = int(callback.data.split("_")[2])
    if delete_item_by_id(item_id):
        await safe_edit_or_send(callback, text="✅ Предмет удален из портфеля!")
    else:
        await callback.answer("❌ Не удалось удалить предмет.")


@router.callback_query(lambda c: (c.data or "").startswith("delete_"))
async def delete_item_callback(callback: CallbackQuery):
    """Обработчик удаления предмета по кнопке."""
    if not callback.data:
        return
    item_id = int(callback.data.split("_")[1])
    if delete_item_by_id(item_id):
        await safe_edit_or_send(callback, text="✅ Предмет удален из портфеля!")
    else:
        await callback.answer("❌ Не удалось удалить предмет.")


@router.message(F.text == "📈 График")
async def history_cmd(message: Message):
    """
    Обработчик кнопки 'График'.
    Собирает данные из истории портфеля и строит график.
    """
    items = get_items_from_db()
    if not items:
        await message.answer(
            "❌ В вашем портфеле пока нет предметов. Добавьте их, чтобы построить график."
        )
        return

    # 1. Сбор данных для графика
    history = get_portfolio_history()
    dates = []
    values = []

    if not history:
        # Если истории еще нет, строим график только с одной точкой - общей ценой закупки
        total_buy_price = get_total_buy_price()
        await message.answer(
            f"❌ Пока нет данных для графика. Добавьте предметы и вызовите отчет '📊 Портфель', чтобы сохранить первую точку истории. \n\n Текущая общая цена закупки: {total_buy_price:,.2f}₴"
        )
        return

    for row in history:
        dates.append(datetime.fromisoformat(row[0]))
        values.append(row[1])

    # --- 2. Стилизация графика 1в1 как в Steam Market ---
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(14, 10), facecolor='#1b2838')

    # Расчет статистики для заголовка - инициализируем переменные по умолчанию
    price_change = 0.0
    price_change_pct = 0.0
    min_price = 0.0
    max_price = 0.0
    avg_price = 0.0
    change_color = '#c7d5e0'
    change_symbol = ''
    
    if len(values) > 1:
        price_change = values[-1] - values[0]
        price_change_pct = (price_change /
                            values[0]) * 100 if values[0] != 0 else 0
        min_price = min(values)
        max_price = max(values)
        avg_price = sum(values) / len(values)
        change_color = '#90c53f' if price_change >= 0 else '#d75f36'
        change_symbol = '+' if price_change >= 0 else ''

    # Создаем верхнюю область для статистики
    text_color = '#c7d5e0'

    # Заголовок
    fig.text(0.08,
             0.95,
             "Стоимость портфеля CS2",
             fontsize=18,
             color=text_color,
             fontweight='bold')

    if len(values) > 1:
        # Изменение цены справа
        fig.text(
            0.92,
            0.95,
            f"Изменение: {change_symbol}{price_change:,.2f}₴ ({price_change_pct:+.1f}%)",
            fontsize=14,
            color=change_color,
            fontweight='bold',
            ha='right')

        # Статистика в одну строку
        stats_text = f"Текущая: {values[-1]:,.2f}₴  •  Максимум: {max_price:,.2f}₴  •  Минимум: {min_price:,.2f}₴  •  Средняя: {avg_price:,.2f}₴"
        fig.text(0.08, 0.90, stats_text, fontsize=11, color=text_color)

    # Создаем основную область графика
    ax = fig.add_subplot(111)
    ax.set_position((0.08, 0.12, 0.84, 0.75))  # [left, bottom, width, height]
    ax.set_facecolor('#1b2838')

    # Точные цвета Steam Market
    line_color = '#66c0f4'  # Синий цвет Steam
    fill_color = '#4c6b22'  # Зеленый для заливки
    grid_color = '#316282'

    # Убираем рамки как в Steam
    for spine in ax.spines.values():
        spine.set_visible(False)

    # Сетка как в Steam - только горизонтальные линии
    ax.grid(True,
            axis='y',
            linestyle='-',
            linewidth=0.5,
            color=grid_color,
            alpha=0.6)
    ax.set_axisbelow(True)

    # Настройки тиков
    ax.tick_params(axis='both', colors=text_color, labelsize=10)
    ax.tick_params(axis='x', length=0)  # Убираем тики на оси X
    ax.tick_params(axis='y', length=0)  # Убираем тики на оси Y

    # Форматирование оси X с умным выбором интервалов
    if len(dates) > 20:
        formatter = DateFormatter('%d.%m')
    elif len(dates) > 7:
        formatter = DateFormatter('%d.%m\n%H:%M')
    else:
        formatter = DateFormatter('%d %b\n%H:%M')

    ax.xaxis.set_major_formatter(formatter)

    # Заливка области под графиком (градиент эффект)
    ax.fill_between(dates, values, color=fill_color, alpha=0.3, zorder=1)
    ax.fill_between(dates, values, color=line_color, alpha=0.1, zorder=2)

    # Основная линия графика
    ax.plot(dates,
            values,
            color=line_color,
            linewidth=2.5,
            zorder=5,
            solid_capstyle='round')

    # Точки на концах и важных моментах
    if len(dates) <= 10:
        ax.scatter(dates,
                   values,
                   color=line_color,
                   s=35,
                   zorder=10,
                   edgecolors='white',
                   linewidth=1)
    else:
        # Только первая и последняя точка если много данных
        ax.scatter([dates[0], dates[-1]], [values[0], values[-1]],
                   color=line_color,
                   s=40,
                   zorder=10,
                   edgecolors='white',
                   linewidth=1.5)

    # Подсветка текущего значения
    if dates and values:
        # Вертикальная линия к последней точке
        ax.axvline(x=dates[-1],
                   color=text_color,
                   linestyle='--',
                   alpha=0.3,
                   linewidth=1)

        # Горизонтальная линия текущей цены
        ax.axhline(y=values[-1],
                   color=line_color,
                   linestyle='--',
                   alpha=0.4,
                   linewidth=1)

    # Форматирование осей
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f'{x:,.0f}₴'))

    # Поворот меток X если нужно
    if len(dates) > 10:
        plt.xticks(rotation=45, ha='right')

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=200)
    buf.seek(0)

    await message.answer_photo(
        photo=BufferedInputFile(buf.getvalue(),
                                filename="portfolio_graph.png"),
        caption=
        "📈 Вот твой красивый график портфеля! Каждая точка - это вызов отчета '📊 Портфель'."
    )

    plt.close(fig)


# --- ОБРАБОТЧИКИ ОТСЛЕЖИВАНИЯ ЦЕН ---
@router.message(F.text == "🔔 Отслеживать цену")
async def track_price_cmd(message: Message, state: FSMContext):
    """Обработчик кнопки 'Отслеживать цену'."""
    await state.set_state(PriceAlertState.waiting_for_item_name)
    await message.answer(
        "🔔 <b>Создание уведомления о цене</b>\n\n"
        "Напиши <b>точное название</b> предмета, за которым хочешь следить:\n\n"
        "Пример: <code>AK-47 | Redline (Field-Tested)</code>")


@router.message(StateFilter(PriceAlertState.waiting_for_item_name))
async def track_price_name(message: Message, state: FSMContext):
    """Обработчик ввода названия предмета для отслеживания."""
    if not message.text:
        return
    item_name = message.text.strip()
    await state.update_data(item_name=item_name)
    await state.set_state(PriceAlertState.waiting_for_target_price)

    await message.answer(f"✅ Предмет: <b>{item_name}</b>\n\n"
                         "💰 Теперь напиши <b>целевую цену в гривнах</b>:\n\n"
                         "Пример: <code>1500</code> или <code>1500.50</code>")


@router.message(StateFilter(PriceAlertState.waiting_for_target_price))
async def track_price_target(message: Message, state: FSMContext):
    """Обработчик ввода целевой цены."""
    if not message.text:
        return
    try:
        target_price = float(message.text.strip())
        if target_price <= 0:
            await message.answer(
                "⚠️ Цена должна быть больше 0. Попробуй еще раз:")
            return

        await state.update_data(target_price=target_price)
        await state.set_state(PriceAlertState.waiting_for_direction)

        # Кнопки выбора направления
        kb = [[
            InlineKeyboardButton(
                text="📈 Уведомить когда цена вырастет до этой суммы",
                callback_data="direction_up")
        ],
              [
                  InlineKeyboardButton(
                      text="📉 Уведомить когда цена упадет до этой суммы",
                      callback_data="direction_down")
              ]]
        markup = InlineKeyboardMarkup(inline_keyboard=kb)

        await message.answer(
            f"💰 Целевая цена: <b>{target_price:,.2f}₴</b>\n\n"
            "🎯 Выбери, когда получать уведомление:",
            reply_markup=markup)
    except ValueError:
        await message.answer(
            "❌ Неверный формат цены. Напиши число (например: 1500 или 1500.50):"
        )


@router.callback_query(lambda c: (c.data or "").startswith("direction_"),
                       StateFilter(PriceAlertState.waiting_for_direction))
async def track_price_direction(callback: CallbackQuery, state: FSMContext):
    """Обработчик выбора направления уведомления."""
    if not callback.data:
        return
    direction = callback.data.split("_")[1]  # "up" или "down"

    data = await state.get_data()
    item_name = data['item_name']
    target_price = data['target_price']

    # Добавляем уведомление в базу
    add_price_alert_to_db(callback.from_user.id, item_name, target_price,
                          direction)

    direction_text = "вырастет до" if direction == "up" else "упадет до"

    await callback.message.edit_text(
        f"✅ <b>Уведомление создано!</b>\n\n"
        f"📦 Предмет: <b>{item_name}</b>\n"
        f"💰 Цена: <b>{target_price:,.2f}₴</b>\n"
        f"🎯 Условие: когда цена <b>{direction_text}</b> указанной суммы\n\n"
        f"Я буду проверять цены каждые 30 минут и уведомлю тебя, когда условие выполнится!"
    )

    await state.clear()
    await callback.answer()


@router.message(F.text == "🔔 Мои отслеживания")
async def my_alerts_cmd(message: Message):
    """Обработчик кнопки 'Мои отслеживания'."""
    alerts = get_price_alerts_by_user(message.from_user.id)

    if not alerts:
        await message.answer("❌ У тебя нет активных уведомлений о ценах.")
        return

    text = "<b>🔔 Твои активные уведомления:</b>\n\n"

    for alert_id, item_name, target_price, direction in alerts:
        direction_emoji = "📈" if direction == "up" else "📉"
        direction_text = "вырастет до" if direction == "up" else "упадет до"

        text += f"{direction_emoji} <b>{item_name}</b>\n"
        text += f"💰 {target_price:,.2f}₴ (когда цена {direction_text})\n\n"

    # Кнопка управления уведомлениями
    kb = [[
        InlineKeyboardButton(text="🗑️ Удалить уведомление",
                             callback_data="manage_alerts")
    ]]
    markup = InlineKeyboardMarkup(inline_keyboard=kb)

    await message.answer(text, reply_markup=markup)


@router.callback_query(lambda c: c.data == "manage_alerts")
async def manage_alerts_callback(callback: CallbackQuery):
    """Обработчик управления уведомлениями."""
    alerts = get_price_alerts_by_user(callback.from_user.id)

    if not alerts:
        await safe_edit_or_send(callback, text="❌ У тебя нет активных уведомлений.")
        return

    buttons = []
    for alert_id, item_name, target_price, direction in alerts:
        direction_emoji = "📈" if direction == "up" else "📉"
        buttons.append([
            InlineKeyboardButton(
                text=f"{direction_emoji} {item_name} ({target_price:,.0f}₴)",
                callback_data=f"delete_alert_{alert_id}")
        ])

    markup = InlineKeyboardMarkup(inline_keyboard=buttons)
    await safe_edit_or_send(callback, text="🗑️ Выбери уведомление для удаления:",
                                     reply_markup=markup)


@router.callback_query(lambda c: (c.data or "").startswith("delete_alert_"))
async def delete_alert_callback(callback: CallbackQuery):
    """Обработчик удаления уведомления."""
    if not callback.data:
        return
    alert_id = int(callback.data.split("_")[2])

    if delete_price_alert(alert_id):
        await safe_edit_or_send(callback, text="✅ Уведомление удалено!")
    else:
        await callback.answer("❌ Не удалось удалить уведомление.")


# --- ОБРАБОТЧИКИ НАСТРОЕК УВЕДОМЛЕНИЙ ---


@router.callback_query(lambda c: c.data == "set_threshold")
async def set_threshold_callback(callback: CallbackQuery, state: FSMContext):
    """Обработчик установки порога уведомлений."""
    await callback.message.edit_text(
        "🎯 <b>Настройка порога уведомлений</b>\n\n"
        "Введи процент изменения цены, при котором я буду отправлять уведомления.\n\n"
        "Например: <code>5</code> - уведомления при изменении цены на ±5%\n"
        "Или: <code>10.5</code> - при изменении на ±10.5%")
    await state.set_state(NotificationSettingsState.waiting_for_threshold)
    await callback.answer()


@router.callback_query(lambda c: (c.data or "").startswith("toggle_items_"))
async def toggle_items_callback(callback: CallbackQuery):
    """Обработчик переключения отслеживания отдельных предметов."""
    if not callback.data:
        return
    new_value = callback.data.split("_")[2] == "True"
    user_id = callback.from_user.id
    update_user_notification_settings(user_id,
                                      check_individual_items=new_value)

    status = "включено" if new_value else "выключено"
    await callback.message.edit_text(
        f"✅ Отслеживание изменений отдельных предметов {status}!")
    await callback.answer()


@router.callback_query(lambda c: (c.data or "").startswith("toggle_portfolio_"))
async def toggle_portfolio_callback(callback: CallbackQuery):
    """Обработчик переключения общих уведомлений о портфеле."""
    if not callback.data:
        return
    new_value = callback.data.split("_")[2] == "True"
    user_id = callback.from_user.id
    update_user_notification_settings(user_id, check_portfolio_total=new_value)

    status = "включены" if new_value else "выключены"
    await callback.message.edit_text(
        f"✅ Общие уведомления о портфеле {status}!")
    await callback.answer()


@router.callback_query(lambda c: c.data == "manage_watchlist")
async def manage_watchlist_callback(callback: CallbackQuery):
    """Обработчик управления списком отслеживания."""
    user_id = callback.from_user.id
    watchlist = get_user_watchlist(user_id)

    if not watchlist:
        kb = [[
            InlineKeyboardButton(text="➕ Добавить предмет",
                                 callback_data="add_to_watchlist")
        ]]
        text = "👁️ <b>Список отслеживания пуст</b>\n\nДобавь предметы, за ценами которых хочешь следить:"
    else:
        text = "👁️ <b>Твой список отслеживания:</b>\n\n"
        kb = []

        for item_name, last_price in watchlist:
            text += f"📦 {item_name}\n💰 Последняя цена: {last_price:,.0f}₴\n\n"
            kb.append([
                InlineKeyboardButton(text=f"🗑️ {item_name}",
                                     callback_data=f"remove_watch_{item_name}")
            ])

        kb.append([
            InlineKeyboardButton(text="➕ Добавить предмет",
                                 callback_data="add_to_watchlist")
        ])

    kb.append([
        InlineKeyboardButton(text="⬅️ Назад к настройкам",
                             callback_data="refresh_settings")
    ])
    markup = InlineKeyboardMarkup(inline_keyboard=kb)

    await safe_edit_or_send(callback, text=text, reply_markup=markup)


@router.callback_query(lambda c: c.data == "add_to_watchlist")
async def add_to_watchlist_callback(callback: CallbackQuery,
                                    state: FSMContext):
    """Обработчик добавления предмета в список отслеживания."""
    await callback.message.edit_text(
        "📝 <b>Добавление в список отслеживания</b>\n\n"
        "Введи точное название предмета CS2, который хочешь отслеживать.\n\n"
        "Например:\n"
        "<code>AK-47 | Redline (Field-Tested)</code>\n"
        "<code>AWP | Dragon Lore (Factory New)</code>")
    await state.set_state(NotificationSettingsState.waiting_for_watchlist_item)
    await callback.answer()


@router.callback_query(lambda c: (c.data or "").startswith("remove_watch_"))
async def remove_from_watchlist_callback(callback: CallbackQuery):
    """Обработчик удаления предмета из списка отслеживания."""
    if not callback.data:
        return
    item_name = callback.data.replace("remove_watch_", "")
    user_id = callback.from_user.id

    if remove_from_watchlist(user_id, item_name):
        await callback.message.edit_text(
            f"✅ '{item_name}' удален из списка отслеживания!")
    else:
        await callback.message.edit_text(
            "❌ Не удалось удалить предмет из списка.")
    await callback.answer()


@router.callback_query(lambda c: c.data == "refresh_settings")
async def refresh_settings_callback(callback: CallbackQuery):
    """Обработчик обновления настроек уведомлений."""
    user_id = callback.from_user.id
    threshold, check_items, check_portfolio, last_prices = get_user_notification_settings(
        user_id)

    # Получаем количество предметов в списке отслеживания
    watchlist = get_user_watchlist(user_id)
    watchlist_count = len(watchlist)

    text = (f"⚙️ <b>Настройки уведомлений</b>\n\n"
            f"🎯 Порог изменения: <b>{threshold}%</b>\n"
            f"💼 Отслеживание портфеля: {'✅' if check_items else '❌'}\n"
            f"📊 Общие уведомления: {'✅' if check_portfolio else '❌'}\n"
            f"👁️ Предметов в отслеживании: <b>{watchlist_count}</b>\n\n"
            f"📋 Выбери что хочешь настроить:")

    kb = [[
        InlineKeyboardButton(text="🎯 Изменить порог (%)",
                             callback_data="set_threshold")
    ],
          [
              InlineKeyboardButton(
                  text="💼 Отслеживание портфеля",
                  callback_data=f"toggle_items_{not check_items}")
          ],
          [
              InlineKeyboardButton(
                  text="📊 Общие уведомления",
                  callback_data=f"toggle_portfolio_{not check_portfolio}")
          ],
          [
              InlineKeyboardButton(text="👁️ Управление списком",
                                   callback_data="manage_watchlist")
          ],
          [
              InlineKeyboardButton(text="🔄 Обновить",
                                   callback_data="refresh_settings")
          ]]
    markup = InlineKeyboardMarkup(inline_keyboard=kb)

    await safe_edit_or_send(callback, text=text, reply_markup=markup)


# --- ОБРАБОТЧИКИ СОСТОЯНИЙ НАСТРОЕК ---


@router.message(StateFilter(NotificationSettingsState.waiting_for_threshold))
async def process_threshold(message: Message, state: FSMContext):
    """Обработчик ввода порога уведомлений."""
    if not message.text:
        return
    try:
        threshold = float(message.text.replace(',', '.'))
        if threshold <= 0 or threshold > 100:
            await message.answer(
                "❌ Порог должен быть от 0.1% до 100%. Попробуй еще раз:")
            return

        user_id = message.from_user.id
        update_user_notification_settings(user_id, threshold_percent=threshold)

        await message.answer(
            f"✅ <b>Порог уведомлений установлен!</b>\n\n"
            f"🎯 Новый порог: <b>{threshold}%</b>\n\n"
            f"Теперь я буду уведомлять тебя, когда цены изменятся на ±{threshold}% или больше.",
            reply_markup=get_main_keyboard())
        await state.clear()

    except ValueError:
        await message.answer("❌ Введи правильное число. Например: 5 или 10.5")


@router.message(
    StateFilter(NotificationSettingsState.waiting_for_watchlist_item))
async def process_watchlist_item(message: Message, state: FSMContext):
    """Обработчик добавления предмета в список отслеживания."""
    if not message.text:
        return
    item_name = message.text.strip()
    user_id = message.from_user.id

    # Проверяем, что предмет существует и получаем его цену
    current_usd, _ = await get_current_prices_and_steam(item_name)

    if current_usd is None:
        await message.answer(
            f"❌ <b>Предмет не найден</b>\n\n"
            f"Предмет '{item_name}' не найден в MarketCSGO.\n\n"
            f"Проверь правильность названия и попробуй еще раз:")
        return

    current_uah = current_usd * USD_TO_UAH
    add_item_to_watchlist(user_id, item_name, current_uah)

    await message.answer(
        f"✅ <b>Предмет добавлен в отслеживание!</b>\n\n"
        f"📦 {item_name}\n"
        f"💰 Текущая цена: {current_uah:,.0f}₴\n\n"
        f"Я буду уведомлять тебя о значительных изменениях цены.",
        reply_markup=get_main_keyboard())
    await state.clear()


# --- НОВЫЕ ФУНКЦИИ ДЛЯ РАСШИРЕННЫХ УВЕДОМЛЕНИЙ ---


def get_user_notification_settings(user_id):
    """Получить настройки уведомлений пользователя."""
    with get_db_cursor() as (cur, _):
        cur.execute(
            "SELECT threshold_percent, check_individual_items, check_portfolio_total, last_item_prices FROM user_notification_settings WHERE user_id = ?",
            (user_id, ))
        result = cur.fetchone()
        if result:
            threshold, check_items, check_portfolio, last_prices_json = result
            import json
            try:
                last_prices = json.loads(
                    last_prices_json) if last_prices_json else {}
            except:
                last_prices = {}
            return threshold, bool(check_items), bool(
                check_portfolio), last_prices
        else:
            # Создаем настройки по умолчанию
            cur.execute(
                "INSERT OR REPLACE INTO user_notification_settings (user_id) VALUES (?)",
                (user_id, ))
            return 5.0, True, True, {}


def update_user_notification_settings(user_id,
                                      threshold_percent=None,
                                      check_individual_items=None,
                                      check_portfolio_total=None,
                                      last_item_prices=None):
    """Обновить настройки уведомлений пользователя."""
    with get_db_cursor() as (cur, _):
        # Получаем текущие настройки
        current = get_user_notification_settings(user_id)

        # Обновляем только переданные параметры
        new_threshold = threshold_percent if threshold_percent is not None else current[
            0]
        new_check_items = check_individual_items if check_individual_items is not None else current[
            1]
        new_check_portfolio = check_portfolio_total if check_portfolio_total is not None else current[
            2]
        new_last_prices = last_item_prices if last_item_prices is not None else current[
            3]

        import json
        last_prices_json = json.dumps(new_last_prices)

        cur.execute(
            """
            INSERT OR REPLACE INTO user_notification_settings 
            (user_id, threshold_percent, check_individual_items, check_portfolio_total, last_item_prices) 
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, new_threshold, int(new_check_items),
              int(new_check_portfolio), last_prices_json))


def add_item_to_watchlist(user_id, item_name, current_price_uah):
    """Добавить предмет в список отслеживания."""
    with get_db_cursor() as (cur, _):
        cur.execute(
            """
            INSERT OR REPLACE INTO item_watch_list (user_id, item_name, last_price_uah, created_at) 
            VALUES (?, ?, ?, ?)
        """, (user_id, item_name, current_price_uah,
              datetime.now().isoformat()))


def get_user_watchlist(user_id):
    """Получить список отслеживаемых предметов пользователя."""
    with get_db_cursor() as (cur, _):
        cur.execute(
            "SELECT item_name, last_price_uah FROM item_watch_list WHERE user_id = ?",
            (user_id, ))
        return cur.fetchall()


def remove_from_watchlist(user_id, item_name):
    """Удалить предмет из списка отслеживания."""
    with get_db_cursor() as (cur, _):
        cur.execute(
            "DELETE FROM item_watch_list WHERE user_id = ? AND item_name = ?",
            (user_id, item_name))
        return cur.rowcount > 0


def update_watchlist_price(user_id, item_name, new_price_uah):
    """Обновить цену предмета в списке отслеживания."""
    with get_db_cursor() as (cur, _):
        cur.execute(
            "UPDATE item_watch_list SET last_price_uah = ? WHERE user_id = ? AND item_name = ?",
            (new_price_uah, user_id, item_name))


# --- НОВАЯ УЛУЧШЕННАЯ СИСТЕМА УВЕДОМЛЕНИЙ ---


async def check_individual_price_changes():
    """Проверяет изменения цен отдельных предметов каждые 30 минут."""
    logging.info("Проверка индивидуальных изменений цен...")

    # Получаем всех пользователей с их настройками
    with get_db_cursor() as (cur, _):
        cur.execute(
            "SELECT user_id FROM user_notification_settings WHERE check_individual_items = 1"
        )
        users = cur.fetchall()

    if not users:
        logging.info(
            "Нет пользователей с включенными уведомлениями о предметах.")
        return

    # Обновляем кэш цен
    await fetch_marketcsgo_prices()

    for (user_id, ) in users:
        try:
            threshold, check_items, check_portfolio, last_prices = get_user_notification_settings(
                user_id)

            if not check_items:
                continue

            # Получаем портфель пользователя + список отслеживания
            portfolio_items = []
            watchlist_items = []

            # Портфель пользователя (если у нас есть данные по пользователю)
            items = get_items_from_db(
            )  # Получаем все предметы (в текущей версии база общая)

            # Список отслеживания пользователя
            watchlist = get_user_watchlist(user_id)

            notifications = []
            new_prices = {}

            # Проверяем предметы портфеля
            for item_id, name, qty, buy_price_uah, buy_price_usd in items:
                current_usd, _ = await get_current_prices_and_steam(name)
                if current_usd is None:
                    continue

                current_uah = current_usd * USD_TO_UAH
                new_prices[name] = current_uah

                # Сравниваем с последней известной ценой
                if name in last_prices:
                    old_price = last_prices[name]
                    change_percent = (
                        (current_uah - old_price) / old_price) * 100

                    if abs(change_percent) >= threshold:
                        emoji = "📈" if change_percent > 0 else "📉"
                        notifications.append({
                            'type': 'portfolio',
                            'name': name,
                            'change_percent': change_percent,
                            'current_price': current_uah,
                            'old_price': old_price,
                            'emoji': emoji,
                            'quantity': qty
                        })

            # Проверяем список отслеживания
            for name, last_price in watchlist:
                current_usd, _ = await get_current_prices_and_steam(name)
                if current_usd is None:
                    continue

                current_uah = current_usd * USD_TO_UAH
                change_percent = (
                    (current_uah - last_price) / last_price) * 100

                if abs(change_percent) >= threshold:
                    emoji = "📈" if change_percent > 0 else "📉"
                    notifications.append({
                        'type': 'watchlist',
                        'name': name,
                        'change_percent': change_percent,
                        'current_price': current_uah,
                        'old_price': last_price,
                        'emoji': emoji
                    })

                # Обновляем цену в списке отслеживания
                update_watchlist_price(user_id, name, current_uah)

            # Отправляем уведомления
            if notifications:
                message_parts = ["🔔 <b>Изменения цен в твоем портфеле:</b>\n"]

                for notif in notifications[:
                                           5]:  # Ограничиваем до 5 уведомлений за раз
                    change_text = f"{notif['change_percent']:+.1f}%"
                    type_text = "💼" if notif['type'] == 'portfolio' else "👁️"

                    message_parts.append(
                        f"\n{type_text} {notif['emoji']} <b>{notif['name']}</b>"
                        f"\n💰 {notif['current_price']:,.0f}₴ ({change_text})")

                    if notif['type'] == 'portfolio':
                        total_value = notif['current_price'] * notif['quantity']
                        message_parts.append(
                            f"\n📊 Общая стоимость: {total_value:,.0f}₴")

                if len(notifications) > 5:
                    message_parts.append(
                        f"\n... и еще {len(notifications) - 5} изменений")

                try:
                    await bot.send_message(user_id, ''.join(message_parts))
                    logging.info(
                        f"Отправлено {len(notifications)} уведомлений пользователю {user_id}"
                    )
                except Exception as e:
                    logging.error(
                        f"Не удалось отправить уведомления пользователю {user_id}: {e}"
                    )

            # Сохраняем новые цены
            update_user_notification_settings(user_id,
                                              last_item_prices=new_prices)

        except Exception as e:
            logging.error(
                f"Ошибка при проверке уведомлений для пользователя {user_id}: {e}"
            )


# --- ФОНОВЫЕ ЗАДАЧИ ---


def should_update_cache():
    global last_cache_update
    return last_cache_update is None or (datetime.now() -
                                         last_cache_update) >= CACHE_TTL


async def check_and_notify():
    """Проверяет изменения портфеля и отправляет уведомления."""
    logging.info("Проверка изменений портфеля...")

    items = get_items_from_db()
    if not items:
        logging.info("Портфель пуст, уведомления не отправляются.")
        return

    await fetch_marketcsgo_prices()

    total_now_uah = 0.0

    for item_id, name, qty, buy_price_uah, buy_price_usd in items:
        marketcsgo_price_usd, _ = await get_current_prices_and_steam(name)

        if marketcsgo_price_usd is not None:
            current_price_uah = marketcsgo_price_usd * USD_TO_UAH
            total_now_uah += current_price_uah * qty
        else:
            # Если цена недоступна, используем закупочную
            total_now_uah += buy_price_uah * qty

    last_value = get_last_known_value()

    if last_value is not None and last_value != 0:
        change_pct = ((total_now_uah - last_value) / last_value) * 100

        if abs(change_pct) >= NOTIFICATION_THRESHOLD_PERCENT:
            message_text = "<b>📈 Уведомление о портфеле:</b>\n\n"
            if change_pct > 0:
                message_text += f"🟢 Твой портфель вырос на <b>{change_pct:+.2f}%</b>!"
            else:
                message_text += f"🔴 Твой портфель упал на <b>{change_pct:+.2f}%</b>."

            message_text += f"\n\nТекущая стоимость: {total_now_uah:,.2f}₴"
            message_text += f"\nПоследняя стоимость: {last_value:,.2f}₴"

            users = get_subscribed_users()
            for user_id in users:
                try:
                    await bot.send_message(user_id, message_text)
                except Exception as e:
                    logging.error(
                        f"Не удалось отправить уведомление пользователю {user_id}: {e}"
                    )

    save_last_known_value(total_now_uah)


async def check_price_alerts():
    """Проверяет активные уведомления о ценах и отправляет сообщения, если условия выполнены."""

    alerts = get_all_price_alerts()
    for alert_id, user_id, item_name, target_price, direction in alerts:
        marketcsgo_usd, steam_usd = await get_current_prices_and_steam(
            item_name)

        if marketcsgo_usd is None:
            continue

        current_uah = marketcsgo_usd * USD_TO_UAH

        should_notify = False
        if direction == "up" and current_uah >= target_price:
            should_notify = True
        elif direction == "down" and current_uah <= target_price:
            should_notify = True

        if should_notify:
            message_text = (f"🔔 <b>Уведомление о цене!</b>\n\n"
                            f"📈 Цена на '{item_name}' достигла твоей цели!\n"
                            f"💰 Текущая цена: {current_uah:,.2f}₴\n"
                            f"🎯 Твоя цель: {target_price:,.2f}₴")
            try:
                await bot.send_message(user_id, message_text)
                delete_price_alert(
                    alert_id)  # Удаляем уведомление после отправки
            except Exception as e:
                logging.error(
                    f"Не удалось отправить уведомление пользователю {user_id}: {e}"
                )


async def generate_portfolio_chart():
    """Генерирует график стоимости портфеля с мультиисточниками."""
    try:
        # Получаем историю портфеля
        with get_db_cursor() as (cur, _):
            cur.execute("""
                SELECT timestamp, value_uah 
                FROM portfolio_history 
                ORDER BY timestamp 
                LIMIT 100
            """)
            history = cur.fetchall()
        
        if len(history) < 2:
            return None
        
        # Готовим данные для графика
        timestamps = [datetime.fromisoformat(row[0]) for row in history]
        values_uah = [float(row[1]) for row in history]
        
        # Создаем график
        plt.figure(figsize=(12, 8))
        ax = plt.gca()
        
        # Основная линия портфеля
        ax.plot([t for t in timestamps], values_uah, 
               color='#2E86AB', linewidth=2.5, 
               label='Портфель (общая стоимость)', marker='o', markersize=3)
        
        # Получаем данные по Steam ценам если доступны
        items = get_items_from_db()
        if items and len(timestamps) > 1:
            steam_values = []
            marketcsgo_values = []
            
            # Рассчитываем приблизительные значения Steam и MarketCSGO
            for i, timestamp in enumerate(timestamps):
                steam_total = 0
                marketcsgo_total = 0
                
                for _, name, qty, buy_uah, buy_usd in items:
                    # Получаем цены из кэша если доступны
                    price_data = multisource_prices_cache.get(name, {})
                    steam_price = price_data.get('steam')
                    sources = price_data.get('sources', {})
                    marketcsgo_price = sources.get('marketcsgo')
                    
                    if steam_price:
                        steam_total += steam_price * USD_TO_UAH * qty
                    if marketcsgo_price:
                        marketcsgo_total += marketcsgo_price * USD_TO_UAH * qty
                
                steam_values.append(steam_total if steam_total > 0 else values_uah[i])
                marketcsgo_values.append(marketcsgo_total if marketcsgo_total > 0 else values_uah[i])
            
            # Добавляем дополнительные линии если есть данные
            if any(v > 0 for v in steam_values):
                ax.plot([t for t in timestamps], steam_values, 
                       color='#FF6B35', linewidth=2, alpha=0.8,
                       label='Steam цены', linestyle='--')
            
            if any(v > 0 for v in marketcsgo_values):
                ax.plot([t for t in timestamps], marketcsgo_values,
                       color='#A23B72', linewidth=2, alpha=0.8, 
                       label='MarketCSGO цены', linestyle=':')
        
        # Настройка графика
        ax.set_title('📈 История стоимости портфеля', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Время', fontsize=12)
        ax.set_ylabel('Стоимость портфеля (₴)', fontsize=12)
        
        # Форматирование осей
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f'{x:,.0f}₴'))
        
        # Поворот меток X если нужно
        if len(timestamps) > 10:
            plt.xticks(rotation=45, ha='right')
        
        # Сетка и легенда
        ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
        ax.legend(loc='upper left', framealpha=0.9)
        
        # Настройка макета
        plt.tight_layout()
        
        # Сохраняем в буфер
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        buffer.seek(0)
        plt.close()
        
        return buffer
        
    except Exception as e:
        logging.error(f"Ошибка генерации графика: {e}")
        return None

async def update_background_charts():
    """Обновляет графики в фоне каждые 10 минут."""
    try:
        items = get_items_from_db()
        if not items:
            return
            
        # Получаем текущие цены и сохраняем значение портфеля
        total_value = 0
        for _, name, qty, buy_uah, buy_usd in items:
            try:
                # Пытаемся получить цену из кэша или API
                await fetch_marketcsgo_prices()
                price_usd = marketcsgo_prices_cache.get(name.lower())
                if price_usd:
                    total_value += price_usd * USD_TO_UAH * qty
                else:
                    # Fallback к цене закупки
                    total_value += buy_uah * qty
            except Exception as e:
                logging.warning(f"Ошибка получения цены для {name}: {e}")
                total_value += buy_uah * qty
        
        # Сохраняем значение в историю
        if total_value > 0:
            save_portfolio_value(total_value)
            logging.info(f"Фоновое обновление: стоимость портфеля {total_value:,.0f}₴")
        
        # Сохраняем снимок для анализа изменений
        save_portfolio_snapshot()
        
    except Exception as e:
        logging.error(f"Ошибка фонового обновления графиков: {e}")

async def keep_bot_alive():
    """Поддерживает бота активным, пингуя собственный URL."""
    try:
        # Выполняем простую операцию с базой данных
        with get_db_cursor() as (cursor, conn):
            cursor.execute("SELECT COUNT(*) FROM items")
            count = cursor.fetchone()[0]

        # Получаем домен из переменной окружения
        domain = os.getenv("REPLIT_DEV_DOMAIN")
        if domain:
            # Пингуем собственный URL
            async with aiohttp.ClientSession() as session:
                async with session.get(f'https://{domain}/', timeout=aiohttp.ClientTimeout(total=10)) as response:
                    logging.info(
                        f"Keep-alive: HTTP-запрос отправлен, статус: {response.status}"
                    )
        else:
            logging.warning("Keep-alive: REPLIT_DEV_DOMAIN не найден, пропускаем внешний пинг")

        # Обновляем кэш, если нужно
        if should_update_cache():
            await fetch_marketcsgo_prices()

        logging.info(f"Keep-alive: бот активен, предметов в БД: {count}")
    except Exception as e:
        logging.warning(f"Ошибка keep-alive: {e}")


def start_scheduled_jobs():
    """Запускает фоновые задачи для уведомлений."""
    scheduler.add_job(fetch_marketcsgo_prices, 'interval', minutes=30)
    scheduler.add_job(check_and_notify, CronTrigger(hour='8-23/4'))
    scheduler.add_job(check_price_alerts, CronTrigger(hour='*', minute='*/30'))
    # Добавляем новую систему проверки индивидуальных изменений цен каждые 30 минут
    scheduler.add_job(check_individual_price_changes, 'interval', minutes=30)
    # Добавляем keep-alive каждые 10 минут
    scheduler.add_job(keep_bot_alive, 'interval', minutes=10)
    # Добавляем фоновое обновление графиков каждые 10 минут
    scheduler.add_job(update_background_charts, 'interval', minutes=10)
    scheduler.start()
    logging.info("Фоновые задачи запущены.")


# --- ОСНОВНАЯ ФУНКЦИЯ ЗАПУСКА ---
async def main():
    """Запускает бота."""
    if not API_TOKEN:
        logging.error("TELEGRAM_BOT_TOKEN не найден в переменных окружения!")
        return

    init_db()
    start_scheduled_jobs()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
