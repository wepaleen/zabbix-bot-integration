import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import BOT_TOKEN, ADMIN_IDS, EVENT_CHECK_INTERVAL, EVENT_REMINDER_INTERVAL
from zabbix_client import ZabbixClient
import redis
import socket

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,  # Изменяем уровень на DEBUG для более подробного логирования
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('zabbix_bot.log')
    ]
)

# Настройка уровней логирования для разных модулей
logging.getLogger('aiogram').setLevel(logging.INFO)
logging.getLogger('pyzabbix').setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)

# Инициализация Redis клиента
redis_client = redis.Redis(
    host='zabbix-bot-redis',
    port=6379,
    db=0,
    decode_responses=True
)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Инициализация клиента Zabbix
zabbix = ZabbixClient()

# Словарь для хранения неподтвержденных событий
unacknowledged_events = {}

# Минимальный уровень важности для уведомлений (2 = Warning)
MIN_SEVERITY = 2

# Ключ для блокировки в Redis
LOCK_KEY = 'zabbix_bot_lock'

# Ключ для хранения состояния отправки событий
REALTIME_EVENTS_KEY = 'realtime_events_enabled'

def get_lock_value():
    """Получение значения блокировки"""
    return f"{socket.gethostname()}:{os.getpid()}"

def check_bot_state():
    """Проверка состояния бота через Redis"""
    try:
        lock_value = redis_client.get(LOCK_KEY)
        if lock_value:
            hostname, pid = lock_value.split(':')
            if hostname == socket.gethostname():
                try:
                    # Проверяем, существует ли процесс
                    os.kill(int(pid), 0)
                    return True
                except OSError:
                    # Если процесс не существует, удаляем блокировку
                    redis_client.delete(LOCK_KEY)
                    return False
        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке состояния бота: {e}")
        return False

def set_bot_state(state):
    """Установка состояния бота"""
    try:
        if state:
            redis_client.set(LOCK_KEY, get_lock_value(), ex=60)  # Блокировка на 60 секунд
        else:
            redis_client.delete(LOCK_KEY)
    except Exception as e:
        logger.error(f"Ошибка при установке состояния бота: {e}")

async def refresh_lock():
    """Периодическое обновление блокировки"""
    while True:
        try:
            if redis_client.get(LOCK_KEY) == get_lock_value():
                redis_client.set(LOCK_KEY, get_lock_value(), ex=60)
            await asyncio.sleep(30)  # Обновляем каждые 30 секунд
        except Exception as e:
            logger.error(f"Ошибка при обновлении блокировки: {e}")
            await asyncio.sleep(5)

def is_realtime_events_enabled():
    """Проверка включена ли отправка событий в реальном времени"""
    return redis_client.get(REALTIME_EVENTS_KEY) == '1'

def toggle_realtime_events():
    """Переключение состояния отправки событий в реальном времени"""
    current_state = is_realtime_events_enabled()
    new_state = '0' if current_state else '1'
    redis_client.set(REALTIME_EVENTS_KEY, new_state)
    return new_state == '1'

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Обработчик команды /start"""
    if message.from_user.id not in ADMIN_IDS:
        return
        
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Проблемы", callback_data="problems"),
            InlineKeyboardButton(text="⚠️ Неподтвержденные", callback_data="unacknowledged")
        ],
        [
            InlineKeyboardButton(text="✅ Подтвержденные", callback_data="acknowledged")
        ],
        [
            InlineKeyboardButton(text="👤 Установить дежурного", callback_data="set_duty")
        ],
        [
            InlineKeyboardButton(
                text="🔔 Уведомления: " + ("ВКЛ" if is_realtime_events_enabled() else "ВЫКЛ"),
                callback_data="toggle_realtime"
            )
        ]
    ])
    
    await message.answer(
        "👋 Привет! Я бот для мониторинга Zabbix.\n\n"
        "Доступные команды:\n"
        "/problems - показать все проблемы\n"
        "/unacknowledged - показать неподтвержденные события\n"
        "/acknowledged - показать подтвержденные события\n"
        "/set_duty Фамилия И.О. - установить текущего дежурного",
        reply_markup=keyboard
    )

@dp.message(Command("problems"))
async def cmd_problems(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    try:
        logger.info("Получение проблем из Zabbix...")
        problems = zabbix.get_problems(hours=2, min_severity=MIN_SEVERITY)
        
        if not problems:
            logger.info("Нет активных проблем за последние 2 часа")
            await message.answer("Нет активных проблем за последние 2 часа.")
            return
            
        for problem in problems:
            try:
                # Получаем информацию о хосте
                host_info = zabbix.zapi.host.get(
                    hostids=problem.get('hostid'),
                    output=['host']
                )
                if host_info:
                    host_name = host_info[0]['host']
                    severity = problem.get('severity', '0')
                    severity_name = zabbix.get_severity_name(severity)
                    event_time = datetime.fromtimestamp(int(problem.get('clock', 0))).strftime('%H:%M:%S %Y.%m.%d')
                    
                    message_text = (
                        f"Проблема: {problem.get('name', 'Нет описания')}\n"
                        f"Время возникновения: {event_time}\n"
                        f"Узел сети: {host_name}\n"
                        f"Важность: {severity_name}\n"
                        f"Комментарии:\n"
                    )
                    
                    # Добавляем комментарии, если они есть
                    if problem.get('comments'):
                        for comment in problem['comments']:
                            comment_time = datetime.fromtimestamp(int(comment.get('clock', 0))).strftime('%H:%M:%S %Y.%m.%d')
                            message_text += f"• {comment.get('message', 'Нет текста')} ({comment_time})\n"
                    
                    # Создание кнопок для подтверждения
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="✅ Подтвердить",
                                callback_data=f"ack_{problem['eventid']}"
                            ),
                            InlineKeyboardButton(
                                text="💬 Подтвердить с комментарием",
                                callback_data=f"ack_comment_{problem['eventid']}"
                            )
                        ]
                    ])
                    
                    await message.answer(message_text, reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Ошибка при обработке проблемы {problem.get('objectid')}: {e}", exc_info=True)
                continue

    except Exception as e:
        logger.error(f"Ошибка в cmd_problems: {e}", exc_info=True)
        await message.answer("Произошла ошибка при получении проблем. Попробуйте позже.")

@dp.message(Command("unacknowledged"))
async def cmd_unacknowledged(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    try:
        logger.info("Получение неподтвержденных событий...")
        events = zabbix.get_unacknowledged_events(hours=2, min_severity=MIN_SEVERITY)
        
        if not events:
            logger.info("Нет неподтвержденных событий за последние 2 часа")
            await message.answer("Нет неподтвержденных событий за последние 2 часа.")
            return
        
        for event in events:
            try:
                # Получаем информацию о хосте
                host_info = zabbix.zapi.host.get(
                    hostids=event.get('hostid'),
                    output=['host']
                )
                if host_info:
                    host_name = host_info[0]['host']
                    severity = event.get('severity', '0')
                    severity_name = zabbix.get_severity_name(severity)
                    event_time = datetime.fromtimestamp(int(event.get('clock', 0))).strftime('%H:%M:%S %Y.%m.%d')
                    
                    message_text = (
                        f"Проблема: {event.get('name', 'Нет описания')}\n"
                        f"Время возникновения: {event_time}\n"
                        f"Узел сети: {host_name}\n"
                        f"Важность: {severity_name}\n"
                        f"Комментарии:\n"
                    )
                    
                    # Добавляем комментарии, если они есть
                    if event.get('comments'):
                        for comment in event['comments']:
                            comment_time = datetime.fromtimestamp(int(comment.get('clock', 0))).strftime('%H:%M:%S %Y.%m.%d')
                            message_text += f"• {comment.get('message', 'Нет текста')} ({comment_time})\n"
                    
                    # Создание кнопок для подтверждения
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="✅ Подтвердить",
                                callback_data=f"ack_{event['eventid']}"
                            ),
                            InlineKeyboardButton(
                                text="💬 Подтвердить с комментарием",
                                callback_data=f"ack_comment_{event['eventid']}"
                            )
                        ]
                    ])
                    
                    await message.answer(message_text, reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Ошибка при обработке события {event.get('eventid')}: {e}", exc_info=True)
                continue

    except Exception as e:
        logger.error(f"Ошибка в cmd_unacknowledged: {e}", exc_info=True)
        await message.answer("Произошла ошибка при получении событий. Попробуйте позже.")

@dp.message(Command("set_duty"))
async def cmd_set_duty(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет доступа к этой функции.")
        return
    
    # Ожидаем формат: /set_duty Фамилия И.О.
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("Пожалуйста, используйте формат: /set_duty Фамилия И.О.")
        return
    
    # Устанавливаем текущего дежурного
    zabbix.current_duty_officer = parts[1]
    await message.answer(f"Текущий дежурный установлен: {zabbix.current_duty_officer}")

# Словарь для хранения состояний ожидания комментария
comment_states = {}

@dp.callback_query(lambda c: c.data.startswith('ack_'))
async def process_acknowledge(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("У вас нет доступа к этой функции.")
        return
    
    try:
        data = callback_query.data.split('_')
        event_id = data[-1]
        
        # Проверяем, установлен ли дежурный
        if not zabbix.current_duty_officer:
            await callback_query.answer("❌ Необходимо установить дежурного с помощью команды /set_duty")
            return
        
        if len(data) == 2:  # Простое подтверждение
            logger.info(f"Подтверждение события {event_id}")
            
            # Формируем сообщение для подтверждения
            confirm_message = f"Подтвердил дежурный {zabbix.current_duty_officer}"
            
            if zabbix.acknowledge_event(event_id, confirm_message):
                await callback_query.answer("Событие подтверждено!")
                
                # Создаем новую клавиатуру только с кнопкой добавления комментария
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="💬 Добавить комментарий",
                            callback_data=f"ack_comment_{event_id}"
                        )
                    ]
                ])
                
                # Обновляем сообщение и клавиатуру
                new_text = callback_query.message.text + f"\n✅ {confirm_message}"
                await callback_query.message.edit_text(new_text, reply_markup=keyboard)
            else:
                await callback_query.answer("Не удалось подтвердить событие. Попробуйте позже.")
                
        elif len(data) == 3 and data[1] == 'comment':  # Подтверждение с комментарием
            event_id = data[2]
            comment_states[callback_query.from_user.id] = event_id
            
            # Создаем новое сообщение с запросом комментария
            await callback_query.message.edit_text(
                callback_query.message.text + "\n\n💬 Пожалуйста, введите комментарий:",
                reply_markup=None
            )
            
    except Exception as e:
        logger.error(f"Ошибка в process_acknowledge: {e}", exc_info=True)
        await callback_query.answer("Произошла ошибка при подтверждении события.")

@dp.message()
async def handle_message(message: types.Message):
    """Обработчик сообщений с комментариями к событиям"""
    if message.from_user.id not in ADMIN_IDS:
        return
        
    # Проверяем, ожидается ли комментарий
    if message.from_user.id in comment_states:
        event_id = comment_states[message.from_user.id]
        try:
            # Проверяем, установлен ли дежурный
            if not zabbix.current_duty_officer:
                await message.answer("❌ Необходимо установить дежурного с помощью команды /set_duty")
                return
                
            # Формируем сообщение для подтверждения
            confirm_message = f"Дежурный {zabbix.current_duty_officer}: {message.text}"
            
            # Подтверждаем событие
            if zabbix.acknowledge_event(event_id, confirm_message):
                await message.answer("✅ Событие успешно подтверждено с комментарием")
            else:
                await message.answer("❌ Ошибка при подтверждении события")
                
        except Exception as e:
            logger.error(f"Ошибка при подтверждении события с комментарием: {e}", exc_info=True)
            await message.answer("❌ Произошла ошибка при подтверждении события")
            
        # Удаляем состояние ожидания комментария
        del comment_states[message.from_user.id]
        return
        
    # Проверяем, является ли сообщение ответом на сообщение с событием
    if message.reply_to_message and message.reply_to_message.text and ('🔗' in message.reply_to_message.text or '✅ Подтвердить' in message.reply_to_message.text or '💬 Подтвердить с комментарием' in message.reply_to_message.text):
        try:
            # Проверяем, установлен ли дежурный
            if not zabbix.current_duty_officer:
                await message.answer("❌ Необходимо установить дежурного с помощью команды /set_duty")
                return
                
            # Извлекаем event_id из URL в сообщении или из callback_data
            text = message.reply_to_message.text
            if '🔗' in text:
                url_start = text.find('🔗') + 1
                url = text[url_start:].strip()
                event_id = url.split('/')[-1]
            else:
                # Ищем event_id в callback_data кнопок
                for row in message.reply_to_message.reply_markup.inline_keyboard:
                    for button in row:
                        if button.callback_data.startswith('ack_'):
                            event_id = button.callback_data.split('_')[-1]
                            break
                    if event_id:
                        break
                if not event_id:
                    await message.answer("❌ Не удалось определить ID события")
                    return
            
            # Формируем сообщение для подтверждения
            if message.text:
                # Если есть комментарий пользователя
                confirm_message = f"Дежурный {zabbix.current_duty_officer}: {message.text}"
            else:
                # Если комментария нет
                confirm_message = f"Подтвердил дежурный {zabbix.current_duty_officer}"
            
            # Подтверждаем событие
            if zabbix.acknowledge_event(event_id, confirm_message):
                await message.answer("✅ Событие успешно подтверждено")
            else:
                await message.answer("❌ Ошибка при подтверждении события")
                
        except Exception as e:
            logger.error(f"Ошибка при подтверждении события с комментарием: {e}", exc_info=True)
            await message.answer("❌ Произошла ошибка при подтверждении события")

@dp.callback_query(lambda c: c.data == "acknowledged")
async def process_acknowledged_button(callback_query: types.CallbackQuery):
    """Обработчик нажатия кнопки подтвержденных событий"""
    if callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("У вас нет доступа к этой функции.")
        return
        
    try:
        await callback_query.answer("Получение подтвержденных событий...")
        
        # Получаем подтвержденные события
        events = zabbix.get_acknowledged_events(hours=2, min_severity=MIN_SEVERITY)
        
        if not events:
            await callback_query.message.answer("Нет подтвержденных событий за последние 2 часа")
            return
            
        # Группируем события по хостам
        host_events = zabbix.group_events_by_host(events)
        
        # Формируем сообщение
        response = "📋 Подтвержденные события (за последние 2 часа):\n\n"
        
        for host, host_events_list in host_events.items():
            for event in host_events_list:
                severity = zabbix.get_severity_name(event.get('severity', '0'))
                event_time = datetime.fromtimestamp(int(event.get('clock', 0))).strftime('%H:%M:%S %Y.%m.%d')
                
                response += f"Проблема: {event.get('name', 'Нет описания')}\n"
                response += f"Время возникновения: {event_time}\n"
                response += f"Узел сети: {host}\n"
                response += f"Важность: {severity}\n"
                
                # Добавляем комментарии, если они есть
                if event.get('comments'):
                    response += "Комментарии:\n"
                    for comment in event['comments']:
                        # Преобразуем timestamp в datetime
                        comment_time = datetime.fromtimestamp(int(comment['clock']))
                        # Форматируем время в нужный формат
                        formatted_time = comment_time.strftime("%d.%m.%Y %H:%M")
                        response += f"• {formatted_time} - {comment['message']}\n"
                else:
                    response += "Комментарии:\n"
                
                # Добавляем кнопку для нового комментария
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="💬 Добавить комментарий",
                            callback_data=f"ack_comment_{event['eventid']}"
                        )
                    ]
                ])
                
                # Отправляем сообщение с событием
                await callback_query.message.answer(response, reply_markup=keyboard)
                response = ""  # Очищаем response для следующего события
        
    except Exception as e:
        logger.error(f"Ошибка в process_acknowledged_button: {e}", exc_info=True)
        await callback_query.message.answer("Произошла ошибка при получении подтвержденных событий")

@dp.callback_query(lambda c: c.data.startswith('ack_comment_'))
async def process_add_comment(callback_query: types.CallbackQuery):
    """Обработчик добавления комментария к подтвержденному событию"""
    if callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("У вас нет доступа к этой функции.")
        return
        
    try:
        event_id = callback_query.data.split('_')[-1]
        
        # Проверяем, установлен ли дежурный
        if not zabbix.current_duty_officer:
            await callback_query.answer("❌ Необходимо установить дежурного с помощью команды /set_duty")
            return
            
        # Сохраняем состояние ожидания комментария
        comment_states[callback_query.from_user.id] = event_id
        
        # Обновляем сообщение с запросом комментария
        await callback_query.message.edit_text(
            callback_query.message.text + "\n\n💬 Пожалуйста, введите комментарий:",
            reply_markup=None
        )
        
    except Exception as e:
        logger.error(f"Ошибка в process_add_comment: {e}", exc_info=True)
        await callback_query.answer("Произошла ошибка при добавлении комментария")

@dp.callback_query(lambda c: c.data == "toggle_realtime")
async def process_toggle_realtime(callback_query: types.CallbackQuery):
    """Обработчик переключения отправки событий в реальном времени"""
    if callback_query.from_user.id not in ADMIN_IDS:
        await callback_query.answer("У вас нет доступа к этой функции.")
        return
        
    try:
        is_enabled = toggle_realtime_events()
        status = "включены" if is_enabled else "выключены"
        await callback_query.answer(f"Уведомления {status}")
        
        # Обновляем текст кнопки
        keyboard = callback_query.message.reply_markup
        for row in keyboard.inline_keyboard:
            for button in row:
                if button.callback_data == "toggle_realtime":
                    button.text = f"🔔 Уведомления: {'ВКЛ' if is_enabled else 'ВЫКЛ'}"
                    break
            if button.callback_data == "toggle_realtime":
                break
                
        await callback_query.message.edit_reply_markup(reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка при переключении уведомлений: {e}", exc_info=True)
        await callback_query.answer("Произошла ошибка при переключении уведомлений")

async def check_events():
    """Периодическая проверка событий"""
    while True:
        try:
            # Проверяем, включены ли уведомления
            if not is_realtime_events_enabled():
                logger.info("Уведомления выключены, пропускаем проверку")
                await asyncio.sleep(EVENT_CHECK_INTERVAL)
                continue
                
            logger.info("Проверка новых событий...")
            events = zabbix.get_unacknowledged_events(hours=12, min_severity=MIN_SEVERITY)
            logger.info(f"Получено {len(events)} неподтвержденных событий")
            
            for event in events:
                event_id = event['eventid']
                if event_id not in unacknowledged_events:
                    try:
                        # Получаем информацию о хосте
                        host_info = zabbix.zapi.host.get(
                            hostids=event.get('hostid'),
                            output=['host']
                        )
                        if host_info:
                            host_name = host_info[0]['host']
                            # Новое событие
                            unacknowledged_events[event_id] = {
                                'first_seen': datetime.now(),
                                'last_reminder': datetime.now()
                            }
                            
                            logger.info(f"Отправка уведомления о новом событии {event_id} для хоста {host_name}")
                            
                            # Создаем сообщение с информацией о событии
                            message_text = (
                                f"🔔 Новое событие!\n"
                                f"Хост: {host_name}\n"
                                f"Описание: {event.get('name', 'Нет описания')}\n"
                                f"Уровень важности: {zabbix.get_severity_name(event.get('severity', '0'))}"
                            )
                            
                            # Создаем клавиатуру с кнопками
                            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                                [
                                    InlineKeyboardButton(
                                        text="✅ Подтвердить",
                                        callback_data=f"ack_{event_id}"
                                    ),
                                    InlineKeyboardButton(
                                        text="💬 Подтвердить с комментарием",
                                        callback_data=f"ack_comment_{event_id}"
                                    )
                                ]
                            ])
                            
                            # Отправка уведомления всем админам
                            for admin_id in ADMIN_IDS:
                                try:
                                    await bot.send_message(
                                        admin_id,
                                        message_text,
                                        reply_markup=keyboard
                                    )
                                    logger.info(f"Уведомление отправлено админу {admin_id}")
                                except Exception as e:
                                    logger.error(f"Ошибка при отправке уведомления админу {admin_id}: {e}")
                    except Exception as e:
                        logger.error(f"Ошибка при обработке нового события {event_id}: {e}", exc_info=True)
                else:
                    # Проверка необходимости напоминания
                    last_reminder = unacknowledged_events[event_id]['last_reminder']
                    if datetime.now() - last_reminder > timedelta(seconds=EVENT_REMINDER_INTERVAL):
                        try:
                            host_info = zabbix.zapi.host.get(
                                hostids=event.get('hostid'),
                                output=['host']
                            )
                            if host_info:
                                host_name = host_info[0]['host']
                                logger.info(f"Отправка напоминания о событии {event_id} для хоста {host_name}")
                                # Отправка напоминания
                                for admin_id in ADMIN_IDS:
                                    try:
                                        await bot.send_message(
                                            admin_id,
                                            f"⚠️ Напоминание о неподтвержденном событии!\n"
                                            f"Хост: {host_name}\n"
                                            f"Описание: {event.get('name', 'Нет описания')}"
                                        )
                                        logger.info(f"Напоминание отправлено админу {admin_id}")
                                    except Exception as e:
                                        logger.error(f"Ошибка при отправке напоминания админу {admin_id}: {e}")
                                unacknowledged_events[event_id]['last_reminder'] = datetime.now()
                        except Exception as e:
                            logger.error(f"Ошибка при отправке напоминания для события {event_id}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Ошибка при проверке событий: {e}", exc_info=True)
        
        await asyncio.sleep(EVENT_CHECK_INTERVAL)

# Функция для проверки, не запущен ли уже бот
def is_bot_running():
    try:
        # Проверяем наличие ключа в Redis
        return redis_client.exists('zabbix_bot_running')
    except Exception as e:
        logger.error(f"Ошибка при проверке состояния бота: {e}")
        return False

# Функция для установки флага запуска бота
def set_bot_running():
    try:
        # Устанавливаем ключ с временем жизни 30 секунд
        redis_client.setex('zabbix_bot_running', 30, '1')
        return True
    except Exception as e:
        logger.error(f"Ошибка при установке состояния бота: {e}")
        return False

# Функция для снятия флага запуска бота
def clear_bot_running():
    try:
        redis_client.delete('zabbix_bot_running')
        return True
    except Exception as e:
        logger.error(f"Ошибка при снятии состояния бота: {e}")
        return False

# Обновляем текст помощи
help_text = """Доступные команды:
/problems - показать все проблемы
/unacknowledged - показать неподтвержденные события
/acknowledged - показать подтвержденные события
/set_duty Фамилия И.О. - установить текущего дежурного"""

async def main():
    try:
        # Проверяем, не запущен ли уже бот
        if is_bot_running():
            logger.error("Бот уже запущен. Завершение работы.")
            return
            
        # Устанавливаем флаг запуска
        if not set_bot_running():
            logger.error("Не удалось установить флаг запуска бота. Завершение работы.")
            return
            
        logger.info("Запуск бота...")
        
        # Устанавливаем уведомления включенными по умолчанию
        if not redis_client.get(REALTIME_EVENTS_KEY):
            redis_client.set(REALTIME_EVENTS_KEY, '1')
            logger.info("Уведомления включены по умолчанию")
        
        # Запускаем проверку событий в отдельной задаче
        asyncio.create_task(check_events())
        
        # Запускаем бота
        await dp.start_polling(bot)
        
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}", exc_info=True)
    finally:
        # Снимаем флаг запуска
        clear_bot_running()
        logger.info("Бот остановлен")

if __name__ == '__main__':
    asyncio.run(main()) 