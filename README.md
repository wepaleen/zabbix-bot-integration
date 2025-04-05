# Zabbix Telegram Bot

Телеграм бот для мониторинга событий Zabbix. Бот позволяет получать уведомления о проблемах, группировать их по узлам и важности, а также подтверждать события прямо из Telegram.

## Возможности

- Получение уведомлений о новых проблемах
- Группировка проблем по узлам
- Группировка по важности событий
- Подтверждение событий через кнопки в Telegram
- Автоматические напоминания о неподтвержденных событиях
- Защита доступа по ID администраторов

## Установка

### Способ 1: Локальная установка

1. Клонируйте репозиторий:
```bash
git clone <repository-url>
cd zabbix-bot-integration
```

2. Установите зависимости:
```bash
pip install -r requirements.txt
```

3. Создайте файл `.env` в корневой директории проекта со следующим содержимым:
```
BOT_TOKEN=your_telegram_bot_token
ADMIN_IDS=123456789,987654321  # ID администраторов через запятую
ZABBIX_URL=http://your-zabbix-server/api_jsonrpc.php
ZABBIX_USER=your_zabbix_username
ZABBIX_PASSWORD=your_zabbix_password
```

4. Запустите бота:
```bash
python main.py
```

### Способ 2: Запуск в Docker

1. Убедитесь, что у вас установлены Docker и Docker Compose

2. Создайте файл `.env` в корневой директории проекта со следующим содержимым:
```
BOT_TOKEN=your_telegram_bot_token
ADMIN_IDS=123456789,987654321  # ID администраторов через запятую
ZABBIX_URL=http://your-zabbix-server/api_jsonrpc.php
ZABBIX_USER=your_zabbix_username
ZABBIX_PASSWORD=your_zabbix_password
```

3. Соберите и запустите контейнер:
```bash
docker-compose up -d
```

4. Для просмотра логов:
```bash
docker-compose logs -f
```

5. Для остановки бота:
```bash
docker-compose down
```

## Использование

В Telegram доступны следующие команды:
- `/start` - начало работы с ботом
- `/set_duty` - установить дежурного смены


