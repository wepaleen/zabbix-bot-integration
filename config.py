import os
from dotenv import load_dotenv

load_dotenv()

# Telegram Bot settings
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_IDS = [int(id) for id in os.getenv('ADMIN_IDS', '').split(',') if id]

# Zabbix settings
ZABBIX_URL = os.getenv('ZABBIX_URL')
ZABBIX_USER = os.getenv('ZABBIX_USER')
ZABBIX_PASSWORD = os.getenv('ZABBIX_PASSWORD')

# Event settings
EVENT_CHECK_INTERVAL = 60  # seconds
EVENT_REMINDER_INTERVAL = 300  # seconds (5 minutes) 