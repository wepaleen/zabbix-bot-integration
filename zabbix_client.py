from pyzabbix import ZabbixAPI
from config import ZABBIX_URL, ZABBIX_USER, ZABBIX_PASSWORD
import logging
from datetime import datetime, timedelta
from collections import defaultdict
import pytz

logger = logging.getLogger(__name__)

# Список тегов для фильтрации
INCLUDED_TAGS = [
    'scope: capacity', 'scope: performance', 'scope: notice', 'scope: security',
    'scope: availability', 'component: fra', 'oracle: blocks', 'transaction',
    'archive_log_depth', 'vimis: availability', 'remd: os', 'remd: job_status',
    'remd: all_success', 'remd: availability', 'epgu: session', 'epgu: availability',
    'remdfer: notice'
]

# Переменная для хранения текущего дежурного
current_duty_officer = None

# Московское время (UTC+3)
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

def get_moscow_timestamp():
    """Получение текущего timestamp в московском времени"""
    return int(datetime.now(MOSCOW_TZ).timestamp())

def get_moscow_time_from_hours(hours):
    """Получение timestamp для времени X часов назад в московском времени"""
    return int((datetime.now(MOSCOW_TZ) - timedelta(hours=hours)).timestamp())

class ZabbixClient:
    def __init__(self):
        logger.info(f"Инициализация подключения к Zabbix API: {ZABBIX_URL}")
        self.zapi = ZabbixAPI(ZABBIX_URL)
        self.current_duty_officer = None  # Инициализируем переменную
        try:
            self.zapi.login(ZABBIX_USER, ZABBIX_PASSWORD)
            logger.info("Успешное подключение к Zabbix API")
        except Exception as e:
            logger.error(f"Ошибка при подключении к Zabbix API: {e}", exc_info=True)
            raise

    def get_severity_name(self, severity):
        """Получение названия уровня важности"""
        severity_map = {
            '0': '🔵 Не классифицировано',
            '1': '⚪ Информация',
            '2': '🟡 Предупреждение',
            '3': '🟠 Средняя',
            '4': '🔴 Высокая',
            '5': '⚫ Чрезвычайная'
        }
        return severity_map.get(severity, '❓ Неизвестно')

    def filter_by_tags(self, events):
        """Фильтрация событий по тегам"""
        filtered_events = []
        for event in events:
            try:
                # Получаем теги события
                event_tags = self.zapi.event.get(
                    output=['eventid', 'tags'],
                    eventids=[event['eventid']],
                    selectTags=['tag', 'value']
                )
                
                if event_tags and event_tags[0].get('tags'):
                    # Логируем теги события для отладки
                    event_tag_names = [f"{tag['tag']}: {tag['value']}" for tag in event_tags[0]['tags']]
                    logger.info(f"Теги события {event['eventid']}: {event_tag_names}")
                    
                    # Проверяем, есть ли у события хотя бы один тег из списка включений
                    has_included_tag = False
                    for event_tag in event_tag_names:
                        # Нормализуем теги (убираем лишние пробелы и приводим к нижнему регистру)
                        normalized_event_tag = event_tag.strip().lower()
                        for included_tag in INCLUDED_TAGS:
                            normalized_included_tag = included_tag.strip().lower()
                            if normalized_event_tag == normalized_included_tag:
                                has_included_tag = True
                                logger.info(f"Событие {event['eventid']} включено (тег: {event_tag})")
                                break
                        if has_included_tag:
                            break
                    
                    if has_included_tag:
                        filtered_events.append(event)
                    else:
                        logger.info(f"Событие {event['eventid']} пропущено (нет тегов из списка включений)")
                else:
                    logger.info(f"Событие {event['eventid']} пропущено (нет тегов)")
                    continue
                    
            except Exception as e:
                logger.error(f"Ошибка при фильтрации события {event.get('eventid')}: {e}", exc_info=True)
                continue
        
        logger.info(f"Отфильтровано {len(filtered_events)} событий из {len(events)}")
        return filtered_events

    def group_problems_by_host(self, problems):
        """Группировка проблем по хостам"""
        host_problems = defaultdict(list)
        for problem in problems:
            try:
                host_info = self.zapi.host.get(
                    hostids=problem.get('hostid'),
                    output=['host', 'name']
                )
                if host_info:
                    host_name = host_info[0]['host']
                    host_problems[host_name].append(problem)
                else:
                    logger.warning(f"Не удалось получить информацию о хосте для проблемы {problem.get('objectid')}")
            except Exception as e:
                logger.error(f"Ошибка при получении информации о хосте: {e}")
                continue
        return dict(host_problems)

    def group_problems_by_severity(self, problems):
        """Группировка проблем по уровню важности"""
        severity_problems = defaultdict(list)
        for problem in problems:
            severity = problem.get('severity', '0')
            severity_name = self.get_severity_name(severity)
            severity_problems[severity_name].append(problem)
        return dict(severity_problems)

    def group_events_by_host(self, events):
        """Группировка событий по хостам"""
        host_events = defaultdict(list)
        for event in events:
            try:
                host_info = self.zapi.host.get(
                    hostids=event.get('hostid'),
                    output=['host', 'name']
                )
                if host_info:
                    host_name = host_info[0]['host']
                    host_events[host_name].append(event)
                else:
                    logger.warning(f"Не удалось получить информацию о хосте для события {event.get('eventid')}")
            except Exception as e:
                logger.error(f"Ошибка при получении информации о хосте: {e}")
                continue
        return dict(host_events)

    def group_events_by_severity(self, events):
        """Группировка событий по уровню важности"""
        severity_events = defaultdict(list)
        for event in events:
            severity = event.get('severity', '0')
            severity_name = self.get_severity_name(severity)
            severity_events[severity_name].append(event)
        return dict(severity_events)

    def get_event_comments(self, event_id: str) -> list:
        """
        Получение комментариев к событию
        :param event_id: ID события
        :return: список комментариев
        """
        try:
            logger.info(f"Запрос комментариев для события {event_id}")
            
            # Получаем проблему с комментариями
            problems = self.zapi.problem.get(
                output='extend',
                selectAcknowledges='extend',
                eventids=event_id,
                sortfield=['eventid'],
                sortorder='DESC'
            )
            
            logger.info(f"Получен ответ от API для события {event_id}: {problems}")
            
            if problems and problems[0].get('acknowledges'):
                logger.info(f"Получено {len(problems[0]['acknowledges'])} комментариев для события {event_id}")
                for ack in problems[0]['acknowledges']:
                    logger.info(f"Комментарий: {ack.get('message')} ({ack.get('clock')})")
                return problems[0]['acknowledges']
            logger.info(f"Нет комментариев для события {event_id}")
            return []
        except Exception as e:
            logger.error(f"Ошибка при получении комментариев к событию {event_id}: {e}", exc_info=True)
            return []

    def get_problems(self, hours=2, min_severity=2):
        """
        Получение активных проблем
        :param hours: количество часов для фильтрации
        :param min_severity: минимальный уровень важности
        :return: список проблем
        """
        try:
            logger.info(f"Запрос активных проблем за последние {hours} часов с уровнем важности >= {min_severity}")
            
            # Получаем проблемы с учетом московского времени
            problems = self.zapi.problem.get(
                output='extend',
                selectAcknowledges='extend',
                selectTags='extend',
                recent=True,
                sortfield=['eventid'],
                sortorder='DESC',
                time_from=get_moscow_time_from_hours(hours),
                severity_gte=min_severity
            )
            
            logger.info(f"Получено {len(problems)} проблем")
            
            # Фильтруем проблемы по тегам
            filtered_problems = []
            for problem in problems:
                try:
                    # Получаем теги проблемы
                    problem_tags = self.zapi.event.get(
                        output=['eventid', 'tags'],
                        eventids=[problem['eventid']],
                        selectTags=['tag', 'value']
                    )
                    
                    if problem_tags and problem_tags[0].get('tags'):
                        # Логируем теги проблемы для отладки
                        problem_tag_names = [f"{tag['tag']}: {tag['value']}" for tag in problem_tags[0]['tags']]
                        logger.info(f"Теги проблемы {problem['eventid']}: {problem_tag_names}")
                        
                        # Проверяем, есть ли у проблемы хотя бы один тег из списка включений
                        has_included_tag = False
                        for problem_tag in problem_tag_names:
                            # Нормализуем теги (убираем лишние пробелы и приводим к нижнему регистру)
                            normalized_problem_tag = problem_tag.strip().lower()
                            for included_tag in INCLUDED_TAGS:
                                normalized_included_tag = included_tag.strip().lower()
                                if normalized_problem_tag == normalized_included_tag:
                                    has_included_tag = True
                                    logger.info(f"Проблема {problem['eventid']} включена (тег: {problem_tag})")
                                    break
                            if has_included_tag:
                                break
                        
                        if has_included_tag:
                            filtered_problems.append(problem)
                        else:
                            logger.info(f"Проблема {problem['eventid']} пропущена (нет тегов из списка включений)")
                    else:
                        logger.info(f"Проблема {problem['eventid']} пропущена (нет тегов)")
                        continue
                        
                except Exception as e:
                    logger.error(f"Ошибка при фильтрации проблемы {problem.get('eventid')}: {e}", exc_info=True)
                    continue
            
            logger.info(f"Отфильтровано {len(filtered_problems)} проблем из {len(problems)}")
            return filtered_problems
            
        except Exception as e:
            logger.error(f"Ошибка при получении проблем: {e}", exc_info=True)
            return []

    def get_unacknowledged_events(self, hours=2, min_severity=1):
        """
        Получение неподтвержденных событий
        :param hours: количество часов для фильтрации
        :param min_severity: минимальный уровень важности (1 - информация)
        :return: список событий
        """
        try:
            logger.info(f"Запрос неподтвержденных событий за последние {hours} часов с уровнем важности >= {min_severity}")
            
            # Получаем проблемы с учетом московского времени
            problems = self.zapi.problem.get(
                output='extend',
                selectAcknowledges='extend',
                selectTags='extend',
                recent=True,
                acknowledged=False,  # Только неподтвержденные
                sortfield=['eventid'],
                sortorder='DESC',
                time_from=get_moscow_time_from_hours(hours),
                severity_min=min_severity,  # Минимальная важность - Информация (1)
                evaltype=2,  # Логическое ИЛИ между условиями
                tags=[
                    {'tag': 'scope', 'value': 'capacity', 'operator': 0},  # equals
                    {'tag': 'scope', 'value': 'performance', 'operator': 0},
                    {'tag': 'scope', 'value': 'notice', 'operator': 0},
                    {'tag': 'scope', 'value': 'security', 'operator': 0},
                    {'tag': 'scope', 'value': 'availability', 'operator': 0},
                    {'tag': 'component', 'value': 'fra', 'operator': 0},
                    {'tag': 'oracle', 'value': 'blocks', 'operator': 0},
                    {'tag': 'transaction', 'operator': 2},  # exists
                    {'tag': 'archive_log_depth', 'operator': 2},
                    {'tag': 'vimis', 'value': 'availability', 'operator': 0},
                    {'tag': 'remd', 'value': 'os', 'operator': 0},
                    {'tag': 'remd', 'value': 'job_status', 'operator': 0},
                    {'tag': 'remd', 'value': 'all_success', 'operator': 0},
                    {'tag': 'remd', 'value': 'availability', 'operator': 0},
                    {'tag': 'epgu', 'value': 'session', 'operator': 0},
                    {'tag': 'epgu', 'value': 'availability', 'operator': 0},
                    {'tag': 'remdfer', 'value': 'notice', 'operator': 0}
                ],
                selectSuppressionData='extend',  # Получаем данные о подавлении
                selectInventory='extend',  # Получаем данные инвентаризации
                selectHosts=['host', 'name', 'hostid'],  # Получаем информацию о хостах
                expandDescription=True,  # Раскрываем макросы в описании
                expandComment=True,  # Раскрываем макросы в комментариях
                expandExpression=True  # Раскрываем макросы в выражениях
            )
            
            logger.info(f"Получено {len(problems)} неподтвержденных проблем")
            return problems
            
        except Exception as e:
            logger.error(f"Ошибка при получении неподтвержденных событий: {e}", exc_info=True)
            return []

    def get_recent_problems(self, hours=2, min_severity=2):
        """
        Получение недавних проблем
        :param hours: количество часов для фильтрации
        :param min_severity: минимальный уровень важности
        :return: список проблем
        """
        try:
            logger.info(f"Запрос недавних проблем за последние {hours} часов с уровнем важности >= {min_severity}")
            
            # Получаем проблемы с учетом московского времени
            problems = self.zapi.problem.get(
                output='extend',
                selectAcknowledges='extend',
                selectTags='extend',
                recent=True,
                sortfield=['eventid'],
                sortorder='DESC',
                time_from=get_moscow_time_from_hours(hours),
                severity_gte=min_severity
            )
            
            logger.info(f"Получено {len(problems)} проблем")
            
            # Фильтруем проблемы по тегам
            filtered_problems = []
            for problem in problems:
                try:
                    # Получаем теги проблемы
                    problem_tags = self.zapi.event.get(
                        output=['eventid', 'tags'],
                        eventids=[problem['eventid']],
                        selectTags=['tag', 'value']
                    )
                    
                    if problem_tags and problem_tags[0].get('tags'):
                        # Логируем теги проблемы для отладки
                        problem_tag_names = [f"{tag['tag']}: {tag['value']}" for tag in problem_tags[0]['tags']]
                        logger.info(f"Теги проблемы {problem['eventid']}: {problem_tag_names}")
                        
                        # Проверяем, есть ли у проблемы хотя бы один тег из списка включений
                        has_included_tag = False
                        for problem_tag in problem_tag_names:
                            # Нормализуем теги (убираем лишние пробелы и приводим к нижнему регистру)
                            normalized_problem_tag = problem_tag.strip().lower()
                            for included_tag in INCLUDED_TAGS:
                                normalized_included_tag = included_tag.strip().lower()
                                if normalized_problem_tag == normalized_included_tag:
                                    has_included_tag = True
                                    logger.info(f"Проблема {problem['eventid']} включена (тег: {problem_tag})")
                                    break
                            if has_included_tag:
                                break
                        
                        if has_included_tag:
                            filtered_problems.append(problem)
                        else:
                            logger.info(f"Проблема {problem['eventid']} пропущена (нет тегов из списка включений)")
                    else:
                        logger.info(f"Проблема {problem['eventid']} пропущена (нет тегов)")
                        continue
                        
                except Exception as e:
                    logger.error(f"Ошибка при фильтрации проблемы {problem.get('eventid')}: {e}", exc_info=True)
                    continue
            
            logger.info(f"Отфильтровано {len(filtered_problems)} проблем из {len(problems)}")
            return filtered_problems
            
        except Exception as e:
            logger.error(f"Ошибка при получении недавних проблем: {e}", exc_info=True)
            return []

    def acknowledge_event(self, event_id: str, message: str = "") -> bool:
        """
        Подтверждение события в Zabbix
        
        Args:
            event_id: ID события
            message: Сообщение для подтверждения
            
        Returns:
            bool: True если подтверждение успешно, False в противном случае
        """
        try:
            # Проверяем, не подтверждено ли уже событие
            event = self.zapi.event.get(
                eventids=event_id,
                output=['eventid', 'acknowledged']
            )
            
            if not event:
                logger.error(f"Событие {event_id} не найдено")
                return False
                
            if event[0]['acknowledged'] == '1':
                logger.info(f"Событие {event_id} уже подтверждено")
                # Добавляем только комментарий, если он указан
                if message:
                    self.zapi.event.acknowledge(
                        eventids=[event_id],
                        action=6,  # 6 = close problem
                        message=message
                    )
                return True
            
            # Если событие не подтверждено, выполняем полное подтверждение
            self.zapi.event.acknowledge(
                eventids=[event_id],
                action=6,  # 6 = close problem
                message=message
            )
            logger.info(f"Событие {event_id} успешно подтверждено")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при подтверждении события {event_id}: {e}", exc_info=True)
            return False

    def get_host_problems(self, host_id, hours=12, min_severity=2):
        """
        Получение проблем для конкретного хоста
        :param host_id: ID хоста
        :param hours: Количество часов для фильтрации (по умолчанию 12)
        :param min_severity: Минимальный уровень важности (0-5)
        """
        try:
            time_from = int((datetime.now() - timedelta(hours=hours)).timestamp())
            logger.info(f"Запрос проблем для хоста {host_id} (за последние {hours} часов, min_severity={min_severity})")
            
            problems = self.zapi.problem.get(
                output='extend',
                selectHosts=['host'],
                hostids=[host_id],
                time_from=time_from,
                sortfield=['eventid'],
                sortorder='DESC'
            )
            
            filtered_problems = [
                problem for problem in problems 
                if int(problem.get('severity', '0')) >= min_severity
            ]
            
            logger.info(f"Получено {len(problems)} проблем для хоста {host_id}, отфильтровано {len(filtered_problems)} по важности")
            return filtered_problems
        except Exception as e:
            logger.error(f"Ошибка при получении проблем для хоста {host_id}: {e}", exc_info=True)
            return []

    def get_acknowledged_events(self, hours=2, min_severity=2):
        """
        Получение подтвержденных событий
        :param hours: Количество часов для фильтрации (по умолчанию 2)
        :param min_severity: Минимальный уровень важности (0-5)
        """
        try:
            # Вычисляем timestamp для фильтрации по времени
            time_from = int((datetime.now() - timedelta(hours=hours)).timestamp())
            logger.info(f"Запрос подтвержденных событий (за последние {hours} часов, min_severity={min_severity})")
            
            # Получаем подтвержденные события
            events = self.zapi.event.get(
                output='extend',
                selectHosts=['host'],
                acknowledged=True,
                time_from=time_from,
                sortfield=['eventid'],
                sortorder='DESC'
            )
            
            logger.info(f"Получено {len(events)} событий из Zabbix")
            
            # Фильтруем события по важности
            filtered_events = [
                event for event in events 
                if int(event.get('severity', '0')) >= min_severity
            ]
            
            logger.info(f"Отфильтровано {len(filtered_events)} событий по важности")
            
            # Добавляем hostid к событиям, если его нет
            for event in filtered_events:
                if 'hostid' not in event:
                    logger.debug(f"Получение hostid для события {event.get('eventid')}")
                    # Получаем связанный хост
                    host = self.zapi.host.get(
                        output=['hostid'],
                        hostids=[event['hosts'][0]['hostid']],
                        limit=1
                    )
                    if host:
                        event['hostid'] = host[0]['hostid']
                        logger.debug(f"Найден hostid: {host[0]['hostid']}")
                    else:
                        logger.warning(f"Не удалось найти хост для события {event.get('eventid')}")
                
                # Добавляем комментарии к событию
                comments = self.get_event_comments(event['eventid'])
                event['comments'] = comments
                logger.debug(f"Добавлено {len(comments)} комментариев к событию {event['eventid']}")
            
            return filtered_events
        except Exception as e:
            logger.error(f"Ошибка при получении подтвержденных событий: {e}", exc_info=True)
            return [] 