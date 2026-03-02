#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Парсер расписания НХТК с интеграцией Yandex Database (YDB)
Проверяет изменения перед отправкой в YDB
"""

import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict
import time
import os
import hashlib

# === Импорт для YDB ===
try:
    import ydb
    from ydb.query_pool import QuerySessionPool

    YDB_AVAILABLE = True
except ImportError:
    YDB_AVAILABLE = False
    print("⚠️ Библиотека ydb не установлена. Установите: pip install ydb")


class YDBClient:
    """Клиент для работы с Yandex Database"""

    def __init__(self):
        self.endpoint = os.getenv("YDB_ENDPOINT", "grpcs://ydb.serverless.yandexcloud.net:2135")
        self.database = os.getenv("YDB_DATABASE", "")
        self.token = os.getenv("YDB_TOKEN")
        self.driver = None
        self.pool = None

    def connect(self) -> bool:
        """Инициализация драйвера и пула сессий"""
        if not YDB_AVAILABLE:
            return False

        if not self.database:
            print("❌ Не указан YDB_DATABASE в переменных окружения")
            return False

        try:
            # Настройка драйвера
            driver_config = ydb.DriverConfig(
                endpoint=self.endpoint,
                database=self.database,
                credentials=(
                    ydb.AccessTokenCredentials(self.token) if self.token
                    else ydb.credentials_from_env_variables()
                ),
                root_certificates=ydb.load_ydb_root_certificate(),
            )
            self.driver = ydb.Driver(driver_config)
            self.driver.wait(timeout=10)

            # Пул сессий для выполнения запросов
            self.pool = QuerySessionPool(self.driver)
            print("✅ Подключение к YDB успешно")
            return True
        except Exception as e:
            print(f"❌ Ошибка подключения к YDB: {e}")
            return False

    def close(self):
        """Закрытие подключения"""
        if self.pool:
            try:
                self.pool.stop()
            except:
                pass
        if self.driver:
            try:
                self.driver.stop()
            except:
                pass

    def get_last_hash(self, group_code: str) -> Optional[str]:
        """Получение последнего хэша для группы"""
        if not self.pool:
            return None

        query = """
        DECLARE $group_code AS Utf8;
        SELECT data_hash FROM `schedule_items`
        WHERE group_code = $group_code
        ORDER BY parsed_at DESC
        LIMIT 1;
        """

        try:
            def query_callback(session: ydb.Session):
                return session.execute_scheme(query, {"$group_code": group_code})

            result = self.pool.retry_operation_sync(query_callback)
            if result and result.result_sets and result.result_sets[0].rows:
                return result.result_sets[0].rows[0].data_hash
            return None
        except Exception as e:
            print(f"⚠️ Ошибка получения хэша из YDB: {e}")
            return None

    def upsert_schedule_items(self, items: List[Dict]) -> bool:
        """Массовая вставка/обновление записей через UPSERT"""
        if not self.pool or not items:
            return False

        # Формируем VALUES для UPSERT с экранированием строк
        values = []
        for item in items:
            # Генерируем уникальный ID
            item_id = f"{item['group_code']}_{item['day']}_{item['lesson_number'] or 0}_{item['data_hash'][:8]}"

            # Экранируем строковые значения для SQL
            def escape_sql(s):
                if s is None:
                    return "NULL"
                return "'" + str(s).replace("'", "''") + "'"

            values.append(f"""(
                {escape_sql(item_id)},
                {escape_sql(item['group_code'])},
                {escape_sql(item.get('period', ''))},
                {escape_sql(item.get('source_url', ''))},
                {escape_sql(item['day'])},
                {item['lesson_number'] if item['lesson_number'] is not None else 'NULL'},
                {escape_sql(item.get('time', ''))},
                {escape_sql(item.get('subject', ''))},
                {escape_sql(item.get('subject_url', ''))},
                {escape_sql(item.get('teacher', ''))},
                {escape_sql(item.get('teacher_url', ''))},
                {escape_sql(item.get('room', ''))},
                {escape_sql(item.get('room_url', ''))},
                {escape_sql(item.get('subgroup', ''))},
                CurrentUtcTimestamp(),
                {escape_sql(item['data_hash'])}
            )""")

        query = f"""
        UPSERT INTO `schedule_items` (
            id, group_code, period, source_url, day, lesson_number, time,
            subject, subject_url, teacher, teacher_url, room, room_url,
            subgroup, parsed_at, data_hash
        ) VALUES {','.join(values)};
        """

        try:
            def query_callback(session: ydb.Session):
                return session.execute_scheme(query)

            self.pool.retry_operation_sync(query_callback)
            print(f"✅ Загружено {len(items)} записей в YDB")
            return True
        except Exception as e:
            print(f"❌ Ошибка UPSERT в YDB: {e}")
            return False


class NHTKLiveParser:
    def __init__(self):
        self.base_url = "https://расписание.нхтк.рф"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive'
        })
        self.ydb_client = YDBClient()

    def fetch_page(self, url: str) -> Optional[str]:
        """Получение HTML страницы"""
        try:
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"❌ Ошибка загрузки: {e}")
            return None

    def parse_schedule(self, html: str, source_url: str) -> Dict:
        """Парсинг расписания из HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        schedule_data = {
            "metadata": {
                "source_url": source_url,
                "parse_date": datetime.now().isoformat(),
                "group": "",
                "period": ""
            },
            "schedule": []
        }

        # Извлечение группы
        group_text = soup.find(string=re.compile(r'Группа\s+[\d\.п]+'))
        if group_text:
            schedule_data["metadata"]["group"] = group_text.strip().replace('Группа', '').strip()

        # Извлечение периода
        period_patterns = [
            r'Расписание занятий.*?\d{4}\s*г\.?',
            r'\d+\s+\w+\s*—\s*\d+\s+\w+\s+\d{4}'
        ]
        for text in soup.find_all(string=True):
            for pattern in period_patterns:
                if re.search(pattern, str(text), re.IGNORECASE):
                    schedule_data["metadata"]["period"] = text.strip()
                    break

        schedule_data["schedule"] = self._parse_table(soup)
        return schedule_data

    def _parse_table(self, soup: BeautifulSoup) -> List[Dict]:
        lessons = []
        current_day = None

        for row in soup.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue

            cell_texts = [cell.get_text(strip=True) for cell in cells]
            full_text = ' '.join(cell_texts)

            day_match = re.match(
                r'^(Понедельник|Вторник|Среда|Четверг|Пятница|Суббота|Воскресенье),\s+\d+\s+\w+',
                full_text
            )
            if day_match:
                current_day = day_match.group(0)
                continue

            if any(kw in full_text for kw in ['Время', 'Предмет', 'Преподаватель', 'Ауд.', 'Препод.']):
                continue

            if current_day and len(cells) >= 4:
                lesson = self._parse_lesson_row(cells, current_day)
                if lesson:
                    lessons.append(lesson)
        return lessons

    def _parse_lesson_row(self, cells, day: str) -> Optional[Dict]:
        try:
            lesson = {
                "day": day, "lesson_number": "", "time": "", "subject": "",
                "subject_url": "", "teacher": "", "teacher_url": "",
                "room": "", "room_url": "", "subgroup": ""
            }

            for i, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                link = cell.find('a', href=True)
                href = link['href'] if link else ""
                if href and not href.startswith('http'):
                    href = self.base_url + '/' + href.lstrip('/')

                if i == 0 and re.match(r'^\d+$', text):
                    lesson["lesson_number"] = text
                    continue
                if re.search(r'\d{1,2}:\d{2}–\d{1,2}:\d{2}', text):
                    lesson["time"] = text
                    continue
                if link and 'do.nhtk-edu.ru' in href:
                    subject_clean = re.sub(r'\s+', ' ', text).strip()
                    subject_clean = re.sub(r'\s*к/п\s*', ' ', subject_clean).strip()
                    lesson["subject"] = subject_clean
                    lesson["subject_url"] = href
                    subgroup_match = re.search(r'\[(\d+\s*п/г)\]', text)
                    if subgroup_match:
                        lesson["subgroup"] = subgroup_match.group(1).strip()
                    continue
                if link and 'расписание.нхтк.рф' in href and not lesson["teacher"]:
                    lesson["teacher"] = text
                    lesson["teacher_url"] = href
                    continue
                if re.match(r'^(\d{2,3}|с/[зк])$', text, re.IGNORECASE):
                    lesson["room"] = text
                    if link:
                        lesson["room_url"] = href
                    continue

            if not lesson["subject"]:
                for cell in cells:
                    text = cell.get_text(strip=True)
                    if text and not re.match(r'^\d+$', text) and not re.search(r'\d{1,2}:\d{2}', text):
                        if not re.match(r'^(\d{2,3}|с/[зк])$', text, re.IGNORECASE):
                            if not lesson["subject"]:
                                lesson["subject"] = re.sub(r'\s+', ' ', text).strip()
                            elif not lesson["teacher"]:
                                lesson["teacher"] = text

            if not lesson["subject"] or not lesson["time"]:
                return None
            return lesson
        except Exception as e:
            print(f"⚠️ Ошибка парсинга строки: {e}")
            return None

    def save_to_json(self, data: Dict, filename: str = "nhtk_schedule.json") -> bool:
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"❌ Ошибка сохранения JSON: {e}")
            return False

    def _get_data_hash(self, schedule: List[Dict]) -> str:
        """Создает хэш данных для проверки изменений"""
        sorted_data = json.dumps(schedule, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(sorted_data.encode('utf-8')).hexdigest()

    def check_data_changed(self, new_data: Dict) -> bool:
        """Проверяет, изменились ли данные по сравнению с тем, что в YDB"""
        if not YDB_AVAILABLE:
            return True

        try:
            group_code = new_data.get("metadata", {}).get("group", "")
            if not group_code:
                return True

            # Подключаемся к YDB, если ещё не подключены
            if not self.ydb_client.pool:
                if not self.ydb_client.connect():
                    return True

            old_hash = self.ydb_client.get_last_hash(group_code)
            new_hash = self._get_data_hash(new_data.get("schedule", []))

            if old_hash == new_hash:
                print("✅ Данные не изменились (хэш совпадает)")
                return False
            else:
                print("🔄 Данные изменились")
                return True

        except Exception as e:
            print(f"⚠️ Ошибка проверки изменений: {e}")
            return True

    def save_to_ydb(self, data: Dict) -> bool:
        """Сохранение данных в YDB"""
        if not YDB_AVAILABLE:
            return False

        try:
            if not self.ydb_client.pool:
                if not self.ydb_client.connect():
                    return False

            schedule_items = data.get("schedule", [])
            metadata = data.get("metadata", {})

            if not schedule_items:
                return False

            current_data_hash = self._get_data_hash(schedule_items)

            items_to_insert = []
            for item in schedule_items:
                lesson_num = None
                if item.get("lesson_number"):
                    try:
                        lesson_num = int(item["lesson_number"])
                    except (ValueError, TypeError):
                        lesson_num = None

                items_to_insert.append({
                    "group_code": metadata.get("group", ""),
                    "period": metadata.get("period", ""),
                    "source_url": metadata.get("source_url", ""),
                    "day": item.get("day", ""),
                    "lesson_number": lesson_num,
                    "time": item.get("time", ""),
                    "subject": item.get("subject", ""),
                    "subject_url": item.get("subject_url", ""),
                    "teacher": item.get("teacher", ""),
                    "teacher_url": item.get("teacher_url", ""),
                    "room": item.get("room", ""),
                    "room_url": item.get("room_url", ""),
                    "subgroup": item.get("subgroup", ""),
                    "parsed_at": datetime.now(timezone.utc),
                    "data_hash": current_data_hash
                })

            return self.ydb_client.upsert_schedule_items(items_to_insert)

        except Exception as e:
            print(f"❌ Ошибка отправки в YDB: {e}")
            return False

    def parse_url(self, url: str, output_file: str = "nhtk_schedule.json",
                  upload_to_db: bool = True, is_scheduled: bool = False) -> bool:
        """Основной метод запуска"""
        if '#заголовок' not in url:
            url = url + '#заголовок'

        html = self.fetch_page(url)
        if not html:
            return False

        print("🔍 Парсинг расписания...")
        data = self.parse_schedule(html, source_url=url)

        print(f"📊 Найдено занятий: {len(data['schedule'])}")

        # Сохраняем локальный JSON всегда
        self.save_to_json(data, output_file)

        # Логика отправки в YDB
        if upload_to_db:
            has_changes = self.check_data_changed(data)

            if not has_changes:
                print("💤 Данные не изменились, загрузка в базу пропущена")
                return True

            print("☁️ Отправка обновленных данных в YDB...")
            return self.save_to_ydb(data)

        return True

    def get_schedule_summary(self, data: Dict) -> Dict:
        summary = {
            "group": data["metadata"]["group"],
            "total_lessons": len(data["schedule"]),
        }
        return summary

    def cleanup(self):
        """Закрытие соединений"""
        self.ydb_client.close()


if __name__ == "__main__":
    print("=" * 60)
    print("🎓 Парсер НХТК (YDB Edition)")
    print("=" * 60)

    parser = NHTKLiveParser()
    url = "https://расписание.нхтк.рф/09.07.13п1.html"

    # Проверка наличия ключей YDB
    has_keys = bool(
        os.getenv("YDB_ENDPOINT") and
        os.getenv("YDB_DATABASE") and
        os.getenv("YDB_TOKEN")
    )
    is_scheduled = os.getenv("IS_SCHEDULED") == 'true'

    print(f"🔑 Ключи YDB: {'Найдены' if has_keys else 'Не найдены'}")
    print(f"🕒 Тип запуска: {'Плановый (Cron)' if is_scheduled else 'Ручной'}")

    try:
        success = parser.parse_url(
            url,
            "nhtk_schedule.json",
            upload_to_db=has_keys,
            is_scheduled=is_scheduled
        )

        if success:
            print("\n✅ Задача выполнена успешно")
        else:
            print("\n❌ Ошибка выполнения")
            exit(1)
    finally:
        # Гарантированное закрытие соединений
        parser.cleanup()

    print("=" * 60)