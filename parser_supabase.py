#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Парсер расписания НХТК с оптимизацией для GitHub Actions
Проверяет изменения перед отправкой в Supabase
"""

import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
from typing import Optional, List, Dict
import time
import os
import hashlib

# === Импорт для Supabase ===
try:
    from supabase import create_client, Client

    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    print("⚠️ Библиотека supabase не установлена.")


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

    def fetch_page(self, url: str) -> Optional[str]:
        """Получение HTML страницы (быстрое)"""
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
            if not cells: continue

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
                "day": day, "lesson_number": None, "time": "", "subject": "",
                "subject_url": "", "teacher": "", "teacher_url": "",
                "room": "", "room_url": "", "subgroup": ""
            }

            for i, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                link = cell.find('a', href=True)
                href = link['href'] if link else ""
                if href and not href.startswith('http'):
                    href = self.base_url + '/' + href.lstrip('/')

                # ✅ Улучшенный парсинг номера пары
                if i < 2 and re.match(r'^\s*[1-9]\s*$', text):
                    try:
                        lesson["lesson_number"] = int(text.strip())
                    except ValueError:
                        pass
                    continue

                # Время
                if re.search(r'\d{1,2}:\d{2}–\d{1,2}:\d{2}', text):
                    lesson["time"] = text
                    # Если номер пары не найден, определяем по времени
                    if lesson["lesson_number"] is None:
                        lesson["lesson_number"] = self._get_lesson_from_time(text)
                    continue

                # Предмет
                if link and 'do.nhtk-edu.ru' in href:
                    subject_clean = re.sub(r'\s+', ' ', text).strip()
                    subject_clean = re.sub(r'\s*к/п\s*', ' ', subject_clean).strip()
                    lesson["subject"] = subject_clean
                    lesson["subject_url"] = href
                    subgroup_match = re.search(r'\[(\d+\s*п/г)\]', text)
                    if subgroup_match:
                        lesson["subgroup"] = subgroup_match.group(1).strip()
                    continue

                # Преподаватель
                if link and 'расписание.нхтк.рф' in href and not lesson["teacher"]:
                    lesson["teacher"] = text
                    lesson["teacher_url"] = href
                    continue

                # Аудитория
                if re.match(r'^(\d{2,3}|с/[зк])$', text, re.IGNORECASE):
                    lesson["room"] = text
                    if link:
                        lesson["room_url"] = href
                    continue

            # Если предмет не найден через ссылку, пробуем найти по тексту
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

    def _get_lesson_from_time(self, time: str) -> Optional[int]:
        """Определяет номер пары по времени начала"""
        if not time:
            return None

        start_time = time.split('–')[0].strip() if '–' in time else time.split('-')[0].strip()

        time_map = {
            '8:30': 1, '08:30': 1,
            '9:00': 1, '09:00': 1,
            '10:15': 2, '10:30': 2,
            '12:00': 3, '12:30': 3,
            '14:00': 4, '14:30': 4,
            '16:00': 5, '16:30': 5,
            '18:00': 6, '18:30': 6,
        }

        return time_map.get(start_time)

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
        """Проверяет, изменились ли данные по сравнению с тем, что в базе"""
        if not SUPABASE_AVAILABLE:
            return True

        try:
            url = os.getenv("SUPABASE_URL")
            key = os.getenv("SUPABASE_KEY")
            if not url or not key:
                return True

            supabase: Client = create_client(url, key)
            group_code = new_data.get("metadata", {}).get("group", "")

            if not group_code:
                return True

            response = supabase.table("schedule_items") \
                .select("data_hash") \
                .eq("group_code", group_code) \
                .order("parsed_at", desc=True) \
                .limit(1) \
                .execute()

            if not response.data:
                print("ℹ️ В базе нет предыдущих данных")
                return True

            old_hash = response.data[0].get("data_hash")
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

    def save_to_supabase(self, data: Dict) -> bool:
        if not SUPABASE_AVAILABLE:
            return False

        try:
            url = os.getenv("SUPABASE_URL")
            key = os.getenv("SUPABASE_KEY")
            if not url or not key:
                return False

            supabase: Client = create_client(url, key)
            schedule_items = data.get("schedule", [])
            metadata = data.get("metadata", {})

            if not schedule_items:
                return False

            current_data_hash = self._get_data_hash(schedule_items)

            items_to_insert = []
            for item in schedule_items:
                items_to_insert.append({
                    "group_code": metadata.get("group", ""),
                    "period": metadata.get("period", ""),
                    "source_url": metadata.get("source_url", ""),
                    "day": item.get("day", ""),
                    "lesson_number": item.get("lesson_number"),  # Уже int или None
                    "time": item.get("time", ""),
                    "subject": item.get("subject", ""),
                    "subject_url": item.get("subject_url", ""),
                    "teacher": item.get("teacher", ""),
                    "teacher_url": item.get("teacher_url", ""),
                    "room": item.get("room", ""),
                    "room_url": item.get("room_url", ""),
                    "subgroup": item.get("subgroup", ""),
                    "parsed_at": datetime.now().isoformat(),
                    "data_hash": current_data_hash
                })

            # ✅ СНАЧАЛА удаляем старые записи этой группы
            print(f"🗑️ Удаляем старые записи для группы: {metadata.get('group', '')}")
            supabase.table("schedule_items").delete().eq("group_code", metadata.get("group")).execute()

            # ✅ ПОТОМ вставляем новые
            response = supabase.table("schedule_items").insert(items_to_insert).execute()
            print(f"✅ Загружено {len(items_to_insert)} записей в Supabase")
            return True

        except Exception as e:
            print(f"❌ Ошибка отправки в Supabase: {e}")
            return False

    def parse_url(self, url: str, output_file: str = "nhtk_schedule.json",
                  upload_to_supabase: bool = True, is_scheduled: bool = False) -> bool:
        """Основной метод запуска"""
        if '#заголовок' not in url:
            url = url + '#заголовок'

        html = self.fetch_page(url)
        if not html:
            return False

        print("🔍 Парсинг расписания...")
        data = self.parse_schedule(html, source_url=url)

        print(f"📊 Найдено занятий: {len(data['schedule'])}")

        self.save_to_json(data, output_file)

        if upload_to_supabase:
            # ✅ Проверка флага принудительного обновления
            force_update = os.getenv("FORCE_UPDATE") == 'true'

            if force_update:
                print("⚡ FORCE_UPDATE: принудительная перезапись данных")
                has_changes = True
            else:
                has_changes = self.check_data_changed(data)

            if not has_changes:
                print("💤 Данные не изменились, загрузка в базу пропущена")
                return True

            print("☁️ Отправка обновленных данных в Supabase...")
            return self.save_to_supabase(data)

        return True

    def get_schedule_summary(self, data: Dict) -> Dict:
        return {
            "group": data["metadata"]["group"],
            "total_lessons": len(data["schedule"]),
        }


if __name__ == "__main__":
    print("=" * 60)
    print("🎓 Парсер НХТК (Optimized for GitHub Actions)")
    print("=" * 60)

    parser = NHTKLiveParser()
    url = "https://расписание.нхтк.рф/09.07.13п1.html"

    has_keys = bool(os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_KEY"))
    is_scheduled = os.getenv("IS_SCHEDULED") == 'true'

    print(f"🔑 Ключи Supabase: {'Найдены' if has_keys else 'Не найдены'}")
    print(f"🕒 Тип запуска: {'Плановый (Cron)' if is_scheduled else 'Ручной'}")

    success = parser.parse_url(
        url,
        "nhtk_schedule.json",
        upload_to_supabase=has_keys,
        is_scheduled=is_scheduled
    )

    if success:
        print("\n✅ Задача выполнена успешно")
    else:
        print("\n❌ Ошибка выполнения")
        exit(1)

    print("=" * 60)