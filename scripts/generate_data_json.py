#!/usr/bin/env python3
import csv
import json
import re
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / 'data'
OUTPUT = ROOT / 'data.json'

FIELD_MAP = {
    '日期': 'date',
    '部门': 'department',
    '项目': 'project',
    '坐席姓名': 'agent',
    '通时': 'talk_time',
    '拨打次数': 'dial_count',
    '有效接通次数': 'connected_count',
    '有效接通率': 'connect_rate_raw',
    '当日意向客户数': 'intent_count',
    '当日申请客户数': 'apply_count',
    '当日任务量': 'task_count',
    '当日任务完成量': 'task_done',
    '当日任务达成率': 'task_rate_raw',
    '当日放款量': 'loan_count',
    '本月月度指标': 'monthly_target',
    '失败客户数': 'failed_count',
}

NUMERIC_FIELDS = [
    'dial_count', 'connected_count', 'intent_count', 'apply_count',
    'task_count', 'task_done', 'loan_count', 'monthly_target', 'failed_count'
]


def parse_number(v):
    if v is None:
        return 0
    s = str(v).strip()
    if s == '' or s.lower() == 'null' or s == '/':
        return 0
    try:
        if '.' in s:
            return float(s)
        return int(s)
    except ValueError:
        try:
            return float(s.replace('%', ''))
        except ValueError:
            return 0


def parse_rate(v):
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s == '' or s == '/':
        return 0.0
    if s.endswith('%'):
        try:
            return float(s[:-1]) / 100.0
        except ValueError:
            return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def parse_talk_seconds(v):
    if v is None:
        return 0
    s = str(v).strip()
    if s == '' or s == '/':
        return 0
    m = re.match(r'^(?:(\d+)分)?(?:(\d+)秒)?$', s)
    if not m:
        return 0
    minutes = int(m.group(1) or 0)
    seconds = int(m.group(2) or 0)
    return minutes * 60 + seconds


def normalize_date(v):
    s = (v or '').strip()
    if not s:
        return ''
    for fmt in ('%Y/%m/%d', '%Y-%m-%d', '%Y.%m.%d'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y/%m/%d')
        except ValueError:
            continue
    return s


def make_key(item):
    return (
        item['date'], item['department'], item['project'], item['agent'],
        item['dial_count'], item['connected_count'], item['intent_count'],
        item['apply_count'], item['task_count'], item['task_done'], item['loan_count']
    )


def row_to_record(row):
    raw = {}
    for cn, en in FIELD_MAP.items():
        raw[en] = row.get(cn, '')

    rec = {
        'date': normalize_date(str(raw['date']).strip()),
        'department': str(raw['department']).strip(),
        'project': str(raw['project']).strip(),
        'agent': str(raw['agent']).strip(),
        'talk_time': str(raw['talk_time']).strip() or '/',
    }

    for field in NUMERIC_FIELDS:
        value = parse_number(raw[field])
        rec[field] = int(value) if float(value).is_integer() else value

    connect_rate_value = parse_rate(raw['connect_rate_raw'])
    task_rate_value = parse_rate(raw['task_rate_raw'])

    if rec['dial_count'] and not connect_rate_value:
        connect_rate_value = rec['connected_count'] / rec['dial_count']
    if rec['task_count'] and not task_rate_value:
        task_rate_value = rec['task_done'] / rec['task_count']

    rec['talk_seconds'] = parse_talk_seconds(rec['talk_time'])
    rec['connect_rate_value'] = round(connect_rate_value, 4)
    rec['task_rate_value'] = round(task_rate_value, 4)
    rec['connect_rate'] = f"{connect_rate_value * 100:.2f}%"
    rec['task_rate'] = f"{task_rate_value * 100:.0f}%"
    return rec


def main():
    files = sorted(DATA_DIR.glob('sync_*.csv'))
    if not files:
        raise SystemExit('No sync_*.csv files found in data directory.')

    records = []
    seen = set()
    for file in files:
        with file.open('r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not any((str(v).strip() if v is not None else '') for v in row.values()):
                    continue
                rec = row_to_record(row)
                if not rec['date'] or not rec['department'] or not rec['project'] or not rec['agent']:
                    continue
                key = make_key(rec)
                if key in seen:
                    continue
                seen.add(key)
                records.append(rec)

    records.sort(key=lambda x: (x['date'], x['department'], x['project'], x['agent']))
    OUTPUT.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'Wrote {len(records)} records to {OUTPUT}')


if __name__ == '__main__':
    main()
