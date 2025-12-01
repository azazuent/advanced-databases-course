# Инфраструктура PostgreSQL с мониторингом

## Описание

Этот проект содержит настройку PostgreSQL 16 с мониторингом через Prometheus и Grafana для курса "Продвинутые СУБД".

## Компоненты

- **PostgreSQL 16** - реляционная база данных
- **postgres_exporter** - экспортер метрик для Prometheus
- **Prometheus** - сбор и хранение метрик
- **Grafana** - визуализация метрик

## Требования

- Docker Desktop 24.x+
- Docker Compose v2.x+
- 4 ГБ свободного места на диске
- 4 ГБ оперативной памяти

## Быстрый старт

### 1. Запуск инфраструктуры

```bash
cd infrastructure/postgres
docker compose up -d
