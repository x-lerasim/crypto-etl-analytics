# Crypto ETL Analytics

Инфраструктурный проект для построения аналитической платформы криптовалютных данных с полным ETL-пайплайном, историчностью данных и витринами для BI.


---

## Описание проекта

Данный проект — это инфраструктура для извлечения и преобразования криптовалютных данных с целью аналитики и построения дашбордов.

В основе лежит пайплайн, который получает данные из API [Coincap.io](https://pro.coincap.io/api-docs/), подготавливает их к загрузке и загружает в аналитическое хранилище с поддержкой историчности данных.

**Crypto ETL Analytics** — это end-to-end pipeline для:

- Извлечения данных из API Coincap  
- Обработки и трансформации данных  
- Загрузки данных в аналитическое хранилище  
- Построения витрин и дашбордов  
- Поддержки историчности данных  

---

## Архитектурные слои

- **Raw layer** — сырые данные из API  
- **Core layer** — очищенные и нормализованные данные  
- **Data Mart** — аналитические витрины (dbt)  
- **BI layer** — Metabase dashboards  

---

## Стек проекта

- **Airflow** — управление и оркестрация ETL-задач  
- **ClickHouse** — аналитическая база данных  
- **dbt** — моделирование данных и построение витрин  
- **PySpark** — распределённая обработка данных  
- **MinIO** — S3-совместимое хранилище  
- **Metabase** — BI и дашборды  
- **Docker / docker-compose** — контейнеризация инфраструктуры  

---

## Как работает пайплайн

Extract:

-  Airflow запускает DAG по расписанию
-  Выполняется запрос к Coincap API
-  Данные сохраняются в raw слой

Transform:
   
-  Очистка данных
-  Приведение типов
-  Нормализация структуры
-  Формирование core слоя

Load:
   
-  Сохранение данных в MinIO (raw/core)
-  Загрузка обработанных данных в ClickHouse
-  Поддержка историчности данных

Modeling (dbt):

-  Построение аналитических моделей
-  Создание витрин (data marts)
-  Подготовка агрегатов и представлений

Visualization (Metabase):

-  Metabase подключается к ClickHouse
-  Построение интерактивных дашбордов

---

## Структура проекта

```text
crypto-etl-analytics/
├── airflow/            # DAG’и и конфигурация Airflow
├── spark/              # Spark jobs
├── dbt/                # Модели и витрины
├── docker/             # Docker-конфигурация
└── docker-compose.yml  # Оркестрация сервисов
```
---

## Структура данных

### MinIO (Data Lake)

**Raw layer**
- `raw/assets` — сырые данные по крипто-активам из Coincap API

**Core layer**
- `core/dim_assets` — очищенные и нормализованные данные (dimension) по активам

---

### ClickHouse (DWH)

- `dim_assets` — таблица измерений по крипто-активам (загружается из `core/dim_assets`)

---

## Data Marts (dbt)

Витрины данных, которые используются для аналитики и дашбордов (папка `models/marts`):

- `mart_daily_summary` — ежедневные агрегаты и сводные метрики
- `mart_market_overview` — обзор рынка (ключевые метрики рынка/активов)
- `mart_top_cryptos` — топ криптовалют по выбранным метрикам

Файлы проекта dbt:
- `models/marts/mart_daily_summary.sql`
- `models/marts/mart_market_overview.sql`
- `models/marts/mart_top_cryptos.sql`
- `models/marts/_mart_top_cryptos.yml` — описание/схема витрины

---

## Установка и запуск

### Клонирование репозитория

```bash
git clone https://github.com/x-lerasim/crypto-etl-analytics.git
cd crypto-etl-analytics
```

### Запуск
```bash
docker-compose up -d --build 
```

---

## Доступные сервисы

После успешного запуска инфраструктуры будут доступны:

| Сервис      | Назначение                   | URL / Порт              |
|------------|------------------------------|-------------------------|
| Airflow UI | Оркестрация и управление DAG | http://localhost:8080   |
| Metabase   | BI и дашборды                | http://localhost:3000   |
| MinIO      | S3-хранилище                 | http://localhost:9001   |
| ClickHouse | Аналитическая база данных    | http://localhost:9000/play  |

---

## Данные авторизации по умолчанию

| Сервис    | Логин  | Пароль   |
|----------|--------|----------|
| Airflow  | airflow | airflow  |
| MinIO    | admin  | admin123 |
| ClickHouse | admin | admin    |
| Metabase | admin@crypto.local | CryptoAdmin_2026!    |

---

## Пример дашборда
![preview](https://github.com/user-attachments/assets/d84880dc-69ad-450a-9ca0-f1813085065f)


