# Fraud Detection dbt Project

dbt проект для анализа транзакций и выявления мошенничества на основе данных из ClickHouse.

## Структура проекта

```
dbt/
├── dbt_project.yml          # Конфигурация проекта
├── packages.yml             # Зависимости (dbt_utils, dbt_expectations, elementary)
├── profiles.yml             # Конфигурация подключения к ClickHouse
├── models/
│   ├── sources/
│   │   └── sources.yml      # Определение источника данных
│   ├── staging/
│   │   ├── stg_transactions.sql
│   │   └── stg_transactions.yml
│   └── marts/
│       ├── mart_daily_state_metrics.sql
│       ├── mart_fraud_by_category.sql
│       ├── mart_fraud_by_state.sql
│       ├── mart_customer_risk_profile.sql
│       ├── mart_hourly_fraud_pattern.sql
│       ├── mart_merchant_analytics.sql
│       └── schema.yml       # Метаданные и тесты для витрин
├── macros/
│   └── amount_bucket.sql    # Макрос для сегментации сумм
├── tests/                   # Singular тесты
│   ├── assert_no_negative_amounts.sql
│   ├── assert_fraud_rate_bounds.sql
│   ├── assert_state_fraud_rate_bounds.sql
│   └── assert_merchant_suspicious_flag.sql
```

```

## Установка

1. Установите dbt-core и dbt-clickhouse:
```bash
pip install dbt-core dbt-clickhouse
```

2. Настройте переменные окружения:
```bash
export CLICKHOUSE_PASSWORD=your_secure_password_here
```

3. Установите зависимости:
```bash
cd dbt/
dbt deps
```

## Использование

### Базовые команды

```bash
# Установка пакетов
dbt deps

# Загрузка seeds (если есть)
dbt seed

# Запуск моделей
dbt run

# Запуск тестов
dbt test

# Генерация документации
dbt docs generate
dbt docs serve
```

## Модели

### Staging

- **stg_transactions**: Очистка и нормализация сырых данных, добавление вычисляемых полей (сегментация сумм, расстояние между клиентом и мерчантом, временные признаки)

### Marts (Витрины)

1. **mart_daily_state_metrics**: Дневные метрики по штатам
   - Количество транзакций, сумма, средний чек, P95, доля крупных транзакций

2. **mart_fraud_by_category**: Анализ фрода по категориям
   - Общее число транзакций, число фродов, fraud_rate (%), суммы

3. **mart_fraud_by_state**: Географический анализ фрода
   - Fraud_rate, уникальные клиенты/мерчанты, суммы по штатам

4. **mart_customer_risk_profile**: Профиль риска клиентов
   - Сегментация по уровню риска (HIGH/MEDIUM/LOW), история транзакций, fraud_rate

5. **mart_hourly_fraud_pattern**: Временные паттерны фрода
   - Анализ по дням недели и часам, выявление временных окон с повышенным риском

6. **mart_merchant_analytics**: Аналитика по мерчантам
   - Оборот, fraud_rate, флаг подозрительности

## Тесты

### Generic тесты
- `not_null`: Проверка на отсутствие NULL значений
- `unique`: Проверка уникальности
- `accepted_values`: Проверка допустимых значений
- `dbt_utils.unique_combination_of_columns`: Уникальность комбинаций колонок
- `dbt_expectations.expect_column_values_to_be_between`: Проверка диапазона значений

### Singular тесты
- `assert_no_negative_amounts`: Проверка отсутствия отрицательных сумм
- `assert_fraud_rate_bounds`: Проверка корректности fraud_rate (0-100%)
- `assert_state_fraud_rate_bounds`: Проверка fraud_rate по штатам
- `assert_merchant_suspicious_flag`: Проверка флага подозрительности мерчанта

## Макросы

- **amount_bucket**: Сегментация сумм транзакций на категории (SMALL, MEDIUM, LARGE, VERY_LARGE)

