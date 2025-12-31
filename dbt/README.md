# Описание результатов
## Версии

- **dbt-core**: 1.11.2
- **dbt-clickhouse**: 1.9.7

## Реализованные витрины (Marts)

Все 6 витрин:

1. **mart_daily_state_metrics** - Дневные метрики по штатам
   - Агрегация транзакций по дате и штату
   - Метрики: количество транзакций, сумма, средний чек, P95, доля крупных транзакций

2. **mart_fraud_by_category** - Анализ фрода по категориям
   - Выявление категорий с наибольшим уровнем мошенничества
   - Метрики: общее число транзакций, число фродов, fraud_rate (%), суммы

3. **mart_fraud_by_state** - Географический анализ фрода
   - Распределение фрода по штатам США
   - Метрики: fraud_rate, уникальные клиенты/мерчанты, суммы

4. **mart_customer_risk_profile** - Профиль риска клиентов
   - Сегментация клиентов по уровню риска (HIGH/MEDIUM/LOW)
   - История транзакций, fraud_rate на клиента, средний чек

5. **mart_hourly_fraud_pattern** - Временные паттерны фрода
   - Анализ по дням недели и часам
   - Выявление временных окон с повышенным риском

6. **mart_merchant_analytics** - Аналитика по мерчантам
   - Метрики по каждому мерчанту: оборот, fraud_rate, флаг подозрительности

## Тесты

### Generic тесты (в schema.yml файлах)

**Staging (stg_transactions.yml):**
- `not_null` - для transaction_id, transaction_time, merchant_id, category_id, amount, is_fraud, us_state
- `unique` - для transaction_id
- `accepted_values` - для is_fraud (0, 1), amount_bucket (SMALL, MEDIUM, LARGE, VERY_LARGE), gender (M, F)
- `dbt_expectations.expect_column_values_to_be_between` - для amount (0-1000000)

**Marts (schema.yml):**
- `not_null` - для всех ключевых полей
- `unique` - для category_id, us_state, customer_id, merchant_id
- `accepted_values` - для risk_level (HIGH, MEDIUM, LOW), is_suspicious (0, 1)
- `dbt_expectations.expect_column_values_to_be_between` - для fraud_rate (0-100%), transaction_hour (0-23), day_of_week (1-7)
- `dbt_utils.unique_combination_of_columns` - для mart_daily_state_metrics (transaction_date, us_state) и mart_hourly_fraud_pattern (transaction_hour, day_of_week)

**Sources (sources.yml):**
- `unique` - для transaction_id в таблице transactions
- `dbt_expectations.expect_column_values_to_be_of_type` - для amount (float64)

### Singular тесты (в папке tests/)

1. **assert_no_negative_amounts.sql** - Проверка отсутствия отрицательных сумм в stg_transactions
2. **assert_fraud_rate_bounds.sql** - Проверка корректности fraud_rate (0-100%) в mart_fraud_by_category
3. **assert_state_fraud_rate_bounds.sql** - Проверка fraud_rate по штатам в mart_fraud_by_state
4. **assert_merchant_suspicious_flag.sql** - Проверка флага подозрительности мерчанта в mart_merchant_analytics

## Макросы

- **amount_bucket** - Сегментация сумм транзакций на категории:
  - SMALL: < 10
  - MEDIUM: 10-50
  - LARGE: 50-200
  - VERY_LARGE: >= 200

## Пакеты

- **dbt-labs/dbt_utils** (1.1.1)
- **metaplane/dbt_expectations** (0.10.1)
- **elementary-data/elementary** (0.21.0)


- Проект настроен на работу с ClickHouse
- Таблица `scores` добавлена в ClickHouse для получения признаков фрода из ML модели

## Лог тестов
~/PycharmProjects/hwml/mts25_mlops_hw2_real_time_fraud_detection/dbt$ dbt test
20:21:15  Running with dbt=1.11.2
20:21:16  Registered adapter: clickhouse=1.9.7
20:21:17  Found 37 models, 2 operations, 55 data tests, 2 sources, 1681 macros
20:21:17  
20:21:17  Concurrency: 1 threads (target='dev')
20:21:17  
20:21:26  Elementary: Runtime data: {"config": {}, "dbt_version": "1.11.2", "elementary_version": "0.21.0", "database": "fraud_detection", "schema": "fraud_detection"}
20:21:26  1 of 1 START hook: elementary.on-run-start.0 ................................... [RUN]
20:21:26  1 of 1 OK hook: elementary.on-run-start.0 ...................................... [OK in 0.21s]
20:21:26  
20:21:26  1 of 55 START test accepted_values_mart_customer_risk_profile_risk_level__HIGH__MEDIUM__LOW  [RUN]
20:21:26  1 of 55 PASS accepted_values_mart_customer_risk_profile_risk_level__HIGH__MEDIUM__LOW  [PASS in 0.10s]
20:21:26  2 of 55 START test accepted_values_mart_merchant_analytics_is_suspicious__0__1 . [RUN]
20:21:26  2 of 55 PASS accepted_values_mart_merchant_analytics_is_suspicious__0__1 ....... [PASS in 0.04s]
20:21:26  3 of 55 START test accepted_values_stg_transactions_amount_bucket__SMALL__MEDIUM__LARGE__VERY_LARGE  [RUN]
20:21:26  3 of 55 PASS accepted_values_stg_transactions_amount_bucket__SMALL__MEDIUM__LARGE__VERY_LARGE  [PASS in 0.10s]
20:21:26  4 of 55 START test accepted_values_stg_transactions_gender__M__F ............... [RUN]
20:21:26  4 of 55 PASS accepted_values_stg_transactions_gender__M__F ..................... [PASS in 0.07s]
20:21:26  5 of 55 START test accepted_values_stg_transactions_is_fraud__0__1 ............. [RUN]
20:21:26  5 of 55 PASS accepted_values_stg_transactions_is_fraud__0__1 ................... [PASS in 0.05s]
20:21:26  6 of 55 START test assert_fraud_rate_bounds .................................... [RUN]
20:21:26  6 of 55 PASS assert_fraud_rate_bounds .......................................... [PASS in 0.03s]
20:21:26  7 of 55 START test assert_merchant_suspicious_flag ............................. [RUN]
20:21:26  7 of 55 PASS assert_merchant_suspicious_flag ................................... [PASS in 0.02s]
20:21:26  8 of 55 START test assert_no_negative_amounts .................................. [RUN]
20:21:26  8 of 55 PASS assert_no_negative_amounts ........................................ [PASS in 0.04s]
20:21:26  9 of 55 START test assert_state_fraud_rate_bounds .............................. [RUN]
20:21:26  9 of 55 PASS assert_state_fraud_rate_bounds .................................... [PASS in 0.11s]
20:21:26  10 of 55 START test dbt_expectations_expect_column_values_to_be_between_mart_customer_risk_profile_fraud_rate__100__0  [RUN]
20:21:27  10 of 55 PASS dbt_expectations_expect_column_values_to_be_between_mart_customer_risk_profile_fraud_rate__100__0  [PASS in 0.04s]
20:21:27  11 of 55 START test dbt_expectations_expect_column_values_to_be_between_mart_daily_state_metrics_transaction_count__0  [RUN]
20:21:27  11 of 55 PASS dbt_expectations_expect_column_values_to_be_between_mart_daily_state_metrics_transaction_count__0  [PASS in 0.03s]
20:21:27  12 of 55 START test dbt_expectations_expect_column_values_to_be_between_mart_fraud_by_category_fraud_rate__100__0  [RUN]
20:21:27  12 of 55 PASS dbt_expectations_expect_column_values_to_be_between_mart_fraud_by_category_fraud_rate__100__0  [PASS in 0.03s]
20:21:27  13 of 55 START test dbt_expectations_expect_column_values_to_be_between_mart_fraud_by_state_fraud_rate__100__0  [RUN]
20:21:27  13 of 55 PASS dbt_expectations_expect_column_values_to_be_between_mart_fraud_by_state_fraud_rate__100__0  [PASS in 0.03s]
20:21:27  14 of 55 START test dbt_expectations_expect_column_values_to_be_between_mart_hourly_fraud_pattern_day_of_week__7__1  [RUN]
20:21:27  14 of 55 PASS dbt_expectations_expect_column_values_to_be_between_mart_hourly_fraud_pattern_day_of_week__7__1  [PASS in 0.05s]
20:21:27  15 of 55 START test dbt_expectations_expect_column_values_to_be_between_mart_hourly_fraud_pattern_fraud_rate__100__0  [RUN]
20:21:27  15 of 55 PASS dbt_expectations_expect_column_values_to_be_between_mart_hourly_fraud_pattern_fraud_rate__100__0  [PASS in 0.06s]
20:21:27  16 of 55 START test dbt_expectations_expect_column_values_to_be_between_mart_hourly_fraud_pattern_transaction_hour__23__0  [RUN]
20:21:27  16 of 55 PASS dbt_expectations_expect_column_values_to_be_between_mart_hourly_fraud_pattern_transaction_hour__23__0  [PASS in 0.07s]
20:21:27  17 of 55 START test dbt_expectations_expect_column_values_to_be_between_mart_merchant_analytics_fraud_rate__100__0  [RUN]
20:21:27  17 of 55 PASS dbt_expectations_expect_column_values_to_be_between_mart_merchant_analytics_fraud_rate__100__0  [PASS in 0.06s]
20:21:27  18 of 55 START test dbt_expectations_expect_column_values_to_be_between_stg_transactions_amount__1000000__0  [RUN]
20:21:27  18 of 55 PASS dbt_expectations_expect_column_values_to_be_between_stg_transactions_amount__1000000__0  [PASS in 0.05s]
20:21:27  19 of 55 START test dbt_expectations_source_expect_column_values_to_be_of_type_transactions_db_transactions_amount__float64  [RUN]
20:21:27  19 of 55 PASS dbt_expectations_source_expect_column_values_to_be_of_type_transactions_db_transactions_amount__float64  [PASS in 0.09s]
20:21:27  20 of 55 START test dbt_utils_unique_combination_of_columns_mart_daily_state_metrics_transaction_date__us_state  [RUN]
20:21:27  20 of 55 PASS dbt_utils_unique_combination_of_columns_mart_daily_state_metrics_transaction_date__us_state  [PASS in 0.04s]
20:21:27  21 of 55 START test dbt_utils_unique_combination_of_columns_mart_hourly_fraud_pattern_transaction_hour__day_of_week  [RUN]
20:21:27  21 of 55 PASS dbt_utils_unique_combination_of_columns_mart_hourly_fraud_pattern_transaction_hour__day_of_week  [PASS in 0.04s]
20:21:27  22 of 55 START test not_null_mart_customer_risk_profile_customer_id ............ [RUN]
20:21:27  22 of 55 PASS not_null_mart_customer_risk_profile_customer_id .................. [PASS in 0.03s]
20:21:27  23 of 55 START test not_null_mart_customer_risk_profile_fraud_rate ............. [RUN]
20:21:27  23 of 55 PASS not_null_mart_customer_risk_profile_fraud_rate ................... [PASS in 0.11s]
20:21:27  24 of 55 START test not_null_mart_customer_risk_profile_risk_level ............. [RUN]
20:21:27  24 of 55 PASS not_null_mart_customer_risk_profile_risk_level ................... [PASS in 0.03s]
20:21:27  25 of 55 START test not_null_mart_daily_state_metrics_transaction_count ........ [RUN]
20:21:27  25 of 55 PASS not_null_mart_daily_state_metrics_transaction_count .............. [PASS in 0.03s]
20:21:27  26 of 55 START test not_null_mart_daily_state_metrics_transaction_date ......... [RUN]
20:21:27  26 of 55 PASS not_null_mart_daily_state_metrics_transaction_date ............... [PASS in 0.03s]
20:21:27  27 of 55 START test not_null_mart_daily_state_metrics_us_state ................. [RUN]
20:21:27  27 of 55 PASS not_null_mart_daily_state_metrics_us_state ....................... [PASS in 0.03s]
20:21:27  28 of 55 START test not_null_mart_fraud_by_category_category_id ................ [RUN]
20:21:27  28 of 55 PASS not_null_mart_fraud_by_category_category_id ...................... [PASS in 0.03s]
20:21:27  29 of 55 START test not_null_mart_fraud_by_category_fraud_count ................ [RUN]
20:21:27  29 of 55 PASS not_null_mart_fraud_by_category_fraud_count ...................... [PASS in 0.03s]
20:21:27  30 of 55 START test not_null_mart_fraud_by_category_fraud_rate ................. [RUN]
20:21:27  30 of 55 PASS not_null_mart_fraud_by_category_fraud_rate ....................... [PASS in 0.03s]
20:21:27  31 of 55 START test not_null_mart_fraud_by_category_total_transactions ......... [RUN]
20:21:28  31 of 55 PASS not_null_mart_fraud_by_category_total_transactions ............... [PASS in 0.03s]
20:21:28  32 of 55 START test not_null_mart_fraud_by_state_fraud_rate .................... [RUN]
20:21:28  32 of 55 PASS not_null_mart_fraud_by_state_fraud_rate .......................... [PASS in 0.03s]
20:21:28  33 of 55 START test not_null_mart_fraud_by_state_unique_customers .............. [RUN]
20:21:28  33 of 55 PASS not_null_mart_fraud_by_state_unique_customers .................... [PASS in 0.03s]
20:21:28  34 of 55 START test not_null_mart_fraud_by_state_unique_merchants .............. [RUN]
20:21:28  34 of 55 PASS not_null_mart_fraud_by_state_unique_merchants .................... [PASS in 0.03s]
20:21:28  35 of 55 START test not_null_mart_fraud_by_state_us_state ...................... [RUN]
20:21:28  35 of 55 PASS not_null_mart_fraud_by_state_us_state ............................ [PASS in 0.03s]
20:21:28  36 of 55 START test not_null_mart_hourly_fraud_pattern_day_of_week ............. [RUN]
20:21:28  36 of 55 PASS not_null_mart_hourly_fraud_pattern_day_of_week ................... [PASS in 0.03s]
20:21:28  37 of 55 START test not_null_mart_hourly_fraud_pattern_fraud_rate .............. [RUN]
20:21:28  37 of 55 PASS not_null_mart_hourly_fraud_pattern_fraud_rate .................... [PASS in 0.11s]
20:21:28  38 of 55 START test not_null_mart_hourly_fraud_pattern_transaction_hour ........ [RUN]
20:21:28  38 of 55 PASS not_null_mart_hourly_fraud_pattern_transaction_hour .............. [PASS in 0.02s]
20:21:28  39 of 55 START test not_null_mart_merchant_analytics_fraud_rate ................ [RUN]
20:21:28  39 of 55 PASS not_null_mart_merchant_analytics_fraud_rate ...................... [PASS in 0.02s]
20:21:28  40 of 55 START test not_null_mart_merchant_analytics_is_suspicious ............. [RUN]
20:21:28  40 of 55 PASS not_null_mart_merchant_analytics_is_suspicious ................... [PASS in 0.03s]
20:21:28  41 of 55 START test not_null_mart_merchant_analytics_merchant_id ............... [RUN]
20:21:28  41 of 55 PASS not_null_mart_merchant_analytics_merchant_id ..................... [PASS in 0.02s]
20:21:28  42 of 55 START test not_null_stg_transactions_amount ........................... [RUN]
20:21:28  42 of 55 PASS not_null_stg_transactions_amount ................................. [PASS in 0.04s]
20:21:28  43 of 55 START test not_null_stg_transactions_category_id ...................... [RUN]
20:21:28  43 of 55 PASS not_null_stg_transactions_category_id ............................ [PASS in 0.04s]
20:21:28  44 of 55 START test not_null_stg_transactions_is_fraud ......................... [RUN]
20:21:28  44 of 55 PASS not_null_stg_transactions_is_fraud ............................... [PASS in 0.04s]
20:21:28  45 of 55 START test not_null_stg_transactions_merchant_id ...................... [RUN]
20:21:28  45 of 55 PASS not_null_stg_transactions_merchant_id ............................ [PASS in 0.05s]
20:21:28  46 of 55 START test not_null_stg_transactions_transaction_id ................... [RUN]
20:21:28  46 of 55 PASS not_null_stg_transactions_transaction_id ......................... [PASS in 0.05s]
20:21:28  47 of 55 START test not_null_stg_transactions_transaction_time ................. [RUN]
20:21:28  47 of 55 PASS not_null_stg_transactions_transaction_time ....................... [PASS in 0.03s]
20:21:28  48 of 55 START test not_null_stg_transactions_us_state ......................... [RUN]
20:21:28  48 of 55 PASS not_null_stg_transactions_us_state ............................... [PASS in 0.06s]
20:21:28  49 of 55 START test source_unique_transactions_db_scores_transaction_id ........ [RUN]
20:21:28  49 of 55 PASS source_unique_transactions_db_scores_transaction_id .............. [PASS in 0.04s]
20:21:28  50 of 55 START test source_unique_transactions_db_transactions_transaction_id .. [RUN]
20:21:28  50 of 55 PASS source_unique_transactions_db_transactions_transaction_id ........ [PASS in 0.03s]
20:21:28  51 of 55 START test unique_mart_customer_risk_profile_customer_id .............. [RUN]
20:21:28  51 of 55 PASS unique_mart_customer_risk_profile_customer_id .................... [PASS in 0.03s]
20:21:28  52 of 55 START test unique_mart_fraud_by_category_category_id .................. [RUN]
20:21:28  52 of 55 PASS unique_mart_fraud_by_category_category_id ........................ [PASS in 0.13s]
20:21:28  53 of 55 START test unique_mart_fraud_by_state_us_state ........................ [RUN]
20:21:28  53 of 55 PASS unique_mart_fraud_by_state_us_state .............................. [PASS in 0.03s]
20:21:28  54 of 55 START test unique_mart_merchant_analytics_merchant_id ................. [RUN]
20:21:29  54 of 55 PASS unique_mart_merchant_analytics_merchant_id ....................... [PASS in 0.03s]
20:21:29  55 of 55 START test unique_stg_transactions_transaction_id ..................... [RUN]
20:21:29  55 of 55 PASS unique_stg_transactions_transaction_id ........................... [PASS in 0.04s]
20:21:29  
20:21:41  1 of 1 START hook: elementary.on-run-end.0 ..................................... [RUN]
20:21:41  1 of 1 OK hook: elementary.on-run-end.0 ........................................ [OK in 12.14s]
20:21:41  
20:21:41  Finished running 2 project hooks, 55 data tests in 0 hours 0 minutes and 23.77 seconds (23.77s).
20:21:41  
20:21:41  Completed successfully
20:21:41  
20:21:41  Done. PASS=57 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=57



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

