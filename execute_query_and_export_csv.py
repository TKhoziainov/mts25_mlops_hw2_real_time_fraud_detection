#!/usr/bin/env python3
import pandas as pd
import argparse
import os
import sys
from clickhouse_driver import Client

def execute_query_and_export(clickhouse_host, clickhouse_port, database, query_file, output_csv, user=None, password=None):
    """
    Выполняет SQL запрос из файла в ClickHouse и сохраняет результат в CSV
    
    Args:
        clickhouse_host: хост ClickHouse (по умолчанию localhost)
        clickhouse_port: порт ClickHouse (по умолчанию 9000)
        database: название базы данных
        query_file: путь к файлу с SQL запросом
        output_csv: путь к выходному CSV файлу
        user: пользователь ClickHouse
        password: пароль ClickHouse
    """
    try:
        print(f"Подключение к ClickHouse: {clickhouse_host}:{clickhouse_port}")
        client_params = {
            'host': clickhouse_host,
            'port': clickhouse_port,
            'database': database
        }
        if user:
            client_params['user'] = user
        if password:
            client_params['password'] = password
        
        client = Client(**client_params)
        
        print(f"Чтение SQL запроса из файла: {query_file}")
        with open(query_file, 'r', encoding='utf-8') as f:
            query_content = f.read()
        
        statements = [s.strip() for s in query_content.split(';') if s.strip()]
        query = None
        for stmt in statements:
            stmt_upper = stmt.upper().strip()
            if stmt_upper.startswith('USE '):
                continue
            query = stmt
            break
        
        if not query:
            print("Ошибка: не найдено SQL запроса для выполнения", file=sys.stderr)
            return False
        
        print("Выполнение запроса...")
        result = client.execute(query, with_column_types=True)
        
        if not result or len(result) == 0:
            print("Запрос не вернул результатов", file=sys.stderr)
            return False
        
        columns = [col[0] for col in result[1]]
        data = result[0]
        
        df = pd.DataFrame(data, columns=columns)
        
        print(f"Получено строк: {len(df)}")
        print(f"Сохранение результата в CSV: {output_csv}")
        df.to_csv(output_csv, index=False)
        
        print(f"✓ Результат успешно сохранен в {output_csv}")
        return True
        
    except FileNotFoundError:
        print(f"Ошибка: файл '{query_file}' не найден", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {str(e)}", file=sys.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Выполняет SQL запрос в ClickHouse и сохраняет результат в CSV'
    )
    parser.add_argument(
        'query_file',
        type=str,
        help='Путь к файлу с SQL запросом'
    )
    parser.add_argument(
        'output_csv',
        type=str,
        help='Путь к выходному CSV файлу'
    )
    parser.add_argument(
        '--clickhouse-host',
        type=str,
        default=os.getenv('CLICKHOUSE_HOST', 'localhost'),
        help='Хост ClickHouse (по умолчанию: localhost или из переменной окружения CLICKHOUSE_HOST)'
    )
    parser.add_argument(
        '--clickhouse-port',
        type=int,
        default=int(os.getenv('CLICKHOUSE_PORT', '9000')),
        help='Порт ClickHouse (по умолчанию: 9000 или из переменной окружения CLICKHOUSE_PORT)'
    )
    parser.add_argument(
        '--database',
        type=str,
        default=os.getenv('CLICKHOUSE_DB', 'fraud_detection'),
        help='Название базы данных (по умолчанию: fraud_detection или из переменной окружения CLICKHOUSE_DB)'
    )
    parser.add_argument(
        '--user',
        type=str,
        default=os.getenv('CLICKHOUSE_USER', 'default'),
        help='Пользователь ClickHouse (по умолчанию: default или из переменной окружения CLICKHOUSE_USER)'
    )
    parser.add_argument(
        '--password',
        type=str,
        default=os.getenv('CLICKHOUSE_PASSWORD') if 'CLICKHOUSE_PASSWORD' in os.environ else None,
        help='Пароль ClickHouse (по умолчанию: из переменной окружения CLICKHOUSE_PASSWORD)'
    )
    
    args = parser.parse_args()
    
    success = execute_query_and_export(
        args.clickhouse_host,
        args.clickhouse_port,
        args.database,
        args.query_file,
        args.output_csv,
        args.user,
        args.password
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()

