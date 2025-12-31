import streamlit as st
import pandas as pd
import numpy as np
from kafka import KafkaProducer
import json
import time
import os
import uuid
import psycopg2

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

# Конфигурация PostgreSQL
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "fraud_detection"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres")
}

RESULTS_LIMIT = os.getenv("RESULTS_LIMIT", 100)

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        # Генерация уникальных ID для всех транзакций
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            # Отправляем данные вместе с ID
            producer.send(
                topic, 
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)
            
        producer.flush()
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

def get_results_from_db(limit=100):
    """Получение результатов скоринга из PostgreSQL"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        query = """
            SELECT transaction_id, score, fraud_flag, created_at
            FROM fraud_scores
            ORDER BY created_at DESC
            LIMIT %s
        """
        df = pd.read_sql_query(query, conn, params=(limit,))
        conn.close()
        return df
    except Exception as e:
        st.error(f"Ошибка подключения к базе данных: {str(e)}")
        return None

# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Интерфейс
tab1, tab2 = st.tabs(["📤 Отправка данных", "📊 Результаты скоринга"])

with tab1:
    st.title("📤 Отправка данных в Kafka")
    
    uploaded_file = st.file_uploader(
        "Загрузите CSV файл с транзакциями",
        type=["csv"]
    )
    
    if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
        st.session_state.uploaded_files[uploaded_file.name] = {
            "status": "Загружен",
            "df": load_file(uploaded_file)
        }
        st.success(f"Файл {uploaded_file.name} успешно загружен!")
    
    if st.session_state.uploaded_files:
        st.subheader("🗂 Список загруженных файлов")
        
        for file_name, file_data in st.session_state.uploaded_files.items():
            cols = st.columns([4, 2, 2])
            
            with cols[0]:
                st.markdown(f"**Файл:** `{file_name}`")
                st.markdown(f"**Статус:** `{file_data['status']}`")
            
            with cols[2]:
                if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                    if file_data["df"] is not None:
                        with st.spinner("Отправка..."):
                            success = send_to_kafka(
                                file_data["df"],
                                KAFKA_CONFIG["topic"],
                                KAFKA_CONFIG["bootstrap_servers"]
                            )
                            if success:
                                st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                                st.rerun()
                    else:
                        st.error("Файл не содержит данных")

with tab2:
    st.title("📊 Результаты скоринга")
    
    if st.button("Посмотреть результаты", key="view_results"):
        with st.spinner("Загрузка результатов из базы данных..."):
            results_df = get_results_from_db(limit=RESULTS_LIMIT)
            
            if results_df is not None and not results_df.empty:
                st.success(f"Загружено записей: {len(results_df)}")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Всего транзакций", len(results_df))
                with col2:
                    fraud_count = results_df['fraud_flag'].sum()
                    st.metric("Мошеннических", fraud_count)
                with col3:
                    fraud_percent = (fraud_count / len(results_df) * 100) if len(results_df) > 0 else 0
                    st.metric("Процент мошенничества", f"{fraud_percent:.2f}%")

                fraud_only_df = results_df[results_df['fraud_flag'] == 1]
                fraud_only_df = fraud_only_df.head(10)
                if not fraud_only_df.empty:
                    st.subheader("Только мошеннические транзакции")
                    st.dataframe(fraud_only_df, use_container_width=True)
                else:
                    st.info("Мошеннических транзакций не найдено.")

                
                st.subheader("Статистика по скорам")
                bins = np.linspace(0, 1, 21)
                hist, edges = np.histogram(results_df['score'], bins=bins)
                
                bin_labels = []
                for i in range(len(hist)):
                    bin_labels.append(f"{edges[i]:.2f}-{edges[i+1]:.2f}")
                
                chart_df = pd.DataFrame({
                    "Score диапазон": bin_labels,
                    "Количество": hist.tolist()
                })
                
                st.bar_chart(chart_df.set_index("Score диапазон"), use_container_width=True)
            elif results_df is not None and results_df.empty:
                st.info("В базе данных пока нет результатов скоринга. Отправьте транзакции для обработки.")
            else:
                st.error("Не удалось загрузить результаты. Проверьте подключение к базе данных.")