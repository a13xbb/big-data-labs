import streamlit as st
import json
import time
import numpy as np
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from sklearn.metrics import accuracy_score, f1_score
from streamlit_autorefresh import st_autorefresh

st.title("ML Results Visualization")

# Автообновление страницы каждые 2 секунды
st_autorefresh(interval=2000, limit=100, key="fizzbuzzcounter")

# Конфигурация Kafka-консюмера
conf = {
    'bootstrap.servers': 'localhost:9095',  # Подключаемся к брокеру (тот же, что используется для ML-консюмера)
    'group.id': 'streamlit-ml-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['ml_results'])

# Инициализируем переменные в session_state для накопления данных
if 'y_true' not in st.session_state:
    st.session_state.y_true = []
if 'y_pred' not in st.session_state:
    st.session_state.y_pred = []
if 'metrics_history' not in st.session_state:
    st.session_state.metrics_history = pd.DataFrame(columns=['Timestamp', 'Accuracy', 'F1'])

# Опрашиваем Kafka в течение 1 секунды
start_time = time.time()
while time.time() - start_time < 1:
    msg = consumer.poll(0.5)
    if msg is None:
        continue
    if msg.error():
        # Игнорируем EOF-ошибки
        if msg.error().code() != KafkaError._PARTITION_EOF:
            st.error(f"Consumer error: {msg.error()}")
        continue
    # Обрабатываем сообщение
    try:
        result = json.loads(msg.value().decode('utf-8'))
        # Предполагается, что сообщение имеет формат:
        # {'y_true': [...], 'y_pred': [...]}
        st.session_state.y_true.extend(result.get('y_true', []))
        st.session_state.y_pred.extend(result.get('y_pred', []))
    except Exception as e:
        st.error(f"Ошибка обработки сообщения: {e}")

consumer.close()

# Если накоплены данные, вычисляем метрики и обновляем историю
if len(st.session_state.y_true) > 0:
    acc = accuracy_score(st.session_state.y_true, st.session_state.y_pred)
    f1 = f1_score(st.session_state.y_true, st.session_state.y_pred, average='weighted')
    st.metric("Accuracy", f"{acc:.2f}")
    st.metric("F1 Score", f"{f1:.2f}")

    # Обновляем историю метрик
    new_row = pd.DataFrame({
        'Timestamp': [time.strftime("%H:%M:%S")],
        'Accuracy': [acc],
        'F1': [f1]
    })
    st.session_state.metrics_history = pd.concat(
        [st.session_state.metrics_history, new_row],
        ignore_index=True
    )
    # Строим линейный график истории
    history_df = st.session_state.metrics_history.set_index('Timestamp')
    st.line_chart(history_df)
else:
    st.write("Ожидаем данные...")
