import streamlit as st
import json
from confluent_kafka import Consumer
from sklearn.metrics import f1_score, accuracy_score
import matplotlib.pyplot as plt
import numpy as np

st.set_page_config(page_title="ML Results", layout="wide")

if 'y_true' not in st.session_state:
    st.session_state['y_true'] = []

if 'y_pred' not in st.session_state:
    st.session_state['y_pred'] = []

if 'num_processed' not in st.session_state:
    st.session_state['num_processed'] = []

conf = {'bootstrap.servers': 'localhost:9095',
        'group.id': 'ml-results',
        'auto.offset.reset': 'earliest' }

consumer = Consumer(conf)
consumer.subscribe(['ml_results'])

st.title('ML results')

chart_holder_f1 = st.empty()
chart_holder_accuracy = st.empty()
chart_holder_amount = st.empty()

f1_scores = []
acc_scores = []

while True:
    msg = consumer.poll(1000)

    try:
        data = json.loads(msg.value().decode('utf-8'))
    except(Exception):
        continue

    if data:

        st.session_state['y_true'].extend(data['y_true'])
        st.session_state['y_pred'].extend(data['y_pred'])
        st.session_state['num_processed'].append(len(st.session_state['y_true']))

        st.text(np.array(st.session_state['y_true']).shape)
        st.text(np.array(st.session_state['y_pred']).shape)

        f1 = f1_score(st.session_state['y_true'], st.session_state['y_pred'], average='micro')
        f1_scores.append(f1)
        chart_holder_f1.line_chart(f1_scores, y_label='f1 score')

        acc = accuracy_score(st.session_state['y_true'], st.session_state['y_pred'])
        acc_scores.append(acc)
        chart_holder_accuracy.line_chart(acc_scores, y_label='accuracy')

        chart_holder_amount.line_chart(st.session_state['num_processed'], y_label='num processed')



