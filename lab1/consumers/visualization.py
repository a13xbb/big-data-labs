import streamlit as st
import json
from confluent_kafka import Consumer
from sklearn.metrics import f1_score, accuracy_score
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import confusion_matrix

# st.set_page_config(page_title="ML Results", layout="wide")

labels = {
    1 : 'Spruce/Fir',
    2 : 'Lodgepole Pine',
    3 : 'Ponderosa Pine',
    4 : 'Cottonwood/Willow',
    5 : 'Aspen',
    6 : 'Douglas-fir',
    7 : 'Krummholz',
}

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

chart_f1 = st.empty()
chart_accuracy = st.empty()
hist_chart = st.empty()
conf_matrix_placeholder = st.empty()

f1_scores_overall = []
acc_scores_overall = []
f1_scores_batches = []
acc_scores_batches = []

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

        #гистограмма распределения классов
        class_counts = pd.Series(st.session_state['y_true']).value_counts().sort_index()
        fig, ax = plt.subplots(figsize=(4, 3))
        colors = ["red", "blue", "green", "purple", "orange", "cyan", "brown"]
        ax.bar(class_counts.index, class_counts.values, color=colors)
        ax.set_xlabel("Classes", fontsize=7)
        ax.set_ylabel("Count", fontsize=7)
        ax.set_title("Ground truth class distribution in test dataset", fontsize=10)
        ax.set_xticks([i + 1 for i in range(7)])
        ax.set_xticklabels(list(labels.values()), rotation=60, fontsize=7)
        hist_chart.pyplot(fig)
        plt.close(fig)

        #график f1-score
        fig, ax = plt.subplots(figsize=(4, 3))
        f1_cur = f1_score(data['y_true'], data['y_pred'], average='micro')
        f1_scores_batches.append(f1_cur)
        ax.plot([i for i in range(len(f1_scores_batches))], f1_scores_batches)
        ax.set_title('F1 scores / batches', fontsize=10)
        ax.set_ylabel('F1-score (micro)', fontsize=7)
        ax.set_ylim((0.3, 1))
        ax.grid(True, linestyle="--", alpha=0.6)
        ticks = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        ax.set_yticks(ticks)
        ax.set_yticklabels(list(map(str, ticks)), fontsize=8)
        chart_f1.pyplot(fig)
        plt.close(fig)

        #график accuracy
        fig, ax = plt.subplots(figsize=(4, 3))
        acc_cur = accuracy_score(data['y_true'], data['y_pred'])
        acc_scores_batches.append(acc_cur)
        ax.plot([i for i in range(len(acc_scores_batches))], acc_scores_batches)
        ax.set_title('Accuracy scores / batches', fontsize=10)
        ax.set_ylabel('Accuracy', fontsize=7)
        ax.set_ylim((0.3, 1))
        ax.grid(True, linestyle="--", alpha=0.6)
        ax.set_yticks(ticks)
        ax.set_yticklabels(list(map(str, ticks)), fontsize=8)
        chart_accuracy.pyplot(fig)
        plt.close(fig)

        #confusion matrix
        cm = confusion_matrix(st.session_state['y_true'], st.session_state['y_pred'], labels=list(labels.keys()))
        
        fig, ax = plt.subplots(figsize=(6, 4))
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax)
        ax.set_xticklabels(list(labels.values()), rotation=60, fontsize=7)
        ax.set_yticklabels(list(labels.values()), rotation=0, fontsize=7)
        ax.set_xlabel("Predicted labels", fontsize=7)
        ax.set_ylabel("True labels", fontsize=7)
        ax.set_title("Confusion Matrix", fontsize=10)

        conf_matrix_placeholder.pyplot(fig)
        plt.close(fig)



