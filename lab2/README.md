1) Датасет: https://www.kaggle.com/datasets/sriharshaeedala/financial-fraud-detection-dataset
2) C помощью скриптов start_1node.sh и start_3nodes.sh запускаются докер контейнеры с hadoop и spark либо с 1 либо c 3 DataNode.
   Внутри эти скрипты запускают docker-compose файлы и копируют csv файл в файловую систему hadoop, csv файл должен лежать в корне проекта.
3) src/spark_app.py - приложение на pyspark с обработкой csv-файла.
4) Запустить spark_app.py можно с помощью команды
   docker exec -it spark-master spark-submit --master spark://spark-master:7077 spark_app.py -d hdfs://namenode:9000/{filename}.csv
   Ключи: -d/--data_path - путь до csv файла, -o/--optimized - использовать ли оптимизации в spark приложении.
5) В файле plots.ipynb представлены графики для времени исполнения программы и использования памяти.

- Когда запущены 3 spark-worker и 3 DataNode, они должны отображаться в spark ui (http://localhost:8080) и hadoop ui (http://localhost:9870) соответственно:
  ![image](https://github.com/user-attachments/assets/e842de41-3a46-4937-9230-cd9c0f9be031)
  ![image](https://github.com/user-attachments/assets/79872ac0-eb33-46f9-a3ba-a23dfb17d142)


