#!/bin/bash

# Создание БД
sleep 10
airflow upgradedb
# cоздание admin user
sleep 10
airflow create_user -r Admin -u admin -f viktor -l ponomarev -p admin -e ewokoman@yandex.ru
sleep 10
# Запуск шедулера и вебсервера
airflow scheduler & airflow webserver
