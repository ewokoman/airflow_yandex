# ETL процесс с помощью Airflow на python
Используя API exchangerate.host подготовл ETL процесс с помощью Airflow на python, для выгрузки данных по валютной паре BTC/USD (Биткоин к доллару), выгрузка идет с шагом 3 часа и записывать данные в БД (валютная пара, дата, текущий курс). для исторических данных выгрузка идет каждый день
____
## Запуск AIRFLOW и его подготовка к работе

Необходимо скопировать к себе репозиторий и перейти в него.

Далее запустить команду 
```docker-compose up -d```
Подождать 2 минтуы пока запустяттся все службы.
Зайти в AIRFLOW по ссылке http://localhost:8001 
Логин ```admin``` пароль ```admin```
Перейти в Admin -> Connections и создать коннект для базы postgres с параметрами как на фото и паролем ```postgres```
![Alt-текст](https://github.com/ewokoman/photo/blob/28ade6f8a6d443cd1f3f27cdf8816b5205305f80/2022-04-28_12-56-11.png "база коннеект")
____
## Подготовка DAG для загрузки исторических данных  
Зайти в файл airflow/dags/historical_data.py и поставить в 24 строке вчерашнюю дату запуска. Сохранить файл.

____
## Запуск DAG

На главной странице запустить DAG c именами data и historical_data

____
## Валидация процесса загрузки

Работа historical_data

![Alt-текст](https://github.com/ewokoman/photo/blob/28ade6f8a6d443cd1f3f27cdf8816b5205305f80/2022-04-28_13-16-53.png "historical_data")

Работа data

![Alt-текст](https://github.com/ewokoman/photo/blob/master/2022-04-28_13-17-21.png "data")

____
## Проверка заполняемости бд 

Перейти по ссылке http://localhost:8282
Выбрать систему PostgreSql
Во всех полях вписать ```postgres```
![Alt-текст](https://github.com/ewokoman/photo/blob/28ade6f8a6d443cd1f3f27cdf8816b5205305f80/2022-04-28_13-18-24.png "sql_connect")

Нажать на таблицу btc_usd_rate
Выбрать Select data
После чего будет видно наполнение талицы
![Alt-текст](https://github.com/ewokoman/photo/blob/28ade6f8a6d443cd1f3f27cdf8816b5205305f80/2022-04-28_13-17-49.png "sql")



