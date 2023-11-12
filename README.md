# Проект 5-го спринта "Создание витрины для расчёта оплаты курьерам"

### Описание
В этом проекте была поставлена задача - создание витрины для расчёта оплаты каждому курьеру за предыдущий месяц. Витрина должна содержать готовые данные, которые бизнес будет использовать для расчётов.

### Структура репозитория
- `/src/dags`
- `/src/ddl`


### Как запустить контейнер
Запустите локально команду:

```
docker run \
-d \
-p 3000:3000 \
-p 3002:3002 \
-p 15432:5432 \
--mount src=airflow_sp5,target=/opt/airflow \
--mount src=lesson_sp5,target=/lessons \
--mount src=db_sp5,target=/var/lib/postgresql/data \
--name=de-sprint-5-server-local \
cr.yandex/crp1r8pht0n0gl25aug1/de-pg-cr-af:latest
```

После того как запустится контейнер, вам будут доступны:
- Airflow
	- `localhost:3000/airflow`
- БД
	- `jovyan:jovyan@localhost:15432/de`
