# BotDailyUpdate
Данный скрипт отчета нужен для автоматической отправки аналитической сводки ленты новостей в чат в телеграм каждое утро. 

Для этого я:
1) Создала своего телеграм-бота с помощью @BotFather
2) Создала сам отчет, в котором присутствует текст с информацией о значениях ключевых метрик за предыдущий день (DAU, просмотры, лайки, CTR);
график с значениями метрик за предыдущие 7 дней;
3) Автоматизировала отправку отчета с помощью Airflow.  
