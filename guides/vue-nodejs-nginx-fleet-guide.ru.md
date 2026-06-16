---
title: "Управление веб-стеком Vue + Node.js + nginx через AI и remote-agents: практический гайд"
description: "Пошаговое руководство по удалённому управлению фронтендом Vue, двумя бэкенд-хостами Node.js и edge-сервером nginx из одного MCP-интерфейса. Деплой, мониторинг логов, перезапуск и автономные задачи через AI."
keywords: "remote agents, MCP, удалённое управление серверами, деплой Vue, деплой Node.js, мониторинг логов nginx, fleet management, DevOps AI, Claude, opencode, управление парком серверов"
slug: vue-nodejs-nginx-fleet-guide
lang: ru
author: remote-agents
date: 2026-06-16
---

# Управление веб-стеком Vue + Node.js + nginx через AI: практический гайд по remote-agents

Как из **одного интерфейса** деплоить фронтенд на Vue, держать два бэкенд-хоста
на Node.js в синхроне и следить за логами nginx — не открывая четыре SSH-сессии,
а просто отдавая команды AI-ассистенту (Claude или opencode). В этом руководстве
по **удалённому управлению парком серверов** мы разберём реальную топологию
веб-приложения и покажем готовые рецепты на базе MCP-сервера
[`remote-agents`](https://www.npmjs.com/package/remote-agents).

> **Коротко:** на каждом хосте запущен лёгкий агент, подключённый к зашифрованной
> комнате релея. AI видит весь парк как один компьютер и вызывает инструменты
> `exec`, `fleet_exec`, `fleet_git`, `read_file`, `schedule_add`, `task_dispatch`
> и другие. Полезные нагрузки шифруются end-to-end — релей форвардит их вслепую.

## Содержание

1. [Архитектура: четыре хоста, одна комната](#архитектура)
2. [Установка агентов и теги](#установка)
3. [Сценарий 1 — фронтенд: сборка и деплой Vue](#vue)
4. [Сценарий 2 — бэкенд: деплой Node.js на два хоста сразу](#nodejs)
5. [Сценарий 3 — edge: правка конфига и перезагрузка nginx](#nginx)
6. [Сценарий 4 — мониторинг логов всего стека](#логи)
7. [Сценарий 5 — автономная задача: хост сам чинит сборку](#автономная)
8. [Безопасность: режимы plan / edit / bypass](#безопасность)
9. [FAQ](#faq)

---

<a name="архитектура"></a>
## Архитектура: четыре хоста, одна комната

Возьмём типичный продакшен небольшого SaaS:

| Хост | Роль | Тег | Стек |
|------|------|-----|------|
| `vue-dev` | Сборка и предпросмотр фронтенда | `frontend` | Vue 3, Vite, npm |
| `node-api-1` | Бэкенд-инстанс #1 | `api` | Node.js, PM2 |
| `node-api-2` | Бэкенд-инстанс #2 | `api` | Node.js, PM2 |
| `edge-nginx` | Reverse proxy + раздача статики | `edge` | nginx |

Все четыре агента подключены к одной комнате релея (например `webstack`). Теги
позволяют адресовать команды **группе хостов** одной операцией: `target="api"`
ударит сразу по двум Node-хостам, `target="os:linux"` — по всем Linux-машинам,
`target="all"` — по всему парку.

```
            ┌────────────── AI (Claude / opencode) ──────────────┐
            │            remote-agents (MCP, stdio)               │
            └───────────────────────┬────────────────────────────┘
                                    │ wss:// (E2E-шифрование)
                            ┌───────┴────────┐  relay (CF Worker или self-host)
                            │   room=webstack │
            ┌───────────────┼─────────┬───────────────┬───────────┐
        vue-dev        node-api-1  node-api-2      edge-nginx
        (frontend)        (api)       (api)          (edge)
```

<a name="установка"></a>
## Установка агентов и теги

На каждом хосте ставим пакет и запускаем агент с нужным тегом:

```bash
# один раз на каждой машине
npm i -g remote-agents

# фронтенд-хост
remote-agents run --relay wss://<релей> --room webstack --token <secret> \
  --name vue-dev --tags frontend

# бэкенд-хосты
remote-agents run ... --name node-api-1 --tags api
remote-agents run ... --name node-api-2 --tags api

# edge
remote-agents run ... --name edge-nginx --tags edge
```

Проверяем, что весь парк на связи. В чате с AI:

> «Покажи агентов в комнате»

Под капотом вызывается `list_agents` и возвращает список с ОС, distro, тегами и —
если вышла новая версия — полем `update_available`.

---

<a name="vue"></a>
## Сценарий 1 — фронтенд: сборка и деплой Vue

Задача: собрать production-бандл Vue на `vue-dev` и выложить статику на edge.

**Шаг 1. Подтянуть код и собрать.** Просишь AI:

> «На vue-dev: подтяни main, поставь зависимости и собери прод-бандл Vue»

Срабатывают:

```text
git_pull   agent_id=vue-dev  repo=/srv/frontend  remote=origin  branch=main
exec       agent_id=vue-dev  command="npm ci && npm run build"  cwd=/srv/frontend  timeout_ms=300000
```

**Шаг 2. Проверить, что бандл собрался.**

```text
list_dir   agent_id=vue-dev  path=/srv/frontend/dist
read_file  agent_id=vue-dev  path=/srv/frontend/dist/index.html
```

**Шаг 3. Доставить статику на edge.** Самый простой путь — собрать архив на
`vue-dev`, прочитать его и записать на `edge-nginx` (или класть в общий каталог,
который уже монтируется на edge). Минимальный вариант через `exec` + rsync, если
между хостами есть доступ:

```text
exec  agent_id=vue-dev  command="rsync -az --delete /srv/frontend/dist/ deploy@edge:/var/www/app/"
```

> **Совет по SEO для самого Vue-приложения:** если нужен SSR/пререндер ради
> индексации, добавь `vite-ssg` или Nuxt и встрой шаг пререндера в `npm run build`
> — тогда edge будет отдавать уже готовый HTML с мета-тегами.

---

<a name="nodejs"></a>
## Сценарий 2 — бэкенд: деплой Node.js на два хоста сразу

Главная экономия времени — **fleet-операции**. Оба Node-хоста помечены тегом
`api`, поэтому деплой идёт одной командой на оба.

**Шаг 1. Синхронный git pull на обоих инстансах:**

```text
fleet_git  target="api"  op=pull  repo=/srv/api  remote=origin  branch=main
```

**Шаг 2. Установить зависимости и перезапустить PM2 без даунтайма:**

```text
fleet_exec target="api"  command="cd /srv/api && npm ci --omit=dev && pm2 reload api --update-env"
```

`pm2 reload` делает rolling-restart, поэтому пользователи не ловят разрыв.

**Шаг 3. Хелс-чек (health-check) обоих инстансов:**

```text
fleet_exec target="api"  command="curl -fsS http://127.0.0.1:3000/health || echo UNHEALTHY"
```

Ответ приходит **по-хостно**: видно, что `node-api-1` вернул `200`, а
`node-api-2`, например, `UNHEALTHY` — и сразу понятно, куда смотреть. Одна
упавшая машина не «топит» весь батч.

**Шаг 4. Точечный разбор проблемного инстанса:**

```text
exec       agent_id=node-api-2  command="pm2 logs api --lines 100 --nostream"
read_file  agent_id=node-api-2  path=/srv/api/.env       # только в режиме чтения!
```

---

<a name="nginx"></a>
## Сценарий 3 — edge: правка конфига и перезагрузка nginx

Edge-хост `edge-nginx` проксирует API на оба Node-инстанса и раздаёт статику Vue.

**Шаг 1. Прочитать текущий конфиг:**

```text
read_file  agent_id=edge-nginx  path=/etc/nginx/conf.d/app.conf
```

**Шаг 2. Обновить апстрим (балансировка на два бэкенда).** Просишь AI вписать
блок — срабатывает `write_file` (в режиме `edit` создаётся бэкап оригинала):

```text
write_file agent_id=edge-nginx  path=/etc/nginx/conf.d/app.conf  content="""
upstream api_pool {
    server 10.0.0.11:3000;   # node-api-1
    server 10.0.0.12:3000;   # node-api-2
    keepalive 32;
}
server {
    listen 80;
    server_name app.example.com;
    root /var/www/app;            # статика Vue
    location / { try_files $uri $uri/ /index.html; }   # SPA-fallback
    location /api/ { proxy_pass http://api_pool/; }
}
"""
```

**Шаг 3. Проверить синтаксис и перезагрузить без обрыва соединений:**

```text
exec  agent_id=edge-nginx  command="nginx -t && systemctl reload nginx"
```

`nginx -t` валидирует конфиг до перезагрузки — если синтаксис битый, `reload`
не выполнится и прод не упадёт.

---

<a name="логи"></a>
## Сценарий 4 — мониторинг логов всего стека

Тут система раскрывается: **наблюдение за логами сразу со всех хостов**.

**Быстрый срез ошибок по бэкенду:**

```text
fleet_exec target="api"  command="tail -n 200 /srv/api/logs/app.log | grep -iE 'error|exception' | tail -n 20"
```

**Ошибки и коды 5xx на edge:**

```text
exec  agent_id=edge-nginx  command="tail -n 500 /var/log/nginx/access.log | awk '$9 ~ /^5/ {print}' | tail -n 30"
exec  agent_id=edge-nginx  command="tail -n 100 /var/log/nginx/error.log"
```

**Регулярный мониторинг через cron прямо на хосте.** Запланируем ежечасную
сводку ошибок на edge — она работает **даже если связь с релеем пропала**, потому
что планировщик живёт на самом агенте (cron — 6-польный: `сек мин час день мес дн_нед`):

```text
schedule_add  agent_id=edge-nginx  name=hourly-5xx \
  cron="0 0 * * * *" \
  command="grep -c ' 5[0-9][0-9] ' /var/log/nginx/access.log >> /var/log/nginx/5xx-hourly.log"
```

**Агрегация логов по всему парку через MapReduce.** Хочешь топ IP-адресов по
всем хостам? `map_fn` гоняется на каждом воркере со своей партицией, `reduce_fn`
сворачивает результаты:

```text
mapreduce
  data=["node-api-1:/srv/api/logs/access.log", "node-api-2:/srv/api/logs/access.log", "edge-nginx:/var/log/nginx/access.log"]
  map_fn="awk '{print $1}' \"$(cut -d: -f2)\" | sort | uniq -c"
  reduce_fn="sort -rn | head -n 20"
```

Падающие партиции автоматически переотправляются (`max_retries`), так что один
недоступный хост не ломает весь отчёт.

---

<a name="автономная"></a>
## Сценарий 5 — автономная задача: хост сам чинит сборку

Если на хосте включён автономный режим и установлен свой AI-CLI (со своим
логином), можно **делегировать задачу целиком** — она выполнится кредами хоста,
не тратя твои токены:

```text
task_dispatch  agent_id=vue-dev \
  prompt="Сборка Vue падает на типах в src/api/client.ts. Найди причину, поправь, прогони npm run build и npm run test, закоммить с понятным сообщением."
```

Ты получаешь `task_id` **сразу**, а сам прогон идёт в фоне. Когда хост
завершит — он пушит событие `TaskCompleted`, и привязанное крон-напоминание
**отменяется автоматически**. Результат забираешь позже:

```text
task_get  agent_id=vue-dev  id=<task_id>
```

Реальный кейс: «пусть `vue-dev` ночью сам доведёт зелёную сборку», а утром ты
читаешь её результат и diff.

---

<a name="безопасность"></a>
## Безопасность: режимы plan / edit / bypass

Прод трогаем аккуратно. У каждого агента есть **режим работы**:

- **`plan`** — только чтение (`read_file`, `git_status`, безопасный `exec`).
  Идеален для разбора инцидента на проде, ничего не сломаешь.
- **`edit`** — запись разрешена, перед перезаписью создаётся **бэкап**.
- **`bypass`** — полный доступ без ограничений.
- **`disabled`** — агент заблокирован.

```text
set_mode  agent_id=edge-nginx  mode=plan    # сначала смотрим
# ... разобрались, нужно править конфиг ...
set_mode  agent_id=edge-nginx  mode=edit    # правим с бэкапом
# ... вернули обратно ...
set_mode  agent_id=edge-nginx  mode=plan
```

Жёсткий denylist (например `/etc/shadow`, `/boot`) действует **даже в bypass**, а
все полезные нагрузки команд и результатов шифруются end-to-end.

---

<a name="faq"></a>
## FAQ

**Чем это отличается от Ansible или Kubernetes?**
Это не замена декларативным плейбукам или оркестратору контейнеров. `remote-agents` —
интерактивный и автономный доступ к shell, файлам, git и cron на множестве хостов
через единый AI-интерфейс с E2E-шифрованием. Лучше всего подходит для небольших
парков, dev/стейджинг-окружений и оперативных задач.

**Нужен ли облачный сервис?**
Нет. Можно использовать готовый Cloudflare-релей или поднять свой Rust-релей
(`remote-agents-relay`) и указать агентам `ws://свой-хост:8080`.

**Безопасно ли отдавать команды на прод?**
Старт в режиме `plan` (только чтение), denylist на критичные пути работает всегда,
payload'ы шифруются, а релей форвардит их вслепую (ключей не видит).

**Как обновлять агентов на всём парке?**
Инструмент `fleet_update_check` подскажет, у каких простаивающих хостов вышла
новая версия, после чего на них выполняется `npm i -g remote-agents@latest`.

**Можно ли адресовать команду по типу ОС?**
Да: `target="os:linux"` или `target="os:macos"`. Также по тегам
(`target="api"`) или всем сразу (`target="all"`).

---

## Итог

Один MCP-интерфейс, четыре хоста, ноль ручных SSH-сессий: фронтенд Vue
собирается и деплоится, два инстанса Node.js обновляются одной fleet-командой,
nginx правится с бэкапом и валидацией, а логи всего стека сводятся в один отчёт.
Добавь автономные задачи и cron на агентах — и парк серверов начинает работать
как один управляемый AI компьютер.

> Установка: `npm i -g remote-agents` →
> [пакет на npm](https://www.npmjs.com/package/remote-agents) ·
> [исходники и документация](https://github.com/47-ronn/tunshell_mcp_agents)
