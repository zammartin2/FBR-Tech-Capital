Bank Ecosystem Monorepo - Структура проекта

```bash
bank-ecosystem/
├── 📁 apps/                          # Микросервисы приложения
│   ├── web-banking-bff/             # BFF для веб-банкинга
│   ├── mobile-app/                  # Backend для мобильного приложения  
│   ├── open-banking-gateway/        # Open Banking API шлюз
│   ├── backoffice-console/          # Административная панель
│   ├── payments-orchestrator/       # Оркестратор платежей
│   ├── ledger-service/              # Сервис главной книги
│   ├── accounts-service/            # Управление счетами
│   ├── identity-service/            # Аутентификация и авторизация
│   ├── aml-service/                 # AML проверки
│   ├── risk-fraud-service/          # Анализ рисков и мошенничества
│   ├── cards-issuing-service/       # Эмиссия карт
│   ├── loans-service/               # Кредитные продукты
│   ├── treasury-service/            # Казначейство
│   ├── notifications-service/       # Уведомления
│   ├── pricing-service/             # Тарифы и комиссии
│   └── reporting-service/           # Отчетность
├── 📁 packages/                      # Внутренние пакеты
│   ├── contracts/                   # Контракты API и событий
│   ├── event-bus/                   # Event Bus (Kafka)
│   ├── ledger-kernel/               # Ядро бухгалтерии
│   ├── money/                       # Работа с денежными суммами
│   ├── identity-sdk/                # SDK для аутентификации
│   ├── validation/                  # Валидация данных
│   ├── observability/               # Логирование, метрики, трейсинг
│   ├── persistence/                 # Базы данных (PostgreSQL, Redis)
│   ├── crypto/                      # Криптографические операции
│   ├── policy/                      # Бизнес-политики
│   └── ui-kit/                      # React компоненты
├── 📁 infra/                         # Инфраструктура
│   ├── k8s/                         # Kubernetes манифесты
│   ├── terraform/                   # Terraform для облака
│   ├── opa-policies/                # Open Policy Agent политики
│   ├── cicd/                        # CI/CD конфигурации
│   └── docker-compose.yml           # Локальная разработка
├── 📁 data/                          # Данные и миграции
│   ├── schemas/                     # JSON Schema, Protobuf
│   ├── migrations/                  # Миграции БД
│   └── fixtures/                    # Тестовые данные
├── 📁 docs/                          # Документация
│   ├── ADR/                         # Architecture Decision Records
│   ├── threat-models/               # Модели угроз безопасности
│   └── runbooks/                    # Руководства по эксплуатации
├── 📁 .github/workflows/             # GitHub Actions
├── package.json                     # Корневой package.json
├── pnpm-workspace.yaml             # Monorepo конфигурация
├── turbo.json                      # TurboRepo сборка
└── tsconfig.base.json              # Базовый TypeScript config
```
🏦 Описание проекта
Bank Ecosystem - это современная микросервисная банковская платформа, построенная по принципам Domain-Driven Design (DDD) и Event-Driven Architecture (EDA).

🎯 Основные характеристики:
Архитектура:

🏗️ Microservices - 15+ специализированных сервисов

📡 Event-Driven - Kafka-based event bus

🗃️ CQRS/ES - Command Query Responsibility Segregation

🔒 Security-First - Zero Trust архитектура

Технологический стек:

Backend: Node.js + TypeScript + Fastify

Базы данных: PostgreSQL + Redis

Брокер сообщений: Kafka (Redpanda)

Мониторинг: OpenTelemetry + Prometheus + Grafana

Инфраструктура: Kubernetes + Terraform

CI/CD: GitHub Actions

Ключевые домены:

Identity & Access - Аутентификация, SAML, OAuth2

Core Banking - Счета, транзакции, главная книга

Payments - Платежи, переводы, эквайринг

Risk & Compliance - AML, KYC, фрод-мониторинг

Products - Карты, кредиты, депозиты

Operations - Казначейство, отчетность, уведомления
