/**
 * FBR Tech Capital — Accounts Service
 * Автор: Фабрициус В.Н. (Fabritsius V.N.)
 * Назначение: управление счетами, публикация событий, расчёт остатков (minor units)
 *
 * Правила игры:
 * -  Деньги считаем в минорных единицах по ISO-4217 (копейки/центы/филсы).
 * -  Источник истины баланса — таблица account_balances; Redis — лишь зеркало с TTL.
 * -  События разделены: account.opened — отдельно, журнал проводок — отдельно.
 * -  Идемпотентность обязательна: processed_events для входящих, Idempotency-Key для API.
 * -  Kafka: ключ сообщения = accountId или journalId для порядка в партиции; ошибки → DLQ.
 */

import Fastify from 'fastify';
import { logger } from '@packages/observability';
import { healthRouter } from './routes/health';
import { accountsRouter } from './routes/accounts';
import { transactionsRouter } from './routes/transactions';
import { kafka } from '@packages/event-bus';
import { pg, redis } from '@packages/persistence';
import {
  ACCOUNTS_ACCOUNT_OPENED_V1,
  LEDGER_JOURNAL_POSTED_V1
} from '@packages/contracts';
import { z } from 'zod';

/* ==========================
    Денежные утилиты (ISO-4217)
   ========================== */
const CURRENCY_DP: Record<string, number> = {
  EUR: 2, USD: 2, GBP: 2, RUB: 2, JPY: 0, KWD: 3,
  CHF: 2, CAD: 2, AUD: 2, CNY: 2, INR: 2, BRL: 2
};
const pow10 = (n: number): number => Math.pow(10, n);
const toMinor = (amount: number, currency: string): number =>
  Math.round(Number(amount) * pow10(CURRENCY_DP[currency] ?? 2));
const fromMinor = (minor: number, currency: string): number =>
  Number(minor) / pow10(CURRENCY_DP[currency] ?? 2);

/* ==========================
   Валидация входных DTO/событий
   ========================== */
const CreateAccountSchema = z.object({
  customerId: z.string().uuid(),
  currency: z.enum(['EUR', 'USD', 'RUB', 'GBP', 'JPY', 'KWD']),
  accountType: z.enum(['CURRENT', 'SAVINGS', 'DEPOSIT']),
  initialBalance: z.number().nonnegative().default(0)
});

const LedgerEntrySchema = z.object({
  accountId: z.string(),
  amountMinor: z.number().int(),
  side: z.enum(['DEBIT', 'CREDIT']),
  description: z.string().optional()
});

const LedgerPostedV1Schema = z.object({
  eventId: z.string().uuid(),
  occurredAt: z.string().datetime(),
  journalId: z.string().uuid(),
  entries: z.array(LedgerEntrySchema),
  metadata: z.record(z.unknown()).optional()
});
type LedgerPostedV1 = z.infer<typeof LedgerPostedV1Schema>;

/* ==========================
   Бухгалтерская семантика (Актив/Пассив)
   ========================== */
// nostro:* — активы банка; клиентские счета — пассивы банка.
const getAccountNature = (accountId: string): 'ASSET' | 'LIABILITY' => {
  if (accountId.startsWith('nostro:')) return 'ASSET';
  return 'LIABILITY';
};
/** Актив:  Дт +, Кт - ; Пассив: Дт -, Кт + */
const calculateDelta = (
  side: 'DEBIT' | 'CREDIT',
  nature: 'ASSET' | 'LIABILITY',
  amountMinor: number
): number => {
  if (nature === 'ASSET') return side === 'DEBIT' ? +amountMinor : -amountMinor;
  return side === 'DEBIT' ? -amountMinor : +amountMinor;
};

/* ==========================
    Приложение Fastify
   ========================== */
const app = Fastify({ logger });

// Роутеры
app.register(healthRouter, { prefix: '/health' });
app.register(accountsRouter, { prefix: '/api/v1/accounts' });
app.register(transactionsRouter, { prefix: '/api/v1/transactions' });

/* ==========================
    Kafka
   ========================== */
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'accounts-service' });

/* ==========================
    Инициализация схем БД
   ========================== */
async function initDatabase() {
  await pg.query(`
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

    CREATE TABLE IF NOT EXISTS accounts (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      customer_id UUID NOT NULL,
      account_number VARCHAR(32) NOT NULL,
      currency TEXT NOT NULL,
      account_type TEXT NOT NULL,
      status TEXT NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
      CONSTRAINT accounts_account_number_uniq UNIQUE (account_number),
      CONSTRAINT accounts_status_chk CHECK (status IN ('ACTIVE','BLOCKED','CLOSED')),
      CONSTRAINT accounts_type_chk   CHECK (account_type IN ('CURRENT','SAVINGS','DEPOSIT'))
    );

    CREATE TABLE IF NOT EXISTS account_balances (
      account_id UUID PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
      balance_minor BIGINT NOT NULL DEFAULT 0,
      updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS processed_events (
      id UUID PRIMARY KEY,
      processed_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS idempotency_keys (
      service VARCHAR(50) NOT NULL,
      key     VARCHAR(255) NOT NULL,
      account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      PRIMARY KEY (service, key)
    );

    CREATE INDEX IF NOT EXISTS idempotency_keys_created_at_idx ON idempotency_keys(created_at);
  `);

  //  В проде вынести в pg_cron; здесь — простой job по часу
  setupIdempotencyCleanupJob();
}

function setupIdempotencyCleanupJob() {
  setInterval(async () => {
    try {
      const res = await pg.query(
        `DELETE FROM idempotency_keys WHERE created_at < NOW() - INTERVAL '24 hours'`
      );
      if (res.rowCount) app.log.info({ cleaned: res.rowCount }, 'Idempotency keys cleaned');
    } catch (e) {
      app.log.error({ e }, 'Idempotency keys cleanup failed');
    }
  }, 60 * 60 * 1000);
}

/* ==========================
    Инициализация Kafka (с ретраями)
   ========================== */
async function initKafka() {
  const maxRetries = 3;
  let lastError: Error | undefined;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await producer.connect();
      await consumer.connect();

      await consumer.subscribe({ topic: LEDGER_JOURNAL_POSTED_V1, fromBeginning: false });

      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          try {
            const raw = JSON.parse(message.value?.toString() || '{}');
            const event = LedgerPostedV1Schema.parse(raw);
            app.log.info({ topic, eventId: event.eventId }, 'Received ledger event');

            if (topic === LEDGER_JOURNAL_POSTED_V1) await handleJournalPosted(event);
          } catch (error) {
            app.log.error({ error, topic, partition, offset: message.offset }, 'Kafka message failed');

            //  DLQ: складываем всё с контекстом и ошибкой/валидацией
            try {
              const raw = JSON.parse(message.value?.toString() || '{}');
              await producer.send({
                topic: `${LEDGER_JOURNAL_POSTED_V1}.dlq`,
                messages: [{
                  key: raw?.eventId || 'unknown',
                  value: JSON.stringify({
                    topic, partition, offset: message.offset,
                    error: String(error),
                    rawEvent: raw,
                    processedAt: new Date().toISOString(),
                    validationError: error instanceof z.ZodError ? error.errors : null
                  })
                }]
              });
            } catch (dlqError) {
              app.log.error({ dlqError }, 'DLQ send failed');
            }
          }
        }
      });

      app.log.info('Kafka initialized');
      return;
    } catch (error) {
      lastError = error as Error;
      app.log.warn({ attempt, error }, 'Kafka init retry...');
      if (attempt < maxRetries) await new Promise(r => setTimeout(r, 2000 * attempt));
    }
  }
  throw new Error(`Kafka init failed after ${maxRetries} attempts: ${lastError?.message}`);
}

/* ==========================
    Применение журнала (идемпотентно, транзакционно)
   ========================== */
async function handleJournalPosted(event: LedgerPostedV1) {
  const client = await pg.connect();
  try {
    await client.query('BEGIN');

    //  Идемпотентность входящего события
    const dedup = await client.query(
      'INSERT INTO processed_events (id) VALUES ($1) ON CONFLICT (id) DO NOTHING RETURNING 1',
      [event.eventId]
    );
    if (dedup.rowCount === 0) {
      app.log.info({ eventId: event.eventId }, 'Duplicate journal — skip');
      await client.query('ROLLBACK');
      return;
    }

    // Атомарно применяем все проводки
    for (const entry of event.entries) {
      const { accountId, amountMinor, side } = entry;
      const nature = getAccountNature(accountId);
      const delta = calculateDelta(side, nature, amountMinor);

      await client.query(`
        INSERT INTO account_balances (account_id, balance_minor)
        VALUES ($1, $2)
        ON CONFLICT (account_id)
        DO UPDATE SET
          balance_minor = account_balances.balance_minor + EXCLUDED.balance_minor,
          updated_at = NOW()
      `, [accountId, delta]);

      // Зеркалим в Redis с TTL
      const res = await client.query(
        'SELECT balance_minor FROM account_balances WHERE account_id = $1',
        [accountId]
      );
      if (res.rows[0]) {
        await redis.set(
          `account:${accountId}:balance_minor`,
          String(res.rows[0].balance_minor),
          { EX: 300 }
        );
      }
    }

    await client.query('COMMIT');
    app.log.info({ eventId: event.eventId }, 'Journal applied');
  } catch (error) {
    await client.query('ROLLBACK');
    app.log.error({ error, eventId: event.eventId }, 'Journal apply failed');
    throw error;
  } finally {
    client.release();
  }
}

/* ==========================
    Интерфейсы домена
   ========================== */
interface CreateAccountRequest {
  customerId: string;
  currency: string;
  accountType: 'CURRENT' | 'SAVINGS' | 'DEPOSIT';
  initialBalance?: number;
}
interface Account {
  id: string;
  customerId: string;
  accountNumber: string;
  currency: string;
  accountType: string;
  status: 'ACTIVE' | 'BLOCKED' | 'CLOSED';
  createdAt: string;
  updatedAt: string;
}

/* ==========================
    Генерация номера счёта
   ========================== */
// Псевдослучайный с таймстемпом; от коллизий — UNIQUE + ретраи на 23505.
function generateAccountNumber(): string {
  const timestamp = Date.now().toString().slice(-8);
  const random = Math.floor(Math.random() * 10000).toString().padStart(4, '0');
  return `40702${timestamp}${random}`;
}

/* ==========================
    Бизнес-логика (декоратор Fastify)
   ========================== */
app.decorate('accountService', {
  /**
   * Открывает счёт. Идемпотентно по Idempotency-Key.
   * Публикует accounts.account.opened.v1; стартовый депозит — отдельным журналом.
   */
  async openAccount(data: CreateAccountRequest, idempotencyKey?: string): Promise<Account> {
    const validated = CreateAccountSchema.parse(data);
    const client = await pg.connect();

    try {
      await client.query('BEGIN');

      // Идемпотентность API: один ключ — один счёт
      if (idempotencyKey) {
        const existing = await client.query(
          'SELECT account_id FROM idempotency_keys WHERE service = $1 AND key = $2',
          ['accounts-service', idempotencyKey]
        );
        if (existing.rows[0]) {
          const acc = await client.query('SELECT * FROM accounts WHERE id = $1', [existing.rows[0].account_id]);
          if (acc.rows[0]) {
            await client.query('COMMIT');
            app.log.info({ accountId: acc.rows[0].id, idempotencyKey }, 'Idempotent return');
            return acc.rows[0];
          }
        }
      }

      let attempts = 0;
      const maxAttempts = 3;

      // Ретраи при unique_violation (23505)
      while (attempts < maxAttempts) {
        const accountId = require('crypto').randomUUID();
        const accountNumber = generateAccountNumber();

        try {
          const insert = await client.query(
            `INSERT INTO accounts (
              id, customer_id, account_number, currency, account_type, status
            ) VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *`,
            [
              accountId,
              validated.customerId,
              accountNumber,
              validated.currency,
              validated.accountType,
              'ACTIVE'
            ]
          );
          const account = insert.rows[0];

          // Запоминаем идемпотентность
          if (idempotencyKey) {
            await client.query(
              'INSERT INTO idempotency_keys (service, key, account_id) VALUES ($1, $2, $3)',
              ['accounts-service', idempotencyKey, accountId]
            );
          }

          // Событие: счёт открыт (occuredAt — время отправки события, createdAt — из БД)
          await producer.send({
            topic: ACCOUNTS_ACCOUNT_OPENED_V1,
            messages: [{
              key: accountId,
              value: JSON.stringify({
                eventId: require('crypto').randomUUID(),
                occurredAt: new Date().toISOString(),
                account: {
                  id: accountId,
                  customerId: validated.customerId,
                  accountNumber,
                  currency: validated.currency,
                  accountType: validated.accountType,
                  status: 'ACTIVE',
                  createdAt: account.created_at
                }
              })
            }]
          });

          // Стартовый депозит — только журналом (без прямого апдейта остатков)
          const initialMinor = toMinor(validated.initialBalance, validated.currency);
          if (initialMinor > 0) {
            await producer.send({
              topic: LEDGER_JOURNAL_POSTED_V1,
              messages: [{
                key: accountId, // порядок по клиентскому счёту
                value: JSON.stringify({
                  eventId: require('crypto').randomUUID(),
                  occurredAt: new Date().toISOString(),
                  journalId: require('crypto').randomUUID(),
                  entries: [
                    { accountId: `nostro:${validated.currency}`, amountMinor: initialMinor, side: 'DEBIT',  description: 'Initial deposit funding' },
                    { accountId: accountId,                      amountMinor: initialMinor, side: 'CREDIT', description: 'Initial deposit' }
                  ],
                  metadata: { source: 'accounts-service', accountOpening: true, currency: validated.currency }
                })
              }]
            });
          }

          await client.query('COMMIT');
          app.log.info({ accountId: account.id }, 'Account opened');
          return account;

        } catch (error: any) {
          if (error.code === '23505') {
            attempts++;
            await client.query('ROLLBACK');
            app.log.warn({ attempts, detail: error.detail }, 'Unique violation — retry');
            continue;
          }
          throw error;
        }
      }

      throw new Error(`Failed to generate unique account number after ${maxAttempts} attempts`);
    } catch (error) {
      await client.query('ROLLBACK');
      if (error instanceof z.ZodError) {
        const err = new Error(
          `Validation failed: ${error.errors.map(e => `${e.path.join('.')}: ${e.message}`).join(', ')}`
        );
        (err as any).statusCode = 400;
        throw err;
      }
      throw error;
    } finally {
      client.release();
    }
  },

  /** Карточка счёта */
  async getAccount(accountId: string): Promise<Account | null> {
    const r = await pg.query('SELECT * FROM accounts WHERE id = $1 AND status != $2', [accountId, 'CLOSED']);
    return r.rows[0] || null;
  },

  /** Баланс счёта с валютой (истина — в БД; Redis — кэш) */
  async getBalance(accountId: string): Promise<{ balance: number; currency: string }> {
    const acc = await pg.query('SELECT currency FROM accounts WHERE id = $1', [accountId]);
    if (!acc.rows[0]) throw new Error('Account not found');

    const currency = acc.rows[0].currency;
    const cached = await redis.get(`account:${accountId}:balance_minor`);
    if (cached) return { balance: fromMinor(Number(cached), currency), currency };

    const r = await pg.query('SELECT balance_minor FROM account_balances WHERE account_id = $1', [accountId]);
    const minor = r.rows[0] ? Number(r.rows[0].balance_minor) : 0;

    await redis.set(`account:${accountId}:balance_minor`, String(minor), { EX: 300 });
    return { balance: fromMinor(minor, currency), currency };
  },

  /** Список активных счетов клиента */
  async getCustomerAccounts(customerId: string): Promise<Account[]> {
    const r = await pg.query(
      'SELECT * FROM accounts WHERE customer_id = $1 AND status = $2 ORDER BY created_at DESC',
      [customerId, 'ACTIVE']
    );
    return r.rows;
  },

  /** Блокировка счёта (и инвалидация кэша) */
  async blockAccount(accountId: string, reason: string): Promise<void> {
    const client = await pg.connect();
    try {
      await client.query('BEGIN');
      const r = await client.query(
        'UPDATE accounts SET status = $1, updated_at = NOW() WHERE id = $2 AND status = $3',
        ['BLOCKED', accountId, 'ACTIVE']
      );
      if (r.rowCount === 0) throw new Error('Account not found or already blocked/closed');

      await redis.del(`account:${accountId}:balance_minor`);
      await client.query('COMMIT');
      app.log.info({ accountId, reason }, 'Account blocked');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }
});

/* ==========================
    Graceful shutdown
   ========================== */
async function gracefulShutdown(signal: string) {
  app.log.info({ signal }, 'Shutdown signal');
  try {
    await app.close();
    await consumer.stop();
    await consumer.disconnect();
    await producer.disconnect();
    try {
      await redis.quit();
    } catch (e: any) {
      if (e?.message !== 'The client is closed') throw e;
    }
    await pg.end();
    app.log.info('Service shutdown completed');
    process.exit(0);
  } catch (error) {
    app.log.error({ error }, 'Shutdown error');
    process.exit(1);
  }
}
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT',  () => gracefulShutdown('SIGINT'));

/* ==========================
    Старт сервиса
   ========================== */
async function start() {
  try {
    await pg.query('SELECT 1');     // БД на месте?
    await redis.ping();             // Redis жив?
    await initDatabase();           // Схемы/индексы/cleanup
    await initKafka();              // Kafka + consumer.run

    const PORT = Number(process.env.PORT || 3002);
    const HOST = process.env.HOST || '0.0.0.0';
    await app.listen({ port: PORT, host: HOST });
    app.log.info({ port: PORT, host: HOST }, 'Accounts Service started');
  } catch (error) {
    app.log.error({ error }, 'Service start failed');
    process.exit(1);
  }
}

/* ==========================
    Расширение типов Fastify
   ========================== */
declare module 'fastify' {
  interface FastifyInstance {
    accountService: {
      openAccount(data: CreateAccountRequest, idempotencyKey?: string): Promise<Account>;
      getAccount(accountId: string): Promise<Account | null>;
      getBalance(accountId: string): Promise<{ balance: number; currency: string }>;
      getCustomerAccounts(customerId: string): Promise<Account[]>;
      blockAccount(accountId: string, reason: string): Promise<void>;
    };
  }
}

start();
