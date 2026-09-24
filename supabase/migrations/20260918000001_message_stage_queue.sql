-- Message Staging Queue (rework 397fc42e — was 805c8e51)
-- v1 core: FIFO per-chat staging between platform intake and conversation loop
-- E-RLS: ENABLE + FORCE RLS, revoke anon/authenticated in THIS migration
--
-- Idempotent: creates table + columns + indexes if they don't already exist
-- (the table may already be on DEV from a prior out-of-VC provision)

-- 1. Create table IF NOT EXISTS
CREATE TABLE IF NOT EXISTS public.message_stage_queue (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_key  text NOT NULL,                 -- composite key for the active session (e.g. "telegram:1942013343")
  platform     text NOT NULL DEFAULT 'telegram',
  chat_id      text NOT NULL,                 -- platform chat identifier
  bot_name     text NOT NULL DEFAULT '',       -- adapter/bot name for logging
  message_id   text NOT NULL DEFAULT '',       -- platform message id (dedupe key)
  sender_id    text NOT NULL DEFAULT '',       -- auth.users id or platform user id of sender
  sender_name  text NOT NULL DEFAULT '',       -- display name of sender
  content      jsonb,                          -- original message payload (legacy alias for event)
  event        jsonb,                          -- serialised MessageEvent for reconstruction
  status       text NOT NULL DEFAULT 'staged' CHECK (status IN ('staged','processing','done','dead')),
  seq          bigint NOT NULL GENERATED ALWAYS AS IDENTITY,  -- FIFO ordering key
  attempts     smallint NOT NULL DEFAULT 0,
  max_attempts smallint NOT NULL DEFAULT 3,
  last_error   text,
  created_at   timestamptz NOT NULL DEFAULT now(),
  released_at  timestamptz,
  delivered_at timestamptz,
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- 2. Add any columns missing from a prior out-of-VC provision
DO $$
BEGIN
  -- Columns that the code expects but may not exist if table was provisioned with a different schema
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='message_stage_queue' AND column_name='session_key') THEN
    ALTER TABLE public.message_stage_queue ADD COLUMN session_key text NOT NULL DEFAULT '';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='message_stage_queue' AND column_name='bot_name') THEN
    ALTER TABLE public.message_stage_queue ADD COLUMN bot_name text NOT NULL DEFAULT '';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='message_stage_queue' AND column_name='sender_id') THEN
    ALTER TABLE public.message_stage_queue ADD COLUMN sender_id text NOT NULL DEFAULT '';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='message_stage_queue' AND column_name='sender_name') THEN
    ALTER TABLE public.message_stage_queue ADD COLUMN sender_name text NOT NULL DEFAULT '';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='message_stage_queue' AND column_name='event') THEN
    ALTER TABLE public.message_stage_queue ADD COLUMN event jsonb;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='message_stage_queue' AND column_name='seq') THEN
    ALTER TABLE public.message_stage_queue ADD COLUMN seq bigint GENERATED ALWAYS AS IDENTITY;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='message_stage_queue' AND column_name='max_attempts') THEN
    ALTER TABLE public.message_stage_queue ADD COLUMN max_attempts smallint NOT NULL DEFAULT 3;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='message_stage_queue' AND column_name='last_error') THEN
    ALTER TABLE public.message_stage_queue ADD COLUMN last_error text;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='message_stage_queue' AND column_name='released_at') THEN
    ALTER TABLE public.message_stage_queue ADD COLUMN released_at timestamptz;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='message_stage_queue' AND column_name='delivered_at') THEN
    ALTER TABLE public.message_stage_queue ADD COLUMN delivered_at timestamptz;
  END IF;
END$$;

-- 3. One message in flight per chat: at most one row with status='processing'
--    (DROP IF EXISTS first since the constraint may already exist from a prior provision)
DROP INDEX IF EXISTS public.idx_msq_one_processing;
CREATE INDEX idx_msq_one_processing ON public.message_stage_queue (chat_id, (CASE WHEN status='processing' THEN 1 ELSE 0 END))
  WHERE status = 'processing';

-- 4. FIFO: pop next staged for a session
CREATE INDEX IF NOT EXISTS idx_msq_pop
  ON public.message_stage_queue (session_key, seq, created_at)
  WHERE status = 'staged';

-- 5. Dedupe by platform message id
CREATE UNIQUE INDEX IF NOT EXISTS idx_msq_platform_msg
  ON public.message_stage_queue (chat_id, message_id)
  WHERE message_id IS NOT NULL AND message_id != '';

-- 6. Dead-letter scan
CREATE INDEX IF NOT EXISTS idx_msq_dead
  ON public.message_stage_queue (status, updated_at)
  WHERE status = 'dead';

-- 7. Depth count index
CREATE INDEX IF NOT EXISTS idx_msq_depth
  ON public.message_stage_queue (session_key, status)
  WHERE status = 'staged';

-- E-RLS: ENABLE + FORCE ROW LEVEL SECURITY in the CREATING migration
ALTER TABLE public.message_stage_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.message_stage_queue FORCE ROW LEVEL SECURITY;

-- Revoke all grants from anon and authenticated (writers use service role bypassrls)
REVOKE ALL ON public.message_stage_queue FROM anon;
REVOKE ALL ON public.message_stage_queue FROM authenticated;

-- Grant only service_role (bypassrls) and postgres full access
-- (No public read — the queue is internal gateway state, not user-facing data)
