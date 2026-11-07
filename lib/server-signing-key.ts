import { createHmac } from "node:crypto"
import { getBootstrapConfig } from "@/lib/config/bootstrap"

const DERIVATION_LABEL = "usertasks/server-signing-key/v1"

/**
 * Key for internal HMAC signatures: queued workflow resumes and queued run
 * initiators. It is SUPABASE_SERVICE_ROLE_KEY when that is set. A deployment
 * without Supabase (the standalone profile) has no such key, so the key is
 * derived from ENCRYPTION_KEY. The derived value is an HMAC of a fixed label,
 * so it never equals ENCRYPTION_KEY. CRON_SECRET is not used: external
 * schedulers hold it, so it must not sign internal jobs. Returns undefined
 * when neither key is set.
 *
 * A change of the key (for example, SUPABASE_SERVICE_ROLE_KEY set later, or a
 * new ENCRYPTION_KEY) makes the signatures of jobs already in the queue fail.
 * Set the key before the first run, and drain the queue before a change.
 */
export function serverSigningKey(): string | undefined {
  const config = getBootstrapConfig()
  if (config.supabase.serviceRoleKey) return config.supabase.serviceRoleKey
  const base = config.encryptionKey
  if (!base) return undefined
  return createHmac("sha256", base).update(DERIVATION_LABEL).digest("hex")
}
