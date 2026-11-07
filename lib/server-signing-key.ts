import { createHmac } from "node:crypto"
import { getBootstrapConfig } from "@/lib/config/bootstrap"

const DERIVATION_LABEL = "usertasks/server-signing-key/v1"

/**
 * Key for internal HMAC signatures: queued workflow resumes and queued run
 * initiators. It is SUPABASE_SERVICE_ROLE_KEY when that is set. A deployment
 * without Supabase (the standalone profile) has no such key, so the key is
 * derived from ENCRYPTION_KEY, or from CRON_SECRET when ENCRYPTION_KEY is not
 * set. The derived value is an HMAC of a fixed label, so it never equals the
 * secret it comes from. Returns undefined when none of the three is set.
 */
export function serverSigningKey(): string | undefined {
  const config = getBootstrapConfig()
  if (config.supabase.serviceRoleKey) return config.supabase.serviceRoleKey
  const base = config.encryptionKey || config.cronSecret
  if (!base) return undefined
  return createHmac("sha256", base).update(DERIVATION_LABEL).digest("hex")
}
