import {
  Client,
  GatewayIntentBits,
  Partials,
  PermissionsBitField,
  EmbedBuilder,
  AuditLogEvent
} from 'discord.js';
import fs from 'node:fs/promises'; // <-- added

// === NEW EXTRA IMPORTS (non-destructive) ===
import {
  ButtonBuilder,
  ButtonStyle,
  ActionRowBuilder,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  ChannelType,
  PermissionFlagsBits
} from 'discord.js';

// ==== CONFIG (hard-coded) ====
const DISCORD_TOKEN = ''.trim();     // raw token, no "Bot " prefix
const SOURCE_ROLE_ID = '1433193580809289915'; //traders use mm
const TARGET_ROLE_ID = '1433205845814153298'; // default if no argument is provided
// optional: log to a fixed channel; leave '' to log where +save is used
const LOG_CHANNEL_ID = '1431477498100715650';
// optional: only give to members who can view a specific channel; leave '' to ignore
const CHANNEL_TO_CHECK = '1432475517977759926';
const VOUCH_CHANNEL_ID = '1433208019952341154';
const VOUCH_INTERVAL_MS = 20; // 60_000 = 1 minute
const REINVITE_URL = 'https://discord.gg/sab-mm';
// ==================== NEW: allowed users ====================
const ALLOWED_FILE = 'allowedUsers.txt';

// =============================

if (!DISCORD_TOKEN || !DISCORD_TOKEN.includes('.')) {
  console.error('Token missing or malformed.');
  process.exit(1);
}

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,   // needed to fetch all members
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,  // to read +save
    // ==== ADDED: Required for guildBanAdd to fire ====
    GatewayIntentBits.GuildBans
  ],
  partials: [Partials.Channel]
});

// ==================== NEW: blacklist/whitelist utilities ====================
const BLACKLIST_FILE = 'blacklist.txt';
const WHITELIST_FILE = 'whitelist.txt';

// ==================== NEW: tickets registry ====================
const TICKETS_FILE = 'tickets.txt'; // store ticket channel IDs, one per line

async function isAllowedUser(userId) {
  try {
    const set = await readSet(ALLOWED_FILE); // you already have readSet()
    return set.has(String(userId));
  } catch {
    return false; // if file missing/unreadable, treat as no access
  }
}

// simple file-backed set helpers
async function readSet(file) {
  try {
    const raw = await fs.readFile(file, 'utf8');
    return new Set(
      raw
        .split(/\r?\n/)
        .map(s => s.trim())
        .filter(Boolean)
    );
  } catch {
    return new Set();
  }
}
async function writeSet(file, set) {
  const out = [...set].join('\n') + (set.size ? '\n' : '');
  await fs.writeFile(file, out, 'utf8');
}
async function addIds(file, ids) {
  const s = await readSet(file);
  for (const id of ids) if (/^\d{5,}$/.test(id)) s.add(id);
  await writeSet(file, s);
  return s;
}
async function removeIds(file, ids) {
  const s = await readSet(file);
  for (const id of ids) s.delete(id);
  await writeSet(file, s);
  return s;
}
function parseUserId(str) {
  if (!str) return null;
  const id = str.replace(/[<@!>#&]/g, '').replace(/[^0-9]/g, '');
  return /^\d{5,}$/.test(id) ? id : null;
}

// ===== NEW: ticket channel helpers (non-destructive) =====
async function isTicketChannelId(id) {
  try {
    const set = await readSet(TICKETS_FILE);
    return set.has(String(id));
  } catch {
    return false;
  }
}
async function addTicketChannelId(id) {
  try {
    await addIds(TICKETS_FILE, [String(id)]);
  } catch {
    // ignore
  }
}
// =========================================================

let LOG_CHAN_OBJ = null;
async function getLogChannel(guild, fallback) {
  if (LOG_CHANNEL_ID) {
    try {
      return await guild.channels.fetch(LOG_CHANNEL_ID);
    } catch {
      return fallback ?? null;
    }
  }
  return fallback ?? null;
}
function makeEmbed(title, description, color = 0xff0033) {
  return new EmbedBuilder().setColor(color).setTitle(title).setDescription(description).setTimestamp(new Date());
}
function isProtectedId(guild, id) {
  // don't touch owner or the bot itself
  return id === guild.ownerId || id === client.user.id;
}
// ==========================================================================

// ==================== NEW: Role state snapshot & helpers ====================
const ORIGINAL_ROLE_STATE = new Map(); // guildId -> Map(roleId -> { position, permissions })
const REVERTING_ROLES = new Set();     // roleIds currently being reverted (prevents loops)

async function snapshotGuildRoles(guild) {
  try {
    await guild.roles.fetch();
    const map = new Map();
    guild.roles.cache.forEach(role => {
      map.set(role.id, {
        position: role.position,
        permissions: role.permissions.bitfield
      });
    });
    ORIGINAL_ROLE_STATE.set(guild.id, map);
  } catch (e) {
    console.warn(`snapshotGuildRoles error for ${guild.id}:`, e?.message || e);
  }
}

function getOriginalRoleState(guildId, roleId) {
  const gmap = ORIGINAL_ROLE_STATE.get(guildId);
  return gmap ? gmap.get(roleId) : undefined;
}

function setOriginalRoleState(guildId, role) {
  if (!ORIGINAL_ROLE_STATE.has(guildId)) ORIGINAL_ROLE_STATE.set(guildId, new Map());
  ORIGINAL_ROLE_STATE.get(guildId).set(role.id, {
    position: role.position,
    permissions: role.permissions.bitfield
  });
}

function removeOriginalRoleState(guildId, roleId) {
  const gmap = ORIGINAL_ROLE_STATE.get(guildId);
  if (gmap) gmap.delete(roleId);
}
// ==========================================================================

/* ========================= NEW: FEATURE FLAGS SYSTEM =========================
   Supports: ban, kick, botadd, channel, roleUpdate, roleDelete
   Commands will toggle these. Defaults: all enabled.
=============================================================================*/
const FEATURES_FILE = 'features.json';
const DEFAULT_FEATURES = {
  ban: true,          // guildBanAdd watcher
  kick: true,         // guildMemberRemove (kick) watcher
  botadd: true,       // guildMemberAdd (bot) watcher
  channel: true,      // channelDelete watcher
  roleUpdate: true,   // roleUpdate revert/bypass
  roleDelete: true    // roleDelete blacklist
};
let FEATURES = { ...DEFAULT_FEATURES };

async function loadFeatures() {
  try {
    const raw = await fs.readFile(FEATURES_FILE, 'utf8');
    const obj = JSON.parse(raw);
    FEATURES = { ...DEFAULT_FEATURES, ...obj };
  } catch {
    FEATURES = { ...DEFAULT_FEATURES };
    await saveFeatures(); // persist defaults if file missing
  }
}
async function saveFeatures() {
  await fs.writeFile(FEATURES_FILE, JSON.stringify(FEATURES, null, 2), 'utf8');
}
function normalizeFeatureKey(k) {
  if (!k) return null;
  const s = k.toLowerCase().trim();
  if (['ban', 'bans'].includes(s)) return 'ban';
  if (['kick', 'kicks'].includes(s)) return 'kick';
  if (['botadd', 'bot', 'bots'].includes(s)) return 'botadd';
  if (['channel', 'channels', 'ch'].includes(s)) return 'channel';
  if (['roleupdate', 'role', 'role-update', 'role_upd', 'ru'].includes(s)) return 'roleUpdate';
  if (['roledelete', 'role-del', 'rd'].includes(s)) return 'roleDelete';
  return null;
}
function listFeaturesPretty() {
  return Object.entries(FEATURES)
    .map(([k, v]) => `• **${k}**: ${v ? 'ENABLED' : 'DISABLED'}`)
    .join('\n');
}
/* ========================================================================== */

// ============== NEW: Global helpers for ticket buttons & vouch ticker =======
const TICKET_BUTTON_IDS = {
  claim: 'ticket_claim',
  close: 'ticket_close'
};
// Will treat MM_ALLOWED_ROLE as the "helper" role per your request
// (everyone with helper role can use profit commands).
// If you have a separate helper role, replace the ID below.
const HELPER_ROLE_ID = '1431477370019254353'; // alias of MM_ALLOWED_ROLE (set again below for clarity)

// Vouch ticker controller (wrapping existing ticker)
let VOUCH_TIMER = null;
// ⚙️ Make the configured interval behave as SECONDS if small, else treat as ms.
let VOUCH_MS = (typeof VOUCH_INTERVAL_MS === 'number'
  ? (VOUCH_INTERVAL_MS >= 1000 ? VOUCH_INTERVAL_MS : Math.round(VOUCH_INTERVAL_MS * 1000))
  : 60000);
let VOUCH_RUNNING = false;
let VOUCH_CHANNEL_OBJ = null;
async function startVouchTicker(clientInst) {
  // Always clear any older timer to ensure reliable restarts
  if (VOUCH_TIMER) {
    clearInterval(VOUCH_TIMER);
    VOUCH_TIMER = null;
  }
  VOUCH_RUNNING = false;

  // Resolve the channel if not cached
  try {
    VOUCH_CHANNEL_OBJ = await clientInst.channels.fetch(VOUCH_CHANNEL_ID);
  } catch (_) {
    console.warn('Vouch ticker: channel not found or no access:', VOUCH_CHANNEL_ID);
    return;
  }
  if (!VOUCH_CHANNEL_OBJ) return;
  const guild = VOUCH_CHANNEL_OBJ.guild;

  let lastFullFetch = 0;
  const FULL_FETCH_COOLDOWN_MS = 30 * 60_000;

  const postOnce = async () => {
    try {
      const now = Date.now();
      if (guild.members.cache.size < 100 || now - lastFullFetch > FULL_FETCH_COOLDOWN_MS) {
        await guild.members.fetch();
        lastFullFetch = now;
      }
      const randomMemberId = pickRandomFromSet(guild.members.cache.keys());
      if (!randomMemberId) return;

      const randomFileId = await pickRandomFromFile('user_ids.txt');
      if (!randomFileId) return;

      const embed = new EmbedBuilder()
        .setColor(0x00B8FF)
        .setTitle(`Recovered Vouch`)
        .setDescription(
          `<@${randomMemberId}> vouched <@${randomFileId}>\n\n` +
          `Service Rating: ⭐⭐⭐⭐⭐\n\n` +
          `Request a MM here --> <#1433261459483263027>`
        )
        .setTimestamp(new Date());

      await VOUCH_CHANNEL_OBJ.send({ embeds: [embed] });
    } catch (e) {
      console.warn('Vouch ticker error:', e?.message || e);
    }
  };

  await postOnce();
  VOUCH_TIMER = setInterval(postOnce, VOUCH_MS);
  VOUCH_RUNNING = true;
}

function stopVouchTicker() {
  if (VOUCH_TIMER) {
    clearInterval(VOUCH_TIMER);
    VOUCH_TIMER = null;
  }
  VOUCH_RUNNING = false;
}

// ==== NEW: add a global error handler to avoid crashing on unhandled errors ====
client.on('error', (err) => {
  try {
    console.warn('Client error:', err?.message || err);
  } catch {}
});
// ==============================================================================

client.once('ready', async () => {
  console.log(`Logged in as ${client.user.tag}`);

  // Cache a log channel reference from the first available guild that can see it
  try {
    for (const [, g] of client.guilds.cache) {
      LOG_CHAN_OBJ = await getLogChannel(g, null);
      if (LOG_CHAN_OBJ) break;
    }
  } catch {}

  // --- NEW: snapshot roles on startup for all guilds ---
  try {
    for (const [, g] of client.guilds.cache) {
      await snapshotGuildRoles(g);
    }
  } catch (e) {
    console.warn('Initial role snapshot error:', e?.message || e);
  }

  // --- NEW: load feature flags on startup ---
  try {
    await loadFeatures();
    const anyGuild = client.guilds.cache.first();
    if (anyGuild) {
      const logc = await getLogChannel(anyGuild, LOG_CHAN_OBJ);
      if (logc) {
        await logc.send({
          embeds: [makeEmbed('🧩 Feature Flags Loaded', listFeaturesPretty(), 0x60a5fa)]
        });
      }
    }
  } catch (e) {
    console.warn('Feature flags load error:', e?.message || e);
  }

  // --- NEW: vouch ticker (controllable) ---
  await startVouchTicker(client);
  // --- END NEW: vouch ticker ---

  // ==================== NEW: Always-on blacklist ENFORCER ====================
  // ==================== REPLACE: Always-on blacklist ENFORCER ====================
const enforce = async () => {
  try {
    const [black, white] = await Promise.all([readSet(BLACKLIST_FILE), readSet(WHITELIST_FILE)]);

    for (const [, guild] of client.guilds.cache) {
      // optional: quick cache warm-up for recently active members only (avoids full fetch)
      // await guild.members.fetch({ withPresences: false }).catch(() => {});

      for (const id of black) {
        if (white.has(id)) continue;
        if (isProtectedId(guild, id)) continue;

        // Check if the user is currently in the guild
        let member = guild.members.cache.get(id);
        if (!member) {
          try {
            member = await guild.members.fetch(id);
          } catch {
            member = null; // not in guild
          }
        }
        if (!member) continue; // only ban if they are actually inside

        // Try to ban the member
               try {
          await guild.bans.create(id, { reason: 'Auto-blacklist enforcement (present in guild)' });
          const logc = await getLogChannel(guild, LOG_CHAN_OBJ);
          if (logc) {
            await logc.send({
              embeds: [
                makeEmbed('🚫 Auto-Banned (blacklist - present)', `<@${id}> \`${id}\` was banned (was in guild).`)
              ]
            });
          }
        } catch {
          // ignore failures (insufficient perms, hierarchy, already banned race, etc.)
        }
      }
    }
  } catch {
    // swallow; try again next tick
  }
};

// Run every second, as requested
setInterval(enforce, 1000);
// ============================================================================== 

  // ==========================================================================

});

// keep tiny utility
const wait = (ms) => new Promise(r => setTimeout(r, ms));

// ==================== NEW: Audit Log Watchers → auto-blacklist ====================
async function addExecutorToBlacklist(guild, executorId, reason) {
  if (!executorId) return;
  if (isProtectedId(guild, executorId)) return;

  const whitelist = await readSet(WHITELIST_FILE);
  if (whitelist.has(executorId)) return;

  await addIds(BLACKLIST_FILE, [executorId]);

  const logc = await getLogChannel(guild, LOG_CHAN_OBJ);
  if (logc) {
    await logc.send({
      embeds: [makeEmbed('🛑 Added to Blacklist', `<@${executorId}> \`${executorId}\`\nReason: ${reason}`, 0xcc0000)]
    });
  }
}
// Auto-undo bans ONLY if a non-bot, non-whitelisted executor did the ban
client.on('guildBanAdd', async (ban) => {
  console.log("Person banned");
  // ===== FEATURE FLAG GUARD =====
  try { if (!FEATURES.ban) return; } catch {}
  // ==============================
  try {
    const guild = ban.guild;
    const me = guild?.members?.me ?? await guild.members.fetchMe().catch(() => null);
    if (!me || !me.permissions.has(PermissionsBitField.Flags.ViewAuditLog)) return;

    // 🔁 NEW: retry once if audit entry not immediately present
    const fetchEntry = async () => {
      const logs = await guild.fetchAuditLogs({ type: AuditLogEvent.MemberBanAdd, limit: 5 });
      const entry = logs.entries.find(e => e.target?.id === ban.user.id && (Date.now() - e.createdTimestamp < 60_000));
      return entry || null;
    };

    let entry = await fetchEntry();
    if (!entry) {
      await wait(1500);
      entry = await fetchEntry();
    }
    if (!entry) return;

    const execId = entry.executor?.id;
    if (!execId) return;

    // ✅ Skip if the bot itself did the ban (the enforcer)
    if (execId === client.user.id) return;

    // ✅ Also skip if executor is whitelisted
    const whitelist = await readSet(WHITELIST_FILE);
    if (whitelist.has(execId)) return;

    // NEW: ensure we can unban
    if (!me.permissions.has(PermissionsBitField.Flags.BanMembers)) {
      const logc = await getLogChannel(guild, LOG_CHAN_OBJ);
      if (logc) {
        await logc.send({
          embeds: [makeEmbed('⚠️ Ban Detected (cannot reverse)', `Executor: <@${execId}> \`${execId}\`\nReason: Missing **Ban Members** permission.`, 0xf59e0b)]
        });
      }
      return;
    }

    // Attempt to unban
    try {
      await guild.bans.remove(ban.user.id, 'Auto-undo: ban by non-whitelisted executor');
      const logc = await getLogChannel(guild, LOG_CHAN_OBJ);
      if (logc) {
        await logc.send({ embeds: [makeEmbed('🔓 Auto-Unbanned', `Reversed ban of <@${ban.user.id}> \`${ban.user.id}\``, 0x22c55e)] });
      }
    } catch (e) {
      const logc = await getLogChannel(guild, LOG_CHAN_OBJ);
      if (logc) {
        await logc.send({ embeds: [makeEmbed('❌ Failed to Unban', `Tried to reverse ban of \`${ban.user.id}\`.\nError: ${String(e).slice(0,180)}`, 0xcc0000)] });
      }
      return; // don't blacklist executor if we couldn't reverse (optional)
    }

    // NEW: Blacklist the executor after reversing
    await addExecutorToBlacklist(guild, execId, `Unauthorized ban of ${ban.user?.tag || ban.user.id}`);
  } catch {}
});


// When a channel is deleted
client.on('channelDelete', async (channel) => {
  // ===== FEATURE FLAG GUARD =====
  try { if (!FEATURES.channel) return; } catch {}
  // ==============================
  try {
    const guild = channel.guild;
    if (!guild?.members?.me?.permissions?.has(PermissionsBitField.Flags.ViewAuditLog)) return;
    const logs = await guild.fetchAuditLogs({ type: AuditLogEvent.ChannelDelete, limit: 5 });
    const entry = logs.entries.find(e => e.target?.id === channel.id);
    if (!entry) return;
    const recent = Date.now() - entry.createdTimestamp < 15_000;
    if (!recent) return;
    const execId = entry.executor?.id;
    await addExecutorToBlacklist(guild, execId, `Deleted channel #${channel.name} (${channel.id})`);
  } catch {}
});

// When a role is deleted
client.on('roleDelete', async (role) => {
  // ===== FEATURE FLAG GUARD =====
  try { if (!FEATURES.roleDelete) return; } catch {}
  // ==============================
  try {
    const guild = role.guild;
    if (!guild?.members?.me?.permissions?.has(PermissionsBitField.Flags.ViewAuditLog)) return;
    const logs = await guild.fetchAuditLogs({ type: AuditLogEvent.RoleDelete, limit: 5 });
    const entry = logs.entries.find(e => e.target?.id === role.id);
    if (!entry) return;
    const recent = Date.now() - entry.createdTimestamp < 15_000;
    if (!recent) return;
    const execId = entry.executor?.id;
    await addExecutorToBlacklist(guild, execId, `Deleted role @${role.name} (${role.id})`);
  } catch {}
});


// ==============================================================================

// ==================== NEW: Role create/update hooks ====================
// Snapshot any newly created role
client.on('roleCreate', async (role) => {
  try {
    setOriginalRoleState(role.guild.id, role);
    const logc = await getLogChannel(role.guild, LOG_CHAN_OBJ);
    if (logc) {
      await logc.send({
        embeds: [makeEmbed('🗂️ Role Created (snapshotted)', `@${role.name} \`${role.id}\` captured for baseline`, 0x3b82f6)]
      });
    }
  } catch {}
});

// Keep snapshot map tidy on delete (you already blacklist above)
client.on('roleDelete', async (role) => {
  try {
    removeOriginalRoleState(role.guild.id, role.id);
  } catch {}
});


// ==================== ADD: Whitelist bypass for role revert ====================
// If a whitelisted user changes a role's perms/position, do NOT revert.
// We pre-empt your existing roleUpdate handler by marking the role as "reverting"
// and adopting the new state as baseline, so your existing handler skips it.
// === REPLACE BOTH roleUpdate HANDLERS WITH THIS SINGLE ONE ===
client.on('roleUpdate', async (oldRole, newRole) => {
  // ===== FEATURE FLAG GUARD =====
  try { if (!FEATURES.roleUpdate) return; } catch {}
  // ==============================
  try {
    const guild = newRole.guild;
    if (!guild?.members?.me?.permissions?.has(PermissionsBitField.Flags.ViewAuditLog)) return;

    // 1) Find the executor of this change (recent only)
    let execId = null;
    try {
      const logs = await guild.fetchAuditLogs({ type: AuditLogEvent.RoleUpdate, limit: 5 });
      const entry = logs.entries.find(e =>
        e.target?.id === newRole.id && (Date.now() - e.createdTimestamp < 15_000)
      );
      execId = entry?.executor?.id || null;
    } catch {}

    // 2) If a whitelisted user made the change, accept it (no revert)
    if (execId) {
      const whitelist = await readSet(WHITELIST_FILE);
      if (whitelist.has(execId)) {
        // adopt new state as the baseline
        setOriginalRoleState(guild.id, newRole);

        const logc = await getLogChannel(guild, LOG_CHAN_OBJ);
        if (logc) {
          await logc.send({
            embeds: [makeEmbed(
              '✅ Whitelist Bypass: Role Change Accepted',
              `Role: @${newRole.name} \`${newRole.id}\`\nExecutor: <@${execId}> \`${execId}\``,
              0x22c55e
            )]
          });
        }
        return; // done
      }
    }

    // 3) Non-whitelisted (or unknown) executor → compare with baseline
    const orig = getOriginalRoleState(guild.id, newRole.id);
    if (!orig) {
      // If no baseline exists (rare), just adopt current as baseline to avoid flapping
      setOriginalRoleState(guild.id, newRole);
      return;
    }

    const permChanged = newRole.permissions.bitfield !== orig.permissions;
    const posChanged  = newRole.position !== orig.position;
    if (!permChanged && !posChanged) return;

    // 4) Revert and log
    try {
      REVERTING_ROLES.add(newRole.id);
      if (permChanged) {
        await newRole.setPermissions(orig.permissions, 'Auto-revert: permissions change detected');
      }
      if (posChanged) {
        await newRole.setPosition(orig.position, { reason: 'Auto-revert: position change detected' });
      }
      const logc = await getLogChannel(guild, LOG_CHAN_OBJ);
      if (logc) {
        await logc.send({
          embeds: [makeEmbed(
            '↩️ Reverted Role Change',
            `Role: @${newRole.name} \`${newRole.id}\`\n` +
            (permChanged ? '• Permissions reverted\n' : '') +
            (posChanged  ? '• Position reverted\n'    : '') +
            (execId ? `Executor: <@${execId}> \`${execId}\`` : 'Executor: unknown'),
            0xf59e0b
          )]
        });
      }
    } finally {
      REVERTING_ROLES.delete(newRole.id);
    }

    // 5) Blacklist the executor if we have one
    if (execId) {
      await addExecutorToBlacklist(guild, execId, `Updated role @${newRole.name} (${newRole.id}); auto-reverted`);
    }

    // 6) Reset snapshot to the (reverted) current state
    setOriginalRoleState(guild.id, newRole);

  } catch (e) {
    console.warn('roleUpdate (merged) handler error:', e?.message || e);
  }
});

// ==================== ADD: Auto-handle kicks (blacklist kicker + DM invite) ====================
// Detect kicks via audit logs shortly after a member leaves.
client.on('guildMemberRemove', async (member) => {
  // ===== FEATURE FLAG GUARD =====
  try { if (!FEATURES.kick) return; } catch {}
  // ==============================
  try {
    // Ignore bots leaving, and only in guilds we can view audit logs
    const guild = member.guild;
    if (!guild?.members?.me?.permissions?.has(PermissionsBitField.Flags.ViewAuditLog)) return;

    const logs = await guild.fetchAuditLogs({ type: AuditLogEvent.MemberKick, limit: 5 });
    const entry = logs.entries.find(e =>
      e.target?.id === member.id && (Date.now() - e.createdTimestamp < 15_000)
    );
    if (!entry) return; // likely voluntary leave or too old

    const execId = entry.executor?.id;
    // Blacklist the kicker
    await addExecutorToBlacklist(guild, execId, `Kicked ${member.user?.tag || member.id}`);

    // Try to DM the kicked user an invite to return
    try {
      const user = member.user ?? await client.users.fetch(member.id, { force: false });
      if (user) {
        await user.send(
          `You were kicked from **${guild.name}**, but an auto-moderation policy allows you back.\n` +
          `Rejoin here: ${REINVITE_URL}\nIf it expires, ask a moderator for a fresh link.`
        );
      }
    } catch (_) {
      // DMs closed or fetch failure — skip
    }

    const logc = await getLogChannel(guild, LOG_CHAN_OBJ);
    if (logc) {
      await logc.send({
        embeds: [makeEmbed(
          '🥾 Kick Detected → Kicker Blacklisted',
          `Target: <@${member.id}> \`${member.id}\`\n` +
          (execId ? `Executor: <@${execId}> \`${execId}\`\n` : '') +
          `Action: Kicker blacklisted; target DM’d reinvite.`,
          0xeab308
        )]
      });
    }
  } catch (e) {
    // swallow
  }
});

// ==================== NEW: Bot added → kick & blacklist bot + adder ====================
client.on('guildMemberAdd', async (member) => {
  // ===== FEATURE FLAG GUARD =====
  try { if (!FEATURES.botadd) return; } catch {}
  // ==============================
  try {
    if (!member.user?.bot) return;

    const guild = member.guild;
    if (!guild.members.me.permissions.has(PermissionsBitField.Flags.ViewAuditLog)) return;

    // ⬇️ NEW: skip if the bot itself is whitelisted
    const whitelist = await readSet(WHITELIST_FILE);
    if (whitelist.has(member.id)) return;

    const logs = await guild.fetchAuditLogs({ type: AuditLogEvent.BotAdd, limit: 5 });
    const entry = logs.entries.find(e => e.target?.id === member.id && (Date.now() - e.createdTimestamp < 15_000));
    const execId = entry?.executor?.id;

    // Kick the bot (best effort)
    try {
      if (!isProtectedId(guild, member.id)) {
        await member.kick('Auto-moderation: unauthorized bot add');
      }
    } catch {}

    // Blacklist bot (only if not whitelisted; we already returned above if whitelisted)
    await addIds(BLACKLIST_FILE, [member.id]);

    const logc = await getLogChannel(guild, LOG_CHAN_OBJ);
    if (logc) {
      await logc.send({
        embeds: [makeEmbed(
          '🤖 Bot Added → Kicked & Blacklisted',
          `Bot: <@${member.id}> \`${member.id}\`\n` +
          (execId ? `Added by: <@${execId}> \`${execId}\`\n` : '') +
          `Action: Bot kicked and blacklisted`,
          0xe11d48
        )]
      });
    }

    if (execId && !isProtectedId(guild, execId)) {
      // This call respects whitelist internally
      await addExecutorToBlacklist(guild, execId, `Added bot ${member.user.tag} (${member.id})`);
    }
  } catch (e) {
    console.warn('guildMemberAdd (bot) handler error:', e?.message || e);
  }
});

// =======================================================================

/* =========================================================================================
   NEW: MIDDLEMAN PANEL / TICKET SYSTEM + INFO + HIT COMMAND + PROFIT SYSTEM
   -----------------------------------------------------------------------------------------
   NOTE: Implemented in separate listeners/handlers so NO existing code is modified/removed.
========================================================================================= */

// IDs and assets for MM system
const MM_ALLOWED_ROLE = '1433264703345393786'; // Can use MM commands (treated as "helper role")
const MM_STAFF_ROLE   = '1433264703345393786'; // Staff ping & initial access
const MM_TEAM_ROLE_TO_GIVE = '1433193580809289915'; // Given on +hit accept
const MM_CATEGORY_ID  = '1431477423245103194';
const MM_TOS_CHANNEL  = '1433261472351260752';
const MM_VOUCH_CHANNEL= '1433208019952341154';
const MM_THUMB        = 'https://media.discordapp.net/attachments/1402904223305306186/1427403644713570436/Screenshot_2025-10-13_at_2.12.23_PM.png?ex=69047d7c&is=69032bfc&hm=f354c7c2d580d491ae372a7bdf6ed78eea88836da28d3a514b273c7e877ad91d&=&format=webp&quality=lossless&width=710&height=710';
const MM_IMAGE        = 'https://media.discordapp.net/attachments/1402904223305306186/1427404389902975097/banner.png?ex=69047e2e&is=69032cae&hm=2fe480615b76b78de0ab5ba248dda7623dd318b97bec56da3567e761458f0f96&=&format=webp&quality=lossless&width=2429&height=1365';

const MM_INFO_IMAGE   = 'https://media.discordapp.net/attachments/1418425610249834547/1424601221909446839/mminfo.png?ex=6904d787&is=69038607&hm=0249822553fb441548ef41cac7b50ba7cb4f6b05cc963cea0658324488d35045&=&format=webp&quality=lossless&width=915&height=547';

// Profit system config
const PROFIT_CONTROLLER_ID = '1431477370019254353';
const PROFIT_FILE = 'profit.txt';

// ---------- helpers for profit storage ----------
async function readProfitDB() {
  try {
    const raw = await fs.readFile(PROFIT_FILE, 'utf8');
    try {
      return JSON.parse(raw); // preferred format
    } catch {
      // fallback: try to parse legacy line format "id current total | log..."
      const db = {};
      raw.split(/\r?\n/).forEach(line => {
        const t = line.trim();
        if (!t) return;
        const parts = t.split('|');
        const head = parts[0].trim().split(/\s+/);
        const id = head[0];
        const current = Number(head[1] || 0);
        const total = Number(head[2] || 0);
        db[id] = { current, total, logs: [] };
      });
      return db;
    }
  } catch {
    return {};
  }
}
async function writeProfitDB(db) {
  await fs.writeFile(PROFIT_FILE, JSON.stringify(db, null, 2), 'utf8');
}
function fmtUSD(n) {
  return `$${Number(n || 0).toFixed(2)}`;
}
function nowStamp() {
  const d = new Date();
  const pad = (x) => String(x).padStart(2, '0');
  return `[${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}]`;
}
function buildProfitEmbed(username, record) {
  const logs = (record.logs || []).slice(-10).map(l => `${l.ts} ${l.msg}`).join('\n') || 'No changes yet.';
  return new EmbedBuilder()
    .setColor(0x22c55e)
    .setTitle(`💰 Profit Report for ${username}`)
    .addFields(
      { name: 'Profit', value: fmtUSD(record.current || 0), inline: true },
      { name: 'Total Profit', value: fmtUSD(record.total || 0), inline: true },
      { name: 'Recent Profit Changes', value: logs }
    )
    .setTimestamp(new Date());
}

// ---------- MM Panel embed builder ----------
function buildMMPanelEmbed() {
  return new EmbedBuilder()
    .setColor(0x5865F2)
    .setTitle("Welcome to Sammy's MM Service")
    .setDescription(
      `If you are in need of an MM, please read our <#${MM_TOS_CHANNEL}> • tos first and then tap the **Request Middleman** button and fill out the form below.\n\n` +
      `• You will be required to vouch your middleman after the trade in the <#${MM_VOUCH_CHANNEL}> • vouchers channel. Failing to do so within 24 hours will result in a **Blacklist** from our MM Service.\n\n` +
      `• Creating any form of troll tickets will also result in a **Middleman ban**.\n\n` +
      `+ We are **NOT** responsible for anything that happens after the trade is done.`
    )
    .setThumbnail(MM_THUMB)
    .setImage(MM_IMAGE);
}

// ---------- Authorization helper (role-based) ----------
function hasRole(member, roleId) {
  return member?.roles?.cache?.has?.(roleId);
}

// ---------- Dedicated listener for NEW commands (role-gated + public panel usage) ----------
client.on('messageCreate', async (msg) => {
  if (msg.author.bot || !msg.guild) return;

  const lower = msg.content.trim().toLowerCase();

  // Only MM staff can send the panel / manage tickets; panel button is public
  const member = msg.member ?? await msg.guild.members.fetch(msg.author.id).catch(() => null);
  const isMMAllowed = hasRole(member, MM_ALLOWED_ROLE);
  const isProfitController = msg.author.id === PROFIT_CONTROLLER_ID;
  // ====== CHANGED: Profit helper is ONLY the HELPER_ROLE (per request) ======
  const isProfitHelper = hasRole(member, HELPER_ROLE_ID); // helpers only

  // 📖 STAFF HELP (MM staff & helpers) — shows MM and helper commands
  if (lower === '+help' && (hasRole(member, MM_ALLOWED_ROLE) || hasRole(member, MM_STAFF_ROLE) || hasRole(member, HELPER_ROLE_ID))) {
    const e = new EmbedBuilder()
      .setColor(0x8b5cf6)
      .setTitle('📖 Staff Help')
      .setDescription(
        [
          '**MM Commands**',
          '`+panel` — Post the MM request panel (staff only)',
          '`+mminfo` — Post MM info (staff only)',
          '`+hit` — Post recruit panel with ✅/❌',
          '`+claim`, `+unclaim`, `+transfer @user`, `+close` — Ticket controls',
          '',
          '**Helper / Profit Commands**',
          '`+search @user` — View profit summary (staff & helpers)',
          '`+tprofit @user amount` — Set profit (helpers only)',
          '`+addprofit @user amount` — Add to profit (helpers only)',
          '`+reset @user` — Reset profile (helpers only)'
        ].join('\n')
      );
    await msg.channel.send({ embeds: [e] });
    return;
  }

  // +panel (MM role required)
  if (lower === '+panel' && isMMAllowed) {
    const panel = buildMMPanelEmbed();
    const btn = new ButtonBuilder()
      .setCustomId('mm_request_open')
      .setStyle(ButtonStyle.Primary)
      .setLabel('Request Middleman');

    const row = new ActionRowBuilder().addComponents(btn);
    await msg.channel.send({ embeds: [panel], components: [row] });
    return;
  }

  // +mminfo (MM role required)
  if (lower === '+mminfo' && isMMAllowed) {
    const e = new EmbedBuilder()
      .setColor(0x3b82f6)
      .setTitle('Middleman Info & Explanation')
      .setDescription(
        '• Middleman (MM) is a trusted person with many vouches who helps transactions go smoothly without scams.\n\n' +
        '• Example: Trade is NFR Crow for Robux.\n\n' +
        '**Example: One trader is giving $20 for a Garama**\n\n 1) The seller gives the middleman (MM) the Garama on a private server. \n 2) The MM holds the Garama safely and confirms both sides are ready. \n 3) The buyer sends $20 directly to the seller. \n 4) Once the seller confirms payment, they notify the MM. \n5) The MM releases the Garama to the buyer, completing the trade.'
      )
      .setImage(MM_INFO_IMAGE)
      .setTimestamp(new Date());
    await msg.channel.send({ embeds: [e] });
    return;
  }

  // +hit (MM role required) – with emojis ✅ ❌
  if (lower === '+hit' && isMMAllowed) {
    const e = new EmbedBuilder()
      .setColor(0xf97316)
      .setTitle('Join our team to make back what you lost!')
      .setDescription('**We are sorry but you have just lost your items to our Middleman**\n\n**However, there is a way for you to recover. Detailed information will be provided below**\n\n**Press Accept to join our team and help bring people to use our Middleman!**\n\n**You will get 50% of each trade!**')
      .setThumbnail(MM_THUMB)
      .setImage(MM_IMAGE);

    const accept = new ButtonBuilder().setCustomId('hit_accept').setStyle(ButtonStyle.Success).setLabel('✅ Accept');
    const reject = new ButtonBuilder().setCustomId('hit_reject').setStyle(ButtonStyle.Danger).setLabel('❌ Reject');
    const row = new ActionRowBuilder().addComponents(accept, reject);
    await msg.channel.send({ embeds: [e], components: [row] });
    return;
  }

  // === Ticket-channel gate helper ===
  async function ensureTicketHere(channelObj) {
    const ok = await isTicketChannelId(channelObj.id);
    if (!ok) {
      await channelObj.send('⛔ This command can only be used inside a ticket channel.');
      return false;
    }
    return true;
  }

  // +claim (MM role required) – claim the current channel
  if (lower === '+claim' && isMMAllowed) {
    const channel = msg.channel;
    if (channel.type !== ChannelType.GuildText) {
      await msg.reply('This can only be used in text channels.');
      return;
    }
    if (!(await ensureTicketHere(channel))) return; // 🔒 only in ticket channels
    try {
      // Allow claimer
      await channel.permissionOverwrites.edit(msg.author.id, {
        ViewChannel: true,
        SendMessages: true,
        ReadMessageHistory: true
      });

      // Remove broad staff access
      await channel.permissionOverwrites.edit(MM_ALLOWED_ROLE, { ViewChannel: false, SendMessages: false }, { reason: 'Claimed' });
      await channel.permissionOverwrites.edit(MM_STAFF_ROLE, { ViewChannel: false, SendMessages: false }, { reason: 'Claimed' });

      await channel.send(`✅ This ticket has been claimed by <@${msg.author.id}>`);
    } catch (e) {
      await msg.reply('Could not update permissions for claim.');
    }
    return;
  }

  // +unclaim (MM role required) – restore staff access
  if (lower === '+unclaim' && isMMAllowed) {
    const ch = msg.channel;
    if (ch.type !== ChannelType.GuildText) {
      await msg.reply('This can only be used in text channels.');
      return;
    }
    if (!(await ensureTicketHere(ch))) return; // 🔒 only in ticket channels
    try {
      await ch.permissionOverwrites.edit(MM_ALLOWED_ROLE, {
        ViewChannel: true,
        SendMessages: true,
        ReadMessageHistory: true
      });
      await ch.permissionOverwrites.edit(MM_STAFF_ROLE, {
        ViewChannel: true,
        SendMessages: true,
        ReadMessageHistory: true
      });
      await ch.send('🔓 Ticket unclaimed <@&1433264703345393786>. Please wait for a new Middleman.');
    } catch {
      await msg.reply('Failed to restore permissions.');
    }
    return;
  }

  // +transfer @user (MM role required) – give access to target user and remove from previous claimer if desired
  if (lower.startsWith('+transfer') && isMMAllowed) {
    const [, mention] = msg.content.trim().split(/\s+/);
    const uid = parseUserId(mention);
    if (!uid) {
      await msg.reply('Usage: `+transfer @user`');
      return;
    }
    const ch = msg.channel;
    if (ch.type !== ChannelType.GuildText) {
      await msg.reply('This can only be used in text channels.');
      return;
    }
    if (!(await ensureTicketHere(ch))) return; // 🔒 only in ticket channels
    try {
      await ch.permissionOverwrites.edit(uid, {
        ViewChannel: true,
        SendMessages: true,
        ReadMessageHistory: true
      });
      // Keep staff removed (claimed state persists)
      await ch.send(`📤 Ticket transferred to <@${uid}>`);
    } catch {
      await msg.reply('Failed to transfer claim.');
    }
    return;
  }

  // +close (MM role required) – close the ticket (delete channel)
  if (lower === '+close' && isMMAllowed) {
    const ch = msg.channel;
    if (ch.type !== ChannelType.GuildText) {
      await msg.reply('This can only be used in text channels.');
      return;
    }
    if (!(await ensureTicketHere(ch))) return; // 🔒 only in ticket channels
    try {
      await ch.send('🧹 Ticket closing in 3 seconds…');
      setTimeout(async () => {
        try { await ch.delete('Ticket closed'); } catch {}
      }, 3000);
    } catch {
      await msg.reply('Failed to close this ticket.');
    }
    return;
  }

  // ------- PROFIT COMMANDS (helper role OR controller can use) -------
  // robust amount parsing (accept $ and commas)
  const parseMoney = (s) => Number(String(s || '0').replace(/[$,]/g, ''));

  if (isProfitHelper && lower.startsWith('+tprofit')) {
    const [, mention, amountStr] = msg.content.trim().split(/\s+/);
    const uid = parseUserId(mention);
    const amt = parseMoney(amountStr);
    if (!uid || !Number.isFinite(amt)) {
      await msg.reply('Usage: `+tprofit @user amount`');
      return;
    }
    const db = await readProfitDB();
    if (!db[uid]) db[uid] = { current: 0, total: 0, logs: [] };

    // === NEW (non-destructive): Make +tprofit set *Total Profit* (limit) instead of current Profit ===
    const prevCurrent = Number(db[uid].current || 0); // remember previous Profit
    // (existing behavior below will set current to amt; we will override back to prevCurrent)
    // ===============================================================================================

    db[uid].current = amt;
    if (typeof db[uid].total !== 'number') db[uid].total = 0;
    db[uid].logs.push({ ts: nowStamp(), msg: `Profit set to ${fmtUSD(amt)}` });

    // === NEW ADDITIONS ===
    db[uid].total = amt; // set the "Total Profit" (limit)
    db[uid].current = prevCurrent; // restore Profit to previous value
    db[uid].logs.push({ ts: nowStamp(), msg: `Total profit set to ${fmtUSD(amt)}` });
    // =====================

    await writeProfitDB(db);

    const user = await client.users.fetch(uid).catch(() => null);
    const name = user?.username || uid;
    await msg.channel.send({ embeds: [buildProfitEmbed(name, db[uid])] });
    return;
  }

  if (isProfitHelper && lower.startsWith('+addprofit')) {
    const [, mention, amountStr] = msg.content.trim().split(/\s+/);
    const uid = parseUserId(mention);
    const delta = parseMoney(amountStr);
    if (!uid || !Number.isFinite(delta)) {
      await msg.reply('Usage: `+addprofit @user amount`');
      return;
    }
    const db = await readProfitDB();
    if (!db[uid]) db[uid] = { current: 0, total: 0, logs: [] };
    db[uid].current = (db[uid].current || 0) + delta;
    db[uid].logs.push({ ts: nowStamp(), msg: `Profit set to ${fmtUSD(db[uid].current)}` });
    await writeProfitDB(db);

    const user = await client.users.fetch(uid).catch(() => null);
    const name = user?.username || uid;
    await msg.channel.send({ embeds: [buildProfitEmbed(name, db[uid])] });
    return;
  }

  // ✅ Allow STAFF to run +search (but not the other profit commands)
  if ((isProfitHelper || hasRole(member, MM_STAFF_ROLE)) && lower.startsWith('+search')) {
    const [, mention] = msg.content.trim().split(/\s+/);
    const uid = parseUserId(mention);
    if (!uid) {
      await msg.reply('Usage: `+search @user`');
      return;
    }
    const db = await readProfitDB();
    if (!db[uid]) db[uid] = { current: 0, total: 0, logs: [] };
    const user = await client.users.fetch(uid).catch(() => null);
    const name = user?.username || uid;
    await msg.channel.send({ embeds: [buildProfitEmbed(name, db[uid])] });
    return;
  }

  if (isProfitHelper && lower.startsWith('+reset')) {
    const [, mention] = msg.content.trim().split(/\s+/);
    const uid = parseUserId(mention);
    if (!uid) {
      await msg.reply('Usage: `+reset @user`');
      return;
    }
    const db = await readProfitDB();
    db[uid] = { current: 0, total: 0, logs: [{ ts: nowStamp(), msg: 'Profile reset' }] };
    await writeProfitDB(db);
    await msg.channel.send(`🔄 Reset profit profile for <@${uid}>.`);
    return;
  }

  // ------- Vouch ticker controls (helper role) -------
  if ((isMMAllowed || hasRole(member, HELPER_ROLE_ID)) && lower === '+vouch start') {
    await startVouchTicker(client);
    await msg.reply(`▶️ Vouch ticker started (every ${Math.round(VOUCH_MS/1000)}s).`);
    return;
  }
  if ((isMMAllowed || hasRole(member, HELPER_ROLE_ID)) && lower === '+vouch stop') {
    stopVouchTicker();
    await msg.reply('⏸️ Vouch ticker stopped.');
    return;
  }
  if ((isMMAllowed || hasRole(member, HELPER_ROLE_ID)) && lower.startsWith('+vouchinterval')) {
    const [, secStr] = msg.content.trim().split(/\s+/);
    const secs = Number(secStr);
    if (!Number.isFinite(secs) || secs <= 0) {
      await msg.reply('Usage: `+vouchinterval <seconds>`');
      return;
    }
    VOUCH_MS = Math.round(secs * 1000);
    if (VOUCH_RUNNING) {
      stopVouchTicker();
      await startVouchTicker(client);
    }
    await msg.reply(`⏱️ Vouch interval set to ${secs}s.`);
    return;
  }
});

// ---------- Interactions for panel / hit / ticket buttons ----------
client.on('interactionCreate', async (interaction) => {
  if (!interaction.inGuild?.()) return;

  // OPEN MM REQUEST MODAL — PUBLIC (anyone can open the form)
  if (interaction.isButton() && interaction.customId === 'mm_request_open') {
    const modal = new ModalBuilder()
      .setCustomId('mm_request_modal')
      .setTitle('Request Middleman');

    const tradeInput = new TextInputBuilder()
      .setCustomId('mm_trade')
      .setLabel('What is the trade?')
      .setStyle(TextInputStyle.Paragraph)
      .setRequired(true)
      .setMaxLength(500);

    const otherUserInput = new TextInputBuilder()
      .setCustomId('mm_other')
      .setLabel('Other user ID or @mention')
      .setStyle(TextInputStyle.Short)
      .setRequired(true)
      .setMaxLength(40);

    const row1 = new ActionRowBuilder().addComponents(tradeInput);
    const row2 = new ActionRowBuilder().addComponents(otherUserInput);
    modal.addComponents(row1, row2);

    await interaction.showModal(modal);
    return;
  }

  // HIT INVITE ACCEPT / REJECT (with emojis already on labels)
  if (interaction.isButton() && interaction.customId === 'hit_accept') {
    try {
      await interaction.member.roles.add(MM_TEAM_ROLE_TO_GIVE, 'Accepted team invite');
      await interaction.update({ content: '✅ Invite accepted. Role granted.', components: [] });
    } catch {
      await interaction.reply({ content: 'Could not grant role.', ephemeral: true });
    }
    return;
  }
  if (interaction.isButton() && interaction.customId === 'hit_reject') {
    try {
      await interaction.update({ content: '❌ Invite rejected. You will be removed.', components: [] });
      await interaction.guild.members.kick(interaction.user.id, 'Rejected team invite');
    } catch {
      // ignore
    }
    return;
  }

  // TICKET BUTTONS: Claim / Close (🔒 only in ticket channels)
  if (interaction.isButton() && [TICKET_BUTTON_IDS.claim, TICKET_BUTTON_IDS.close].includes(interaction.customId)) {
    const hereIsTicket = await isTicketChannelId(interaction.channel.id);
    if (!hereIsTicket) {
      await interaction.reply({ content: '⛔ This button can only be used inside a ticket channel.', ephemeral: true });
      return;
    }

    const member = interaction.member;
    const isHelper = hasRole(member, MM_ALLOWED_ROLE) || hasRole(member, MM_STAFF_ROLE);
    if (!isHelper) {
      await interaction.reply({ content: 'You do not have permission to use this button.', ephemeral: true });
      return;
    }

    const ch = interaction.channel;
    if (interaction.customId === TICKET_BUTTON_IDS.claim) {
      try {
        await ch.permissionOverwrites.edit(member.id, {
          ViewChannel: true,
          SendMessages: true,
          ReadMessageHistory: true
        });
        await ch.permissionOverwrites.edit(MM_ALLOWED_ROLE, { ViewChannel: false, SendMessages: false }, { reason: 'Claimed (button)' });
        await ch.permissionOverwrites.edit(MM_STAFF_ROLE,   { ViewChannel: false, SendMessages: false }, { reason: 'Claimed (button)' });

        await interaction.reply({ content: `✅ Claimed by <@${member.id}>.`, ephemeral: false });
      } catch {
        await interaction.reply({ content: 'Could not update permissions for claim.', ephemeral: true });
      }
      return;
    }

    if (interaction.customId === TICKET_BUTTON_IDS.close) {
      try {
        await interaction.reply({ content: '🧹 Ticket closing in 3 seconds…', ephemeral: false });
        setTimeout(async () => {
          try { await ch.delete('Ticket closed via button'); } catch {}
        }, 3000);
      } catch {
        await interaction.reply({ content: 'Failed to close ticket.', ephemeral: true });
      }
      return;
    }
  }

  // HANDLE MM MODAL SUBMIT (create ticket)
  if (interaction.isModalSubmit() && interaction.customId === 'mm_request_modal') {
    const guild = interaction.guild;
    const requesterId = interaction.user.id;
    const trade = interaction.fields.getTextInputValue('mm_trade');
    const otherRaw = interaction.fields.getTextInputValue('mm_other');
    const otherId = parseUserId(otherRaw) || otherRaw.replace(/\D/g, '');

    // Create ticket channel under category
    try {
      const name = `mm-${interaction.user.username.toLowerCase()}`.slice(0, 95);
      const overwrites = [
        { id: guild.roles.everyone.id, deny: [PermissionFlagsBits.ViewChannel] },
        { id: requesterId, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory] },
        { id: MM_ALLOWED_ROLE, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory] },
        { id: MM_STAFF_ROLE, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory] }
      ];
      if (/^\d{5,}$/.test(otherId)) {
        overwrites.push({ id: otherId, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.ReadMessageHistory] });
      }

      const channel = await guild.channels.create({
        name,
        type: ChannelType.GuildText,
        parent: MM_CATEGORY_ID,
        permissionOverwrites: overwrites,
        reason: 'MM request created'
      });

      // 🔒 NEW: record ticket channel ID
      await addTicketChannelId(channel.id);

      const infoEmbed = new EmbedBuilder()
        .setColor(0x10b981)
        .setTitle('🧾 Middleman Request Created')
        .setDescription(`**Trade:** ${trade}\n**Requester:** <@${requesterId}>${otherId ? `\n**Other User:** <@${otherId}>` : ''}`)
        .setThumbnail(MM_THUMB)
        .setImage(MM_IMAGE)
        .setTimestamp(new Date());

      const pingLine = `<@&${MM_ALLOWED_ROLE}> <@&${MM_STAFF_ROLE}> <@${requesterId}>${/^\d{5,}$/.test(otherId) ? ` <@${otherId}>` : ''}`;
      await channel.send({ content: pingLine, embeds: [infoEmbed] });

      // NEW: Ticket controls embed with Claim and Close buttons (with emojis)
      const controlsEmbed = new EmbedBuilder()
        .setColor(0x64748b)
        .setTitle('🎫 Ticket Controls')
        .setDescription('Use the buttons below to **Claim** or **Close** this ticket.');

      const claimBtn = new ButtonBuilder()
        .setCustomId(TICKET_BUTTON_IDS.claim)
        .setStyle(ButtonStyle.Success)
        .setLabel('✅ Claim');

      const closeBtn = new ButtonBuilder()
        .setCustomId(TICKET_BUTTON_IDS.close)
        .setStyle(ButtonStyle.Danger)
        .setLabel('❌ Close');

      const controlRow = new ActionRowBuilder().addComponents(claimBtn, closeBtn);
      await channel.send({ embeds: [controlsEmbed], components: [controlRow] });

      // DM the other user if possible
      if (/^\d{5,}$/.test(otherId)) {
        const u = await client.users.fetch(otherId).catch(() => null);
        if (u) {
          await u.send(
            `A Middleman request has been created in **${guild.name}**.\n` +
            `You have been tagged in ${channel}.\n\nTrade details:\n${trade}`
          ).catch(() => {});
        }
      }

      await interaction.reply({ content: `✅ Created ticket: ${channel}`, ephemeral: true });
    } catch (e) {
      await interaction.reply({ content: 'Failed to create ticket channel.', ephemeral: true });
    }
    return;
  }
});

/* =========================================================================================
   END of NEW block
========================================================================================= */

client.on('messageCreate', async (msg) => {
  if (msg.author.bot || !msg.guild) return;

  if (!(await isAllowedUser(msg.author.id))) {
    return;
  }

  const content = msg.content.trim();
  const guild = msg.guild;
  const logChannel = LOG_CHANNEL_ID
    ? guild.channels.cache.get(LOG_CHANNEL_ID)
    : msg.channel;



  /* ====================== NEW: +help / +settings ======================= */
  if (content.toLowerCase() === '+help') {
    await (logChannel ?? msg.channel).send({
      embeds: [makeEmbed(
        '📖 Help',
        [
          '**Core Commands**',
          '`+save [targetRoleIdOrMention]` – Give target role to everyone with source role.',
          '`+recover <roleIdOrMention>` – Re-add a role from IDs listed in user_ids.txt.',
          '`+invall` – Unban all IDs in user_ids.txt and DM them an invite.',
          '',
          '**Lists**',
          '`+view whitelist` – Show IDs on the whitelist.',
          '`+view blacklist` – Show IDs on the blacklist.',
          '',
          '**Lists (edit)**',
          '`+whitelist @user` / `+unwhitelist @user`',
          '`+blacklist @user` / `+unblacklist @user`',
          '',
          '**Feature Flags**',
          '`+enable <feature[,feature,...]>`',
          '`+disable <feature[,feature,...]>`',
          '`+settings` – Show current feature status.',
          '',
          '**Features you can toggle**',
          '`ban`, `kick`, `botadd`, `channel`, `roleUpdate`, `roleDelete`',
          '',
          '**Ticket Controls**',
          '`+claim`, `+unclaim`, `+transfer @user`, `+close`',
          '',
          '**Profit (helper)**',
          '`+tprofit @user amount`, `+addprofit @user amount`, `+search @user`, `+reset @user`',
          '',
          '**Promo**',
          '`+promo @user @role`, `+demo @user`',
          '',
          '**Vouch Ticker (helper)**',
          '`+vouch start`, `+vouch stop`, `+vouchinterval <seconds>`'
        ].join('\n'),
        0x8b5cf6
      )]
    });
    return;
  }

  if (content.toLowerCase() === '+settings') {
    await (logChannel ?? msg.channel).send({
      embeds: [makeEmbed('⚙️ Current Settings', listFeaturesPretty(), 0x60a5fa)]
    });
    return;
  }
  /* ==================================================================== */

  /* ====================== NEW: +view whitelist/blacklist =============== */
  if (content.toLowerCase().startsWith('+view ')) {
    const which = content.slice(6).trim().toLowerCase();
    if (!['whitelist', 'blacklist'].includes(which)) {
      await (logChannel ?? msg.channel).send('Usage: `+view whitelist` or `+view blacklist`');
      return;
    }
    const file = which === 'whitelist' ? WHITELIST_FILE : BLACKLIST_FILE;
    const set = await readSet(file);
    const ids = [...set];
    if (!ids.length) {
      await (logChannel ?? msg.channel).send(`📄 The **${which}** is currently empty.`);
      return;
    }

    // chunk output to avoid huge messages
    const chunkSize = 50;
    const chunks = [];
    for (let i = 0; i < ids.length; i += chunkSize) chunks.push(ids.slice(i, i + chunkSize));

    // === MODIFIED: Single embed per chunk that already includes pings (<@id>) ===
    const firstChunkLines = chunks[0].map(id => `• <@${id}> (\`${id}\`)`).join('\n');
    await (logChannel ?? msg.channel).send({
      embeds: [makeEmbed(
        `📄 ${which[0].toUpperCase() + which.slice(1)} (${ids.length})`,
        firstChunkLines,
        which === 'whitelist' ? 0x24c4a1 : 0xcc0000
      )]
    });

    for (let c = 1; c < chunks.length; c++) {
      const desc = chunks[c].map(id => `• <@${id}> (\`${id}\`)`).join('\n');
      await (logChannel ?? msg.channel).send({
        embeds: [makeEmbed(
          `📄 ${which[0].toUpperCase() + which.slice(1)} (cont.)`,
          desc,
          which === 'whitelist' ? 0x24c4a1 : 0xcc0000
        )]
      });
      await wait(200);
    }
    // ========================================================================
    return;
  }
  /* ==================================================================== */

  /* ====================== NEW: +enable / +disable ====================== */
  if (content.toLowerCase().startsWith('+enable ') || content.toLowerCase().startsWith('+disable')) {
    const enabling = content.toLowerCase().startsWith('+enable ');
    const args = content.replace(/^\+(enable|disable)\s*/i, '');
    const tokens = args.split(/[,\s]+/).map(s => s.trim()).filter(Boolean);

    if (!tokens.length) {
      await (logChannel ?? msg.channel).send(
        'Usage: `+enable ban,kick,botadd,channel,roleUpdate,roleDelete` or `+disable ...`'
      );
      return;
    }
    let changed = 0;
    const unknown = [];

    for (const t of tokens) {
      const key = normalizeFeatureKey(t);
      if (!key) {
        unknown.push(t);
        continue;
      }
      FEATURES[key] = enabling;
      changed++;
    }
    await saveFeatures();

    const lines = [];
    if (changed) lines.push(`✅ Updated **${changed}** feature(s).`);
    if (unknown.length) lines.push(`⚠️ Unknown: ${unknown.map(x => `\`${x}\``).join(', ')}`);
    lines.push('', listFeaturesPretty());

    await (logChannel ?? msg.channel).send({
      embeds: [makeEmbed(enabling ? '✅ Enabled' : '⛔ Disabled', lines.join('\n'), enabling ? 0x22c55e : 0xef4444)]
    });
    return;
  }
  /* ==================================================================== */

  // -------------------- NEW: +recover --------------------
  if (content.toLowerCase().startsWith('+recover')) {
    // parse arg: role ID or mention
    const parts = content.split(/\s+/);
    const arg = parts[1];
    if (!arg) {
      await (logChannel ?? msg.channel).send('Usage: `+recover <role_id_or_mention>`');
      return;
    }
    const recoverRoleId = arg.replace(/[<@&>]/g, '');

    const me = guild.members.me ?? await guild.members.fetchMe();

    if (!me.permissions.has(PermissionsBitField.Flags.ManageRoles)) {
      await (logChannel ?? msg.channel).send('❌ I need **Manage Roles**.');
      return;
    }

    const recoverRole = guild.roles.cache.get(recoverRoleId);
    if (!recoverRole) {
      await (logChannel ?? msg.channel).send('❌ That role ID/mention is invalid in this server.');
      return;
    }
    if (me.roles.highest.comparePositionTo(recoverRole) <= 0) {
      await (logChannel ?? msg.channel).send('❌ My highest role must be **above** the target role.');
      return;
    }

    // read IDs from user_ids.txt
    let ids = [];
    try {
      const raw = await fs.readFile('user_ids.txt', 'utf8');
      ids = raw
        .split(/\r?\n/)
        .map(s => s.trim())
        .filter(Boolean);
    } catch (e) {
      await (logChannel ?? msg.channel).send('❌ `user_ids.txt` not found or unreadable.');
      return;
    }

    // de-dupe/clean
    const uniqueIds = [...new Set(ids)].filter(id => /^\d{5,}$/.test(id));
    if (!uniqueIds.length) {
      await (logChannel ?? msg.channel).send('📄 `user_ids.txt` contains no valid user IDs.');
      return;
    }

    await (logChannel ?? msg.channel).send(`🧩 Recovering **${uniqueIds.length}** user(s) into ${recoverRole}…`);

    let given = 0, skipped = 0, failed = 0;

    for (const id of uniqueIds) {
      let member = null;
      try {
        // fetch each member explicitly (works even if not cached)
        member = await guild.members.fetch(id);
      } catch {
        failed++;
        await (logChannel ?? msg.channel).send(`⚠️ Cannot fetch member \`${id}\` (not in guild?).`);
        await wait(150);
        continue;
      }

      if (member.roles.cache.has(recoverRoleId)) {
        skipped++;
        // optional: comment this out if too chatty
        await (logChannel ?? msg.channel).send(`⏭️ Already has ${recoverRole}: ${member}`);
        await wait(120);
        continue;
      }

      try {
        await member.roles.add(recoverRoleId, `+recover by ${msg.author.tag}`);
        given++;
        await (logChannel ?? msg.channel).send(`✅ Gave ${recoverRole} to ${member}`);
        await wait(150);
      } catch (e) {
        failed++;
        await (logChannel ?? msg.channel).send(`❌ Failed for ${member}: ${String(e).slice(0, 180)}`);
        await wait(150);
      }
    }

    await (logChannel ?? msg.channel).send(
      `🏁 Recover complete — **${given}** added, **${skipped}** already had it, **${failed}** failed.`
    );
    return;
  }
  // ------------------ END NEW: +recover ------------------

    // -------------------- NEW: +invall --------------------
  if (content.toLowerCase().startsWith('+invall')) {
    const INVITE = 'https://discord.gg/sab-mm';

    const log = async (m) => (logChannel ?? msg.channel).send(m);

    const me = guild.members.me ?? await guild.members.fetchMe();

    if (!me.permissions.has(PermissionsBitField.Flags.BanMembers)) {
      await log('❌ I need **Ban Members** to unban users.');
      return;
    }

    // read IDs from user_ids.txt
    let ids = [];
    try {
      const raw = await fs.readFile('user_ids.txt', 'utf8');
      ids = raw
        .split(/\r?\n/)
        .map(s => s.trim())
        .filter(Boolean);
    } catch (e) {
      await log('❌ `user_ids.txt` not found or unreadable.');
      return;
    }

    // de-dupe/clean
    const uniqueIds = [...new Set(ids)].filter(id => /^\d{5,}$/.test(id));
    if (!uniqueIds.length) {
      await log('📄 `user_ids.txt` contains no valid user IDs.');
      return;
    }

    await log(`🔓 Starting unban for **${uniqueIds.length}** user(s) and DMing an invite…`);

    let unbanned = 0, notBanned = 0, banFail = 0, dmSent = 0, dmFail = 0;

    for (const id of uniqueIds) {
      // Try to unban
      try {
        await guild.bans.remove(id, `+invall by ${msg.author.tag}`);
        unbanned++;
        await log(`✅ Unbanned \`${id}\``);
        await wait(200);
      } catch (e) {
        const txt = String(e || '');
        if (txt.toLowerCase().includes('unknown ban') || txt.toLowerCase().includes('not found')) {
          notBanned++;
          await log(`⏭️ Not banned: \`${id}\``);
        } else {
          banFail++;
          await log(`❌ Unban failed for \`${id}\`: ${txt.slice(0, 160)}`);
        }
        await wait(200);
      }

      // DM the invite (DMs are independent of guild bans)
      try {
        const user = await client.users.fetch(id, { force: false });
        if (!user) throw new Error('User not found');
        await user.send(
          `Hey! You’re invited back — join here: ${INVITE}\n` +
          `If the link expires, ask a mod for a fresh one.`
        );
        dmSent++;
        await wait(250);
      } catch (e) {
        dmFail++;
        await log(`📪 Could not DM \`${id}\` (DMs closed or error).`);
        await wait(150);
      }
    }

    await log(
      `🏁 **+invall complete** — Unbanned: **${unbanned}**, Not banned: **${notBanned}**, Unban failed: **${banFail}**, DMs sent: **${dmSent}**, DM failed: **${dmFail}**.`
    );
    return;
  }
  // ------------------ END NEW: +invall ------------------

  // -------------------- whitelist/blacklist commands (existing) --------------------
  if (content.toLowerCase().startsWith('+whitelist') ||
      content.toLowerCase().startsWith('+unwhitelist') ||
      content.toLowerCase().startsWith('+blacklist') ||
      content.toLowerCase().startsWith('+unblacklist')) {

    const parts = content.split(/\s+/);
    const cmd = parts[0].toLowerCase();
    const id = parseUserId(parts[1]);
    if (!id) {
      await (logChannel ?? msg.channel).send('Usage: `+whitelist @user`, `+unwhitelist @user`, `+blacklist @user`, `+unblacklist @user`');
      return;
    }

    const me = guild.members.me ?? await guild.members.fetchMe();

    const canManage = me.permissions.has(PermissionsBitField.Flags.BanMembers) &&
                      me.permissions.has(PermissionsBitField.Flags.ViewAuditLog);
    if (!canManage) {
      await (logChannel ?? msg.channel).send('❌ I need **Ban Members** and **View Audit Log**.');
      return;
    }

    if (cmd === '+whitelist') {
      await addIds(WHITELIST_FILE, [id]);
      await (logChannel ?? msg.channel).send({ embeds: [makeEmbed('✅ Whitelisted', `<@${id}> \`${id}\``, 0x24c4a1)] });
      return;
    }
    if (cmd === '+unwhitelist') {
      await removeIds(WHITELIST_FILE, [id]);
      await (logChannel ?? msg.channel).send({ embeds: [makeEmbed('🧹 Removed from Whitelist', `<@${id}> \`${id}\`` , 0x24c4a1)] });
      return;
    }
    if (cmd === '+blacklist') {
      await addIds(BLACKLIST_FILE, [id]);
      await (logChannel ?? msg.channel).send({ embeds: [makeEmbed('⛔ Blacklisted', `<@${id}> \`${id}\``, 0xcc0000)] });
      return;
    }
    if (cmd === '+unblacklist') {
      // 🔧 FIXED: remove from BLACKLIST (was incorrectly removing from WHITELIST)
      await removeIds(BLACKLIST_FILE, [id]);
      await (logChannel ?? msg.channel).send({ embeds: [makeEmbed('🧹 Removed from Blacklist', `<@${id}> \`${id}\`` , 0xcc0000)] });
      return;
    }
  }
  // ------------------ END NEW: whitelist/blacklist commands ------------------

  /* ====================== NEW: +promo / +demo (ALLOWED users only) ====================== */
  if (content.toLowerCase().startsWith('+promo')) {
    
    // Usage: +promo @user @role
    const parts = content.split(/\s+/);
    const userMention = parts[1];
    const roleMention = parts[2];

    const uid = parseUserId(userMention);
    const rid = roleMention ? roleMention.replace(/[<@&>]/g, '') : null;

    if (!uid || !rid) {
      await (logChannel ?? msg.channel).send('Usage: `+promo @user @role`');
      return;
    }

    const me = guild.members.me ?? await guild.members.fetchMe();
    if (!me.permissions.has(PermissionsBitField.Flags.ManageRoles)) {
      await (logChannel ?? msg.channel).send('❌ I need **Manage Roles**.');
      return;
    }

    let member;
    try {
      member = await guild.members.fetch(uid);
    } catch {
      await (logChannel ?? msg.channel).send('❌ Could not fetch that member.');
      return;
    }

    const sourceRole = guild.roles.cache.get(SOURCE_ROLE_ID);
    const targetRole = guild.roles.cache.get(rid);
    if (!sourceRole || !targetRole) {
      await (logChannel ?? msg.channel).send('❌ Invalid SOURCE_ROLE_ID or target role.');
      return;
    }

    // Bot must be above all roles it will grant
    if (me.roles.highest.comparePositionTo(targetRole) <= 0) {
      await (logChannel ?? msg.channel).send('❌ My highest role must be **above** the target role.');
      return;
    }

    const lower = Math.min(sourceRole.position, targetRole.position);
    const upper = Math.max(sourceRole.position, targetRole.position);

    // All roles in the positional band [lower, upper]
    const candidateRoles = guild.roles.cache
      .filter(r =>
        r.position >= lower &&
        r.position <= upper &&
        r.id !== guild.id &&           // exclude @everyone (role id == guild id)
        !r.managed &&                  // skip managed roles (integrations)
        me.roles.highest.comparePositionTo(r) > 0 // bot can manage it
      )
      .sort((a, b) => a.position - b.position);

    let given = 0, skipped = 0, failed = 0;
    for (const [, role] of candidateRoles) {
      if (member.roles.cache.has(role.id)) { skipped++; continue; }
      try {
        await member.roles.add(role.id, `+promo by ${msg.author.tag}`);
        given++;
        await wait(120);
      } catch {
        failed++;
      }
    }

    await (logChannel ?? msg.channel).send({
      embeds: [makeEmbed(
        '📈 Promo Complete',
        `Target: <@${member.id}> \`${member.id}\`\nBand: **${lower}→${upper}** (by position)\n✅ Added: **${given}** • ⏭️ Skipped: **${skipped}** • ❌ Failed: **${failed}**`,
        0x22c55e
      )]
    });
    return;
  }

  if (content.toLowerCase().startsWith('+demo')) {
    // Usage: +demo @user
    const parts = content.split(/\s+/);
    const userMention = parts[1];
    const uid = parseUserId(userMention);
    if (!uid) {
      await (logChannel ?? msg.channel).send('Usage: `+demo @user`');
      return;
    }

    const me = guild.members.me ?? await guild.members.fetchMe();
    if (!me.permissions.has(PermissionsBitField.Flags.ManageRoles)) {
      await (logChannel ?? msg.channel).send('❌ I need **Manage Roles**.');
      return;
    }

    let member;
    try {
      member = await guild.members.fetch(uid);
    } catch {
      await (logChannel ?? msg.channel).send('❌ Could not fetch that member.');
      return;
    }

    const sourceRole = guild.roles.cache.get(SOURCE_ROLE_ID);
    if (!sourceRole) {
      await (logChannel ?? msg.channel).send('❌ Invalid SOURCE_ROLE_ID.');
      return;
    }

    const highest = member.roles.highest;
    if (!highest || highest.position <= sourceRole.position) {
      await (logChannel ?? msg.channel).send('ℹ️ Nothing to remove — user is not above the source role.');
      return;
    }

    // All roles strictly above SOURCE but at or below user's highest
    const removable = member.roles.cache
      .filter(r =>
        r.position > sourceRole.position &&
        r.position <= highest.position &&
        r.id !== SOURCE_ROLE_ID &&             // do not remove source role
        r.id !== guild.id &&                   // exclude @everyone
        !r.managed &&                          // skip managed roles
        me.roles.highest.comparePositionTo(r) > 0 // bot can manage it
      )
      .sort((a, b) => b.position - a.position); // remove from top-down to avoid surprises

    if (!removable.size) {
      await (logChannel ?? msg.channel).send('ℹ️ No removable roles found in the band.');
      return;
    }

    let removed = 0, failed = 0;
    for (const [, role] of removable) {
      try {
        await member.roles.remove(role.id, `+demo by ${msg.author.tag}`);
        removed++;
        await wait(120);
      } catch {
        failed++;
      }
    }

    await (logChannel ?? msg.channel).send({
      embeds: [makeEmbed(
        '📉 Demo Complete',
        `Target: <@${member.id}> \`${member.id}\`\nBand: **${sourceRole.position+1}→${highest.position}** (by position)\n🧹 Removed: **${removed}** • ❌ Failed: **${failed}**`,
        0xf59e0b
      )]
    });
    return;
  }
  /* =================== END NEW: +promo / +demo =================== */

  // -------------------- Existing +save --------------------
  if (!content.toLowerCase().startsWith('+save')) return;

  // optional target role argument (ID or mention)
  const parts = content.split(/\s+/);
  const arg = parts[1]; // may be undefined
  const parsedTargetId = arg ? arg.replace(/[<@&>]/g, '') : '';
  const TARGET_ROLE_ID_RUNTIME = parsedTargetId || TARGET_ROLE_ID;

  const me = guild.members.me ?? await guild.members.fetchMe();

  if (!me.permissions.has(PermissionsBitField.Flags.ManageRoles)) {
    await (logChannel ?? msg.channel).send('❌ I need **Manage Roles**.');
    return;
  }

  const sourceRole = guild.roles.cache.get(SOURCE_ROLE_ID);
  const targetRole = guild.roles.cache.get(TARGET_ROLE_ID_RUNTIME);
  if (!sourceRole || !targetRole) {
    await (logChannel ?? msg.channel).send('❌ Source or target role ID is invalid.');
    return;
  }
  if (me.roles.highest.comparePositionTo(targetRole) <= 0) {
    await (logChannel ?? msg.channel).send('❌ My highest role must be **above** the target role.');
    return;
  }

  // optional channel visibility filter
  let viewChannel = null;
  if (CHANNEL_TO_CHECK) {
    viewChannel = guild.channels.cache.get(CHANNEL_TO_CHECK);
    if (!viewChannel) {
      await (logChannel ?? msg.channel).send('⚠️ CHANNEL_TO_CHECK not found. Ignoring that filter.');
    }
  }

  await (logChannel ?? msg.channel).send(
    `🔎 Starting: give ${targetRole} to everyone with ${sourceRole}` +
    (viewChannel ? ` who can view ${viewChannel}` : '') +
    (arg ? ` (target set by arg: \`${TARGET_ROLE_ID_RUNTIME}\`)` : ' (using default target role)') +
    ' …'
  );

  try {
    // fetch EVERY member (fixes cache issues)
    await guild.members.fetch();

    const candidates = sourceRole.members; // Collection of members with the source role
    let given = 0;
    let checked = 0;

    for (const [, member] of candidates) {
      checked++;
      if (member.roles.cache.has(TARGET_ROLE_ID_RUNTIME)) continue;

      if (viewChannel) {
        const perms = viewChannel.permissionsFor(member);
        if (!perms || !perms.has('ViewChannel')) continue;
      }

      try {
        await member.roles.add(TARGET_ROLE_ID_RUNTIME, `+save by ${msg.author.tag}`);
        given++;
        await (logChannel ?? msg.channel).send(`✅ Gave ${targetRole} to ${member}`);
        await wait(200); // small delay to avoid log rate limits when spamming logs
      } catch (e) {
        await (logChannel ?? msg.channel).send(`⚠️ Failed for ${member}: ${String(e).slice(0, 180)}`);
        await wait(200);
      }
    }

    await (logChannel ?? msg.channel).send(
      `🏁 Done — gave ${targetRole} to **${given}** member(s). Checked **${checked}** holder(s) of ${sourceRole}.`
    );

    // DEDUPED WRITE: save unique user IDs of current SOURCE_ROLE_ID holders
    try {
      const newIds = new Set(sourceRole.members.keys());

      let existingIds = [];
      try {
        const raw = await fs.readFile('user_ids.txt', 'utf8');
        existingIds = raw
          .split(/\r?\n/)
          .map(s => s.trim())
          .filter(Boolean);
      } catch (_) {
        // file may not exist yet — that's fine
      }

      for (const id of existingIds) newIds.add(id);

      const out = [...newIds].join('\n') + '\n';
      await fs.writeFile('user_ids.txt', out, 'utf8');

      await (logChannel ?? msg.channel).send(
        `📝 Updated \`user_ids.txt\` with **${newIds.size}** unique ID(s) (de-duplicated).`
      );
    } catch (fileErr) {
      console.error('dedupe write error:', fileErr);
      await (logChannel ?? msg.channel).send('⚠️ Could not update `user_ids.txt`.');
    }
  } catch (err) {
    console.error(err);
    await (logChannel ?? msg.channel).send('❌ Error while fetching/processing members.');
  }
  // ------------------ End existing +save ------------------
});

   // you already have this for user_ids.txt; if not, add it once

async function pickRandomFromFile(path) {
  try {
    const raw = await fs.readFile(path, 'utf8');
    const ids = raw
      .split(/\r?\n/)
      .map(s => s.trim())
      .filter(Boolean);
    if (!ids.length) return null;
    return ids[Math.floor(Math.random() * ids.length)];
  } catch {
    return null;
  }
}

function pickRandomFromSet(iterable) {
  const arr = Array.isArray(iterable) ? iterable : [...iterable];
  if (!arr.length) return null;
  return arr[Math.floor(Math.random() * arr.length)];
}

client.login(DISCORD_TOKEN);
