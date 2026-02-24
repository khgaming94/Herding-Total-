// index.js
import { Client, GatewayIntentBits, Partials, PermissionsBitField } from "discord.js";
import Database from "better-sqlite3";

const token = process.env.DISCORD_TOKEN;
const listenChannelId = process.env.LISTEN_CHANNEL_ID;
const reportWebhookUrl = process.env.REPORT_WEBHOOK_URL || null; // optional webhook for scheduled reports
const guildIdEnv = process.env.GUILD_ID || null; // used to fetch guild for nicknames

if (!token || !listenChannelId) {
  console.error("Missing DISCORD_TOKEN or LISTEN_CHANNEL_ID (set them in Replit Secrets)");
  process.exit(1);
}

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent
  ],
  partials: [Partials.Channel]
});

// ----------------- Database & migration -----------------
const db = new Database("/data/ranch.sqlite");

// create base table if not exists (we'll migrate columns if needed)
db.exec(`
CREATE TABLE IF NOT EXISTS gathers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  channel_id TEXT NOT NULL,
  message_id TEXT NOT NULL UNIQUE,
  discord_id TEXT,
  ranch_id INTEGER,
  item_type TEXT NOT NULL,
  amount INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gathers_ts ON gathers(ts);
CREATE INDEX IF NOT EXISTS idx_gathers_user ON gathers(discord_id);
CREATE INDEX IF NOT EXISTS idx_gathers_ranch ON gathers(ranch_id);

CREATE TABLE IF NOT EXISTS report_subscribers (
  discord_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT
);
`);

// migration: add value (REAL) and subtype (TEXT) columns if missing
try {
  const cols = db.prepare("PRAGMA table_info(gathers)").all();
  const colNames = cols.map(c => c.name);
  if (!colNames.includes("value")) {
    db.prepare("ALTER TABLE gathers ADD COLUMN value REAL DEFAULT 0").run();
    console.log("DB migration: added 'value' column to gathers");
  }
  if (!colNames.includes("subtype")) {
    db.prepare("ALTER TABLE gathers ADD COLUMN subtype TEXT DEFAULT NULL").run();
    console.log("DB migration: added 'subtype' column to gathers");
  }
} catch (e) {
  console.error("DB migration error (safe to ignore if you plan to recreate DB):", e);
}

// prepared statements (updated to include value & subtype)
const insertGather = db.prepare(`
INSERT INTO gathers (ts, channel_id, message_id, discord_id, ranch_id, item_type, amount, value, subtype)
VALUES (@ts, @channel_id, @message_id, @discord_id, @ranch_id, @item_type, @amount, @value, @subtype)
`);

const countRecentDuplicate = db.prepare(`
  SELECT COUNT(1) AS cnt FROM gathers
  WHERE channel_id = @channel_id
    AND item_type = @item_type
    AND amount = @amount
    AND (
      (@discord_id IS NULL AND discord_id IS NULL)
      OR (discord_id = @discord_id)
    )
    AND ts >= @since_ts
`);

// sums for eggs & milk remain (value is separate)
const sumTotals = db.prepare(`
SELECT
  COALESCE(SUM(CASE WHEN item_type='eggs' THEN amount END), 0) AS eggs,
  COALESCE(SUM(CASE WHEN item_type='milk' THEN amount END), 0) AS milk
FROM gathers
WHERE (@ranch_id IS NULL OR ranch_id = @ranch_id)
  AND (@since_ts IS NULL OR ts >= @since_ts)
`);

const sumTotalsSince = db.prepare(`
  SELECT
    COALESCE(SUM(CASE WHEN item_type='eggs' THEN amount END), 0) AS eggs,
    COALESCE(SUM(CASE WHEN item_type='milk' THEN amount END), 0) AS milk
  FROM gathers
  WHERE ts >= @since_ts
    AND (@ranch_id IS NULL OR ranch_id = @ranch_id)
    AND (@discord_id IS NULL OR discord_id = @discord_id)
`);

// top users - now select herd buy/sell counts and values
const topUsers = db.prepare(`
SELECT discord_id,
       COALESCE(SUM(CASE WHEN item_type='eggs' THEN amount END), 0) AS eggs,
       COALESCE(SUM(CASE WHEN item_type='milk' THEN amount END), 0) AS milk,
       COALESCE(SUM(CASE WHEN item_type='herd_buy' THEN amount END), 0) AS herd_bought,
       COALESCE(SUM(CASE WHEN item_type='herd_sell' THEN amount END), 0) AS herd_sold,
       COALESCE(SUM(CASE WHEN item_type='herd_buy' THEN value END), 0) AS herd_buy_cost,
       COALESCE(SUM(CASE WHEN item_type='herd_sell' THEN value END), 0) AS herd_sell_revenue
FROM gathers
WHERE (@since_ts IS NULL OR ts >= @since_ts)
  AND (@ranch_id IS NULL OR ranch_id = @ranch_id)
  AND discord_id IS NOT NULL
GROUP BY discord_id
ORDER BY (eggs + milk) DESC
LIMIT @limit
`);

// weekly per-user (same fields)
const weeklyPerUser = db.prepare(`
SELECT discord_id,
       COALESCE(SUM(CASE WHEN item_type='eggs' THEN amount END), 0) AS eggs,
       COALESCE(SUM(CASE WHEN item_type='milk' THEN amount END), 0) AS milk,
       COALESCE(SUM(CASE WHEN item_type='herd_buy' THEN amount END), 0) AS herd_bought,
       COALESCE(SUM(CASE WHEN item_type='herd_sell' THEN amount END), 0) AS herd_sold,
       COALESCE(SUM(CASE WHEN item_type='herd_buy' THEN value END), 0) AS herd_buy_cost,
       COALESCE(SUM(CASE WHEN item_type='herd_sell' THEN value END), 0) AS herd_sell_revenue
FROM gathers
WHERE ts >= @since_ts
  AND discord_id IS NOT NULL
GROUP BY discord_id
ORDER BY (eggs + milk) DESC
`);

const deleteSince = db.prepare(`
  DELETE FROM gathers
  WHERE ts >= @since_ts
  AND (@ranch_id IS NULL OR ranch_id = @ranch_id)
`);

const addSubscriber = db.prepare(`INSERT OR REPLACE INTO report_subscribers (discord_id) VALUES (@discord_id)`);
const removeSubscriber = db.prepare(`DELETE FROM report_subscribers WHERE discord_id = @discord_id`);
const listSubscribers = db.prepare(`SELECT discord_id FROM report_subscribers`);
const getMeta = db.prepare(`SELECT value FROM meta WHERE key = @key`);
const setMeta = db.prepare(`
  INSERT INTO meta (key, value) VALUES (@key, @value)
  ON CONFLICT(key) DO UPDATE SET value = @value
`);

// ----------------- Settings -----------------
const DUPLICATE_WINDOW_MS = 10 * 1000; // 10s duplicate suppression
const PRICE_PER_ITEM = 1.25; // dollars per milk/egg

// Default weekly schedule: Monday 09:00 America/Toronto
const DEFAULT_WEEKDAY = 1; // Monday
const DEFAULT_HOUR = 9;
const DEFAULT_MINUTE = 0;

// ----------------- Timezone helpers -----------------
function parseSinceToTs(since) {
  if (!since) return null;
  const m = since.trim().match(/^(\d+)\s*(h|d)$/i);
  if (!m) return null;
  const n = Number(m[1]);
  const unit = m[2].toLowerCase();
  const ms = unit === "h" ? n * 60 * 60 * 1000 : n * 24 * 60 * 60 * 1000;
  return Date.now() - ms;
}
function sevenDaysAgoTs() {
  return Date.now() - 7 * 24 * 60 * 60 * 1000;
}
function getTorontoDateParts() {
  const now = new Date();
  const opts = { timeZone: "America/Toronto", hour12: false, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" };
  const s = now.toLocaleString("en-CA", opts);
  const parts = s.split(",");
  const datePart = (parts[0] || "").trim();
  const timePart = (parts[1] || "").trim();
  const ymd = datePart.split("-");
  const hourMin = timePart.split(":");
  const year = Number(ymd[0]);
  const month = Number(ymd[1]);
  const day = Number(ymd[2]);
  const hour = Number(hourMin[0]);
  const minute = Number(hourMin[1]);
  const weekdayStr = now.toLocaleString("en-US", { timeZone: "America/Toronto", weekday: "long" });
  const weekdayMap = { "Sunday":0, "Monday":1, "Tuesday":2, "Wednesday":3, "Thursday":4, "Friday":5, "Saturday":6 };
  const weekday = weekdayMap[weekdayStr] !== undefined ? weekdayMap[weekdayStr] : new Date().getUTCDay();
  return { year, month, day, hour, minute, weekday };
}
function isReportTime(nowParts, scheduledWeekday, scheduledHour, scheduledMinute) {
  if (!nowParts) return false;
  return nowParts.weekday === scheduledWeekday && nowParts.hour === scheduledHour && nowParts.minute === scheduledMinute;
}
function getScheduleMeta() {
  const ms = getMeta.get({ key: "report_schedule" });
  if (!ms || !ms.value) return { weekday: DEFAULT_WEEKDAY, hour: DEFAULT_HOUR, minute: DEFAULT_MINUTE };
  try {
    const o = JSON.parse(ms.value);
    return { weekday: Number(o.weekday), hour: Number(o.hour), minute: Number(o.minute) };
  } catch (e) {
    return { weekday: DEFAULT_WEEKDAY, hour: DEFAULT_HOUR, minute: DEFAULT_MINUTE };
  }
}
function setScheduleMeta(weekday, hour, minute) {
  setMeta.run({ key: "report_schedule", value: JSON.stringify({ weekday, hour, minute }) });
}

// ----------------- Parsing helpers -----------------
function extractDiscordId(text) {
  if (!text) return null;
  const m = text.match(/<@!?(\d{17,20})>/);
  return m ? m[1] : null;
}
function extractActorName(text) {
  if (!text) return null;
  const br = text.match(/^\[([^\]]{1,64})\]/) || text.match(/\[([^\]]{1,64})\]/);
  if (br) return br[1].trim();
  const pipe = text.match(/^([^|\n]{1,64})\s*\|/);
  if (pipe) return pipe[1].trim();
  return null;
}
function extractRanchId(text) {
  if (!text) return null;
  const m = text.match(/ranch\s*id\s*[:#]?\s*(\d+)/i) || text.match(/\branch\s*(\d+)/i);
  return m ? Number(m[1]) : null;
}
function extractItemAndAmount(text) {
  if (!text) return null;
  const t = text.replace(/\s+/g, " ").trim();
  // milk / eggs pattern first
  const near = t.match(/(\d+)\s*(eggs?|milk)\b/i);
  if (near) {
    const amount = Number(near[1]);
    const itemRaw = near[2].toLowerCase();
    const item_type = itemRaw.indexOf("milk") === 0 ? "milk" : "eggs";
    return { item_type, amount, value: 0, subtype: null };
  }

  // herd buy/sell pattern: bought 5 Bison for $300 OR sold 4 Bison for 960.0$
  const herd = t.match(/\b(bought|sold)\b\s+(\d+)\s+([A-Za-z]+)\s+for\s+\$?([\d,\.]+)\$?/i);
  if (herd) {
    const action = herd[1].toLowerCase(); // bought|sold
    const cnt = Number(herd[2]);
    const animal = herd[3].toLowerCase();
    const priceRaw = herd[4].replace(/,/g, "");
    const price = Number(priceRaw);
    if (action === "bought") {
      return { item_type: "herd_buy", amount: cnt, value: price, subtype: animal };
    } else {
      return { item_type: "herd_sell", amount: cnt, value: price, subtype: animal };
    }
  }

  // fallback: see if text mentions eggs or milk but with numbers elsewhere
  const hasEggs = /\beggs?\b/i.test(t);
  const hasMilk = /\bmilk\b/i.test(t);
  if (!hasEggs && !hasMilk) return null;
  const ranchId = extractRanchId(t);
  const nums = (t.match(/\d+/g) || []).map(Number);
  let candidates = nums;
  if (ranchId) candidates = nums.filter(n => n !== ranchId);
  if (!candidates.length) return null;
  const amount = candidates[candidates.length - 1];
  const item_type = hasMilk ? "milk" : "eggs";
  return { item_type, amount, value: 0, subtype: null };
}

function parseGather(text) {
  if (!text) return null;
  const discord_id = extractDiscordId(text);
  const actor = extractActorName(text);
  const ranch_id = extractRanchId(text);
  const item = extractItemAndAmount(text);
  if (!item || !item.amount || item.amount <= 0) return null;
  if (item.amount > 100000) return null;
  return {
    discord_id,
    actor,
    ranch_id,
    item_type: item.item_type,
    amount: item.amount,
    value: item.value || 0,
    subtype: item.subtype || null
  };
}

// ----------------- Get server display name (nickname) -----------------
async function getDisplayNameForGuild(uid, guild) {
  if (!uid) return "Unknown";
  // prefer guild nickname if guild object provided
  if (guild && guild.members && typeof guild.members.fetch === "function") {
    try {
      const member = await guild.members.fetch(uid).catch(() => null);
      if (member && member.displayName) return member.displayName;
    } catch (e) { /* ignore */ }
  }
  try {
    const u = await client.users.fetch(uid).catch(() => null);
    if (u) return u.username + (u.discriminator ? ("#" + u.discriminator) : "");
  } catch (e) {}
  return uid;
}

// ----------------- Message listener -----------------
client.on("messageCreate", function(message) {
  try {
    console.log("MSG:", "channelId=", message.channelId, "isWebhook=", !!message.webhookId, "contentLen=", (message.content || "").length, "embeds=", (message.embeds && message.embeds.length) ? message.embeds.length : 0);

    if (message.channelId !== listenChannelId) return;

    let text = message.content || "";
    if ((!text || text.trim() === "") && message.embeds && message.embeds.length > 0) {
      const e = message.embeds[0];
      let fields = "";
      if (e.fields && e.fields.length) {
        for (let i = 0; i < e.fields.length; i++) {
          const f = e.fields[i];
          fields += " " + (f.name || "") + " " + (f.value || "");
        }
      }
      text = ((e.title || "") + " " + (e.description || "") + " " + fields).trim();
    }

    console.log("TEXT_USED:", text);

    const parsed = parseGather(text);
    if (!parsed) {
      console.log("PARSE_FAIL");
      return;
    }

    // If we don't have a mention / discord id, skip (as before)
    if (!parsed.discord_id) {
      if (parsed.actor && /^unknown$/i.test(parsed.actor.trim())) {
        console.log("SKIP_UNKNOWN_ACTOR: skipping message with actor 'unknown'. text:", text);
        return;
      }
      console.log("SKIP_NO_MENTION: skipping message with no Discord mention. text:", text);
      return;
    }

    // duplicate suppression
    const sinceTs = Date.now() - DUPLICATE_WINDOW_MS;
    const dupRow = countRecentDuplicate.get({
      channel_id: message.channelId,
      item_type: parsed.item_type,
      amount: parsed.amount,
      discord_id: parsed.discord_id || null,
      since_ts: sinceTs
    });
    if (dupRow && dupRow.cnt && dupRow.cnt > 0) {
      console.log("DUPLICATE_SKIPPED: same entry within duplicate window. text:", text);
      return;
    }

    // insert; use parsed.value and parsed.subtype for herd events
    insertGather.run({
      ts: Date.now(),
      channel_id: message.channelId,
      message_id: message.id,
      discord_id: parsed.discord_id,
      ranch_id: parsed.ranch_id,
      item_type: parsed.item_type,
      amount: parsed.amount,
      value: parsed.value || 0,
      subtype: parsed.subtype || null
    });

    console.log("LOGGED:", parsed.item_type, "+" + parsed.amount, "value=" + (parsed.value || 0), "user=" + parsed.discord_id, "ranch=" + (parsed.ranch_id || "n/a"));
  } catch (e) {
    if (String(e).includes("UNIQUE constraint failed")) return;
    console.error("ERROR in messageCreate:", e);
  }
});

// ----------------- Utility: send embeds in batches (10 per message) -----------------
async function sendEmbedsInBatches(target, embeds) {
  const BATCH_SIZE = 10;
  if (!embeds || embeds.length === 0) return;
  const hasReply = target && typeof target.reply === "function" && typeof target.followUp === "function";
  const hasSend = target && typeof target.send === "function";

  for (let i = 0; i < embeds.length; i += BATCH_SIZE) {
    const batch = embeds.slice(i, i + BATCH_SIZE);
    try {
      if (hasReply) {
        if (!target.replied && !target.deferred) {
          await target.reply({ embeds: batch });
        } else {
          await target.followUp({ embeds: batch });
        }
      } else if (hasSend) {
        await target.send({ embeds: batch });
      } else {
        console.error("sendEmbedsInBatches: no valid send target");
      }
    } catch (err) {
      console.error("Failed to send embed batch:", err);
    }
  }
}

// ----------------- Weekly report & reset (DMs use server nicknames) -----------------
async function performWeeklyReportAndReset() {
  try {
    const since_ts = sevenDaysAgoTs();
    const totals = sumTotalsSince.get({ since_ts, ranch_id: null, discord_id: null });
    const eggs = totals.eggs || 0;
    const milk = totals.milk || 0;

    const rows = weeklyPerUser.all({ since_ts });

    // Attempt to get the configured guild (for nicknames).
    let guild = null;
    if (guildIdEnv) {
      try {
        guild = client.guilds.cache.get(guildIdEnv) || await client.guilds.fetch(guildIdEnv).catch(() => null);
      } catch (e) {
        guild = null;
      }
    }

    // Build per-user rows with display names (nickname when available)
    const userRows = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const uid = r.discord_id;
      let display = await getDisplayNameForGuild(uid, guild).catch(() => null);
      if (!display) display = uid || "Unknown";
      const eggsCount = r.eggs || 0;
      const milkCount = r.milk || 0;
      const herdBought = r.herd_bought || 0;
      const herdSold = r.herd_sold || 0;
      const herdBuyCost = r.herd_buy_cost || 0;
      const herdSellRevenue = r.herd_sell_revenue || 0;
      const herdNet = (herdSellRevenue - herdBuyCost);
      const totalItems = eggsCount + milkCount;
      userRows.push({
        discord_id: uid,
        display,
        eggs: eggsCount,
        milk: milkCount,
        totalItems,
        herdBought,
        herdSold,
        herdBuyCost,
        herdSellRevenue,
        herdNet
      });
    }

    // Build summary + per-person embeds
    const overallItems = (eggs || 0) + (milk || 0);
    // compute herd totals for overall revenue
    const overallHerdBuyCost = db.prepare(`SELECT COALESCE(SUM(value),0) AS v FROM gathers WHERE ts >= @since_ts AND item_type='herd_buy'`).get({ since_ts }).v || 0;
    const overallHerdSellRevenue = db.prepare(`SELECT COALESCE(SUM(value),0) AS v FROM gathers WHERE ts >= @since_ts AND item_type='herd_sell'`).get({ since_ts }).v || 0;
    const overallHerdNet = overallHerdSellRevenue - overallHerdBuyCost;
    const overallRevenue = (overallItems * PRICE_PER_ITEM) + overallHerdNet;

    const embeds = [];

    embeds.push({
      title: "Weekly Summary — last 7 days",
      description: `🥚 Eggs: **${eggs}**  |  🥛 Milk: **${milk}**  |  Total Items: **${overallItems}**`,
      color: 0x2ecc71,
      fields: [
        { name: "Items Revenue", value: `$${(overallItems * PRICE_PER_ITEM).toFixed(2)}`, inline: true },
        { name: "Herd Net", value: `$${overallHerdNet.toFixed(2)}`, inline: true },
        { name: "Total Revenue", value: `$${overallRevenue.toFixed(2)}`, inline: false }
      ],
      timestamp: new Date().toISOString()
    });

    for (let j = 0; j < userRows.length; j++) {
      const ur = userRows[j];
      const personItemsRevenue = ur.totalItems * PRICE_PER_ITEM;
      const personTotalRevenue = personItemsRevenue + (ur.herdNet || 0);
      const embed = {
        title: `${j + 1}. ${ur.display}`,
        description: `Items collected: **${ur.totalItems}**`,
        color: 0x3498db,
        fields: [
          { name: "Eggs", value: String(ur.eggs || 0), inline: true },
          { name: "Milk", value: String(ur.milk || 0), inline: true },
          { name: "Herd Bought", value: String(ur.herdBought || 0), inline: true },
          { name: "Herd Sold", value: String(ur.herdSold || 0), inline: true },
          { name: "Herd Net", value: `$${(ur.herdNet || 0).toFixed(2)}`, inline: false },
          { name: "Total Revenue", value: `$${personTotalRevenue.toFixed(2)}`, inline: false }
        ],
        footer: { text: "Ranch report • last 7 days" },
        timestamp: new Date().toISOString()
      };
      embeds.push(embed);
    }

    // If REPORT_WEBHOOK_URL is set -> post embeds to webhook (batches)
    if (reportWebhookUrl) {
      try {
        for (let i = 0; i < embeds.length; i += 10) {
          const batch = embeds.slice(i, i + 10);
          await fetch(reportWebhookUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ username: "Ranch Report", embeds: batch })
          }).catch(e => console.error("Webhook post error:", e));
          await new Promise(r => setTimeout(r, 200));
        }
      } catch (e) {
        console.error("Failed to post webhook embeds:", e);
      }
    } else {
      // DM subscribers: send a plaintext DM using nicknames
      const subs = listSubscribers.all();
      if (!subs || subs.length === 0) {
        console.log("weekly report: no subscribers to DM and no REPORT_WEBHOOK_URL set. Skipping reset.");
        return;
      }

      // Build plaintext DM: summary + per-person lines
      let dmText = `Weekly Ranch Totals (last 7 days)\n`;
      dmText += `Eggs: ${eggs}\nMilk: ${milk}\nTotal items: ${overallItems}\nItems Revenue: $${(overallItems * PRICE_PER_ITEM).toFixed(2)}\nHerd Net: $${overallHerdNet.toFixed(2)}\nTotal Revenue: $${overallRevenue.toFixed(2)}\n\nPer-person:\n`;
      if (userRows.length === 0) dmText += "_No collectors found in the last 7 days._\n";
      else {
        for (let i = 0; i < userRows.length; i++) {
          const ur = userRows[i];
          const personItemsRevenue = ur.totalItems * PRICE_PER_ITEM;
          const personTotalRevenue = personItemsRevenue + (ur.herdNet || 0);
          dmText += `${i + 1}. ${ur.display} — Eggs: ${ur.eggs} | Milk: ${ur.milk} | Items Rev: $${personItemsRevenue.toFixed(2)} | Herd Bought: ${ur.herdBought} | Herd Sold: ${ur.herdSold} | Herd Net: $${(ur.herdNet||0).toFixed(2)} | Total: $${personTotalRevenue.toFixed(2)}\n`;
        }
      }

      for (let s = 0; s < subs.length; s++) {
        const sid = subs[s].discord_id;
        try {
          const u = await client.users.fetch(sid);
          if (u) {
            await u.send(dmText).catch(e => console.error("DM failed:", e));
            console.log("Sent weekly DM to", sid);
          }
        } catch (e) {
          console.error("Failed to DM subscriber", sid, e);
        }
      }
    }

    // After sending, delete last 7 days entries
    const info = deleteSince.run({ since_ts, ranch_id: null });
    const deleted = info.changes || 0;
    console.log("Weekly reset: deleted", deleted, "rows from the last 7 days.");
  } catch (err) {
    console.error("Error running weekly report & reset:", err);
  }
}

// ----------------- Scheduler -----------------
const lastReportDateKey = "last_report_date";
function getLastReportDate() {
  const r = getMeta.get({ key: lastReportDateKey });
  return r && r.value ? r.value : null;
}
function setLastReportDate(val) {
  setMeta.run({ key: lastReportDateKey, value: val });
}
function startScheduler() {
  console.log("Report schedule:", getScheduleMeta());
  setInterval(async () => {
    try {
      const nowParts = getTorontoDateParts();
      const scheduleNow = getScheduleMeta();
      if (isReportTime(nowParts, Number(scheduleNow.weekday), Number(scheduleNow.hour), Number(scheduleNow.minute))) {
        const dateStr = `${nowParts.year}-${String(nowParts.month).padStart(2,"0")}-${String(nowParts.day).padStart(2,"0")}`;
        const last = getLastReportDate();
        if (last === dateStr) return;
        console.log("Triggering weekly report for", dateStr);
        await performWeeklyReportAndReset();
        setLastReportDate(dateStr);
      }
    } catch (e) {
      console.error("Scheduler error:", e);
    }
  }, 60 * 1000);
}

// ----------------- Interaction handlers (slash commands) -----------------
client.on("interactionCreate", async (interaction) => {
  let isChatInput = false;
  try {
    if (typeof interaction.isChatInputCommand === "function") isChatInput = interaction.isChatInputCommand();
    else isChatInput = !!interaction.commandName;
  } catch (e) {
    isChatInput = !!interaction.commandName;
  }
  if (!isChatInput) return;

  try {
    const name = interaction.commandName;

    if (name === "totals") {
      let since = null;
      try { since = interaction.options.getString("since"); } catch (e) {}
      const sinceTs = parseSinceToTs(since);
      const totals = sumTotals.get({ ranch_id: null, since_ts: sinceTs });
      const titleParts = ["All ranches", sinceTs ? ("last " + since) : "all-time"];
      await interaction.reply(`**Totals (${titleParts.join(" · ")})**\n🥚 Eggs: **${totals.eggs}**\n🥛 Milk: **${totals.milk}**`);
      return;
    }

    if (name === "leaderboard") {
      let since = null;
      try { since = interaction.options.getString("since"); } catch (e) {}
      const limit = Math.max(1, Math.min(200, interaction.options.getInteger("limit") || 50));
      const sinceTs = parseSinceToTs(since);

      const rows = topUsers.all({ ranch_id: null, since_ts: sinceTs, limit });

      if (!rows || rows.length === 0) {
        await interaction.reply("No data yet.");
        return;
      }

      // Build per-person embeds (no mentions, use server nickname)
      const embeds = [];
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const uid = r.discord_id;
        const display = await getDisplayNameForGuild(uid, interaction.guild).catch(() => (uid || "Unknown"));
        const eggsCount = r.eggs || 0;
        const milkCount = r.milk || 0;
        const herdBought = r.herd_bought || 0;
        const herdSold = r.herd_sold || 0;
        const herdBuyCost = r.herd_buy_cost || 0;
        const herdSellRevenue = r.herd_sell_revenue || 0;
        const herdNet = herdSellRevenue - herdBuyCost;
        const itemsTotal = eggsCount + milkCount;
        const itemsRevenue = itemsTotal * PRICE_PER_ITEM;
        const totalRevenue = itemsRevenue + herdNet;

        embeds.push({
          title: `${i + 1}. ${display}`,
          description: `Collected Items: **${itemsTotal}**`,
          color: 0xe67e22,
          fields: [
            { name: "Eggs", value: String(eggsCount), inline: true },
            { name: "Milk", value: String(milkCount), inline: true },
            { name: "Herd Bought", value: String(herdBought), inline: true },
            { name: "Herd Sold", value: String(herdSold), inline: true },
            { name: "Herd Net", value: `$${herdNet.toFixed(2)}`, inline: false },
            { name: "Total Revenue", value: `$${totalRevenue.toFixed(2)}`, inline: false }
          ],
          footer: { text: `Leaderboard — ${since || "all-time"}` },
          timestamp: new Date().toISOString()
        });
      }

      await interaction.deferReply({ ephemeral: true });
      await sendEmbedsInBatches(interaction, embeds);
      await interaction.followUp({ content: `Posted ${embeds.length} embeds (per-person) in this channel.`, ephemeral: true });
      return;
    }

    if (name === "weekly_totals") {
      const since_ts = sevenDaysAgoTs();
      const rows = weeklyPerUser.all({ since_ts });

      if (!rows || rows.length === 0) {
        await interaction.reply("No data in the last 7 days.");
        return;
      }

      // overall totals for header
      const overallTotals = sumTotalsSince.get({ since_ts, ranch_id: null, discord_id: null });
      const overallEggs = overallTotals.eggs || 0;
      const overallMilk = overallTotals.milk || 0;
      const overallItems = overallEggs + overallMilk;

      // compute herd totals in JS
      const overallHerdBuyCost = db.prepare(`SELECT COALESCE(SUM(value),0) AS v FROM gathers WHERE ts >= @since_ts AND item_type='herd_buy'`).get({ since_ts }).v || 0;
      const overallHerdSellRevenue = db.prepare(`SELECT COALESCE(SUM(value),0) AS v FROM gathers WHERE ts >= @since_ts AND item_type='herd_sell'`).get({ since_ts }).v || 0;
      const overallHerdNet = overallHerdSellRevenue - overallHerdBuyCost;
      const overallRevenue = (overallItems * PRICE_PER_ITEM) + overallHerdNet;

      const embeds = [];

      // summary embed
      embeds.push({
        title: `Weekly Summary — last 7 days`,
        description: `🥚 Eggs: **${overallEggs}**  |  🥛 Milk: **${overallMilk}**  |  Total Items: **${overallItems}**`,
        color: 0x2ecc71,
        fields: [
          { name: "Items Revenue", value: `$${(overallItems * PRICE_PER_ITEM).toFixed(2)}`, inline: true },
          { name: "Herd Net", value: `$${overallHerdNet.toFixed(2)}`, inline: true },
          { name: "Total Revenue", value: `$${overallRevenue.toFixed(2)}`, inline: false }
        ],
        timestamp: new Date().toISOString()
      });

      // per-person embeds
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const uid = r.discord_id;
        const display = await getDisplayNameForGuild(uid, interaction.guild).catch(() => (uid || "Unknown"));
        const eggsCount = r.eggs || 0;
        const milkCount = r.milk || 0;
        const herdBought = r.herd_bought || 0;
        const herdSold = r.herd_sold || 0;
        const herdBuyCost = r.herd_buy_cost || 0;
        const herdSellRevenue = r.herd_sell_revenue || 0;
        const herdNet = herdSellRevenue - herdBuyCost;
        const itemsTotal = eggsCount + milkCount;
        const itemsRevenue = itemsTotal * PRICE_PER_ITEM;
        const totalRevenue = itemsRevenue + herdNet;

        embeds.push({
          title: `${i + 1}. ${display}`,
          description: `Total collected: **${itemsTotal}**`,
          color: 0x1abc9c,
          fields: [
            { name: "Eggs", value: String(eggsCount), inline: true },
            { name: "Milk", value: String(milkCount), inline: true },
            { name: "Herd Bought", value: String(herdBought), inline: true },
            { name: "Herd Sold", value: String(herdSold), inline: true },
            { name: "Herd Net", value: `$${herdNet.toFixed(2)}`, inline: false },
            { name: "Total Revenue", value: `$${totalRevenue.toFixed(2)}`, inline: false }
          ],
          footer: { text: "Weekly totals — last 7 days" },
          timestamp: new Date().toISOString()
        });
      }

      await interaction.deferReply({ ephemeral: true });
      await sendEmbedsInBatches(interaction, embeds);
      await interaction.followUp({ content: `Posted ${embeds.length - 1} per-person embeds (plus 1 summary) in this channel.`, ephemeral: true });
      return;
    }

    if (name === "reset_week") {
      const memberPerms = interaction.memberPermissions;
      let allowed = false;
      try { allowed = memberPerms && typeof memberPerms.has === "function" && memberPerms.has(PermissionsBitField.Flags.ManageGuild); } catch(e) { allowed = false; }
      if (!allowed) {
        await interaction.reply({ content: "You need the **Manage Server** permission to run this command.", ephemeral: true });
        return;
      }
      const info = deleteSince.run({ since_ts: sevenDaysAgoTs(), ranch_id: null });
      const deleted = info.changes || 0;
      await interaction.reply({ content: `✅ Weekly totals have been reset. Deleted ${deleted} logged entries.`, ephemeral: true });
      return;
    }

    if (name === "subscribe_reports") {
      addSubscriber.run({ discord_id: interaction.user.id });
      await interaction.reply({ content: "✅ You are now subscribed to weekly DM reports.", ephemeral: true });
      return;
    }

    if (name === "unsubscribe_reports") {
      removeSubscriber.run({ discord_id: interaction.user.id });
      await interaction.reply({ content: "✅ You have been unsubscribed from weekly DM reports.", ephemeral: true });
      return;
    }

    if (name === "set_report_schedule") {
      const memberPerms = interaction.memberPermissions;
      let allowed = false;
      try { allowed = memberPerms && typeof memberPerms.has === "function" && memberPerms.has(PermissionsBitField.Flags.ManageGuild); } catch(e) { allowed = false; }
      if (!allowed) {
        await interaction.reply({ content: "You need the **Manage Server** permission to run this command.", ephemeral: true });
        return;
      }

      const weekday = interaction.options.getInteger("weekday");
      const hour = interaction.options.getInteger("hour");
      const minute = interaction.options.getInteger("minute");
      if (weekday === null || hour === null || minute === null) {
        await interaction.reply({ content: "Invalid arguments. Provide weekday, hour, and minute.", ephemeral: true });
        return;
      }
      if (weekday < 0 || weekday > 6 || hour < 0 || hour > 23 || minute < 0 || minute > 59) {
        await interaction.reply({ content: "Invalid range. weekday 0-6, hour 0-23, minute 0-59.", ephemeral: true });
        return;
      }

      setScheduleMeta(weekday, hour, minute);
      setLastReportDate(null);
      await interaction.reply({ content: `✅ Report schedule updated: weekday=${weekday} hour=${hour} minute=${minute} (America/Toronto).`, ephemeral: true });
      console.log("Report schedule updated by", interaction.user.id, { weekday, hour, minute });
      return;
    }

    if (name === "get_report_schedule") {
      const schedule = getScheduleMeta();
      await interaction.reply({ content: `Current report schedule: weekday=${schedule.weekday} hour=${schedule.hour} minute=${schedule.minute} (America/Toronto).`, ephemeral: true });
      return;
    }

    if (name === "run_weekly_report_now") {
      const memberPerms = interaction.memberPermissions;
      let allowed = false;
      try { allowed = memberPerms && typeof memberPerms.has === "function" && memberPerms.has(PermissionsBitField.Flags.ManageGuild); } catch(e) { allowed = false; }
      if (!allowed) {
        await interaction.reply({ content: "You need the **Manage Server** permission to run this command.", ephemeral: true });
        return;
      }
      await interaction.reply({ content: "Running weekly report now...", ephemeral: true });
      await performWeeklyReportAndReset();
      await interaction.followUp({ content: "Weekly report (test) completed.", ephemeral: true });
      return;
    }

  } catch (err) {
    console.error("interaction error:", err);
    try { if (!interaction.replied) await interaction.reply({ content: "An error occurred while running that command.", ephemeral: true }); } catch(e) {}
  }
});

// ----------------- Start -----------------
client.once("ready", () => {
  console.log("🤖 Logged in as", client.user.tag);
  startScheduler();
});

client.login(token);

// keep-alive http server so Railway treats this as a service
import http from "http";

const PORT = process.env.PORT || 3000;

http.createServer((req, res) => {
  res.writeHead(200);
  res.end("Ranch bot running");
}).listen(PORT, () => {
  console.log("Health server running on port", PORT);
});










