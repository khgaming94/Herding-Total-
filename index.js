// index.js
// Ranch Discord bot — collects milk/eggs/herding from webhook messages and produces reports
import { Client, GatewayIntentBits, Partials, PermissionsBitField } from "discord.js";
import Database from "better-sqlite3";
import fetch from "node-fetch"; // node 18+ may have global fetch; this is for safety
import http from "http";

// ================= CONFIG =================
const PRICE_PER_ITEM = 1.25; // revenue per milk or egg
// per-animal buy prices — change these to set buy cost per animal
const HERD_PRICES = {
  bison: 60,
  cow: 40,
  goat: 20,
  pig: 30,
  sheep: 30,
  goat: 18,
  deer: 50,
  chicken: 10,
  pronghorn: 40,
  ram: 40
};
// ==========================================

// env
const token = process.env.DISCORD_TOKEN;
const listenChannelId = process.env.LISTEN_CHANNEL_ID;
const reportWebhookUrl = process.env.REPORT_WEBHOOK_URL || null;
const guildIdEnv = process.env.GUILD_ID || null;

if (!token || !listenChannelId) {
  console.error("Missing DISCORD_TOKEN or LISTEN_CHANNEL_ID in env.");
  process.exit(1);
}

// client
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent
  ],
  partials: [Partials.Channel]
});

// ----------------- DB -----------------
// use persistent path (Railway volume mount at /data)
const DB_PATH = process.env.DB_PATH || "/data/ranch.sqlite";
const db = new Database(DB_PATH);

// base schema + migration
db.exec(`
CREATE TABLE IF NOT EXISTS gathers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  channel_id TEXT NOT NULL,
  message_id TEXT NOT NULL UNIQUE,
  discord_id TEXT,
  ranch_id INTEGER,
  item_type TEXT NOT NULL,
  amount INTEGER NOT NULL,
  value REAL DEFAULT 0,
  subtype TEXT DEFAULT NULL
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

// prepared statements
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
LIMIT @limit
`);

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
const DUPLICATE_WINDOW_MS = 10 * 1000; // 10s
const DEFAULT_WEEKDAY = 1; // Mon
const DEFAULT_HOUR = 9;
const DEFAULT_MINUTE = 0;

// ----------------- Time helpers -----------------
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

// parse gathers: milk/eggs OR herd buy/sell (buys priced from HERD_PRICES)
function parseGather(text) {
  if (!text) return null;
  const uid = extractDiscordId(text);
  const actor = extractActorName(text);
  const ranchId = extractRanchId(text);
  const t = text.replace(/\s+/g, " ").trim();

  // milk / eggs pattern
  const near = t.match(/(\d+)\s*(eggs?|milk)\b/i);
  if (near) {
    const amount = Number(near[1]);
    const itemRaw = near[2].toLowerCase();
    const item_type = itemRaw.indexOf("milk") === 0 ? "milk" : "eggs";
    return { discord_id: uid, actor, ranch_id: ranchId, item_type, amount, value: 0, subtype: null };
  }

  // herd buy/sell pattern simplified:
  // matches "bought 5 Bison" or "sold 4 Bison ... for 960.0$"
  const herd = t.match(/\b(bought|sold)\b\s+(\d+)\s+([A-Za-z]+)\b/i);
  if (herd) {
    const action = herd[1].toLowerCase();
    const qty = parseInt(herd[2], 10);
    const animal = herd[3].toLowerCase();
    let value = 0;
    if (action === "bought") {
      // calculate buy cost from config
      const per = HERD_PRICES[animal] || 0;
      value = qty * per;
    } else {
      // for sells, try to extract the sale amount (if present)
      const saleMatch = t.match(/for\s+\$?([\d,\.]+)\$?/i);
      if (saleMatch) value = parseFloat(saleMatch[1].replace(/,/g,""));
    }
    return {
      discord_id: uid,
      actor,
      ranch_id: ranchId,
      item_type: action === "bought" ? "herd_buy" : "herd_sell",
      amount: qty,
      value,
      subtype: animal
    };
  }

  return null;
}

// ----------------- Helper: get server display name (nickname) -----------------
async function getDisplayNameForGuild(uid, guild) {
  if (!uid) return "Unknown";
  if (guild && guild.members && typeof guild.members.fetch === "function") {
    try {
      const member = await guild.members.fetch(uid).catch(() => null);
      if (member && member.displayName) return member.displayName;
    } catch (e) {}
  }
  try {
    const u = await client.users.fetch(uid).catch(() => null);
    if (u) return u.username + (u.discriminator ? ("#" + u.discriminator) : "");
  } catch (e) {}
  return uid;
}

// ----------------- Message listener -----------------
client.on("messageCreate", async (message) => {
  try {
    console.log("MSG:", "channelId=", message.channelId, "isWebhook=", !!message.webhookId, "contentLen=", (message.content || "").length, "embeds=", (message.embeds && message.embeds.length) ? message.embeds.length : 0);
    if (message.channelId !== listenChannelId) return;

    let text = message.content || "";
    if ((!text || text.trim() === "") && message.embeds && message.embeds.length > 0) {
      const e = message.embeds[0];
      const fields = (e.fields || []).map(f => `${f.name} ${f.value}`).join(" ");
      text = [e.title || "", e.description || "", fields].join(" ").trim();
    }

    console.log("TEXT_USED:", text);
    const parsed = parseGather(text);
    if (!parsed) {
      console.log("PARSE_FAIL");
      return;
    }

    // require mention to identify user
    if (!parsed.discord_id) {
      if (parsed.actor && /^unknown$/i.test(parsed.actor.trim())) {
        console.log("SKIP_UNKNOWN_ACTOR");
        return;
      }
      console.log("SKIP_NO_MENTION");
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
      console.log("DUPLICATE_SKIPPED");
      return;
    }

    // insert
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

    console.log("INSERTED GATHER:", {
      type: parsed.item_type,
      amount: parsed.amount,
      value: parsed.value || 0,
      subtype: parsed.subtype || null,
      user: parsed.discord_id,
      ranch: parsed.ranch_id || "n/a",
      sample: text.slice(0,200)
    });
  } catch (e) {
    if (String(e).includes("UNIQUE constraint failed")) return;
    console.error("ERROR in messageCreate:", e);
  }
});

// ----------------- Send embeds in batches -----------------
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
        console.error("No valid send target for embeds");
      }
    } catch (err) {
      console.error("Failed to send embed batch:", err);
    }
  }
}

// ----------------- Weekly report & reset -----------------
async function performWeeklyReportAndReset() {
  try {
    const since_ts = sevenDaysAgoTs();
    const totals = sumTotalsSince.get({ since_ts, ranch_id: null, discord_id: null });
    const eggs = totals.eggs || 0;
    const milk = totals.milk || 0;

    const rows = weeklyPerUser.all({ since_ts });

    // get guild for nicknames if possible
    let guild = null;
    if (guildIdEnv) {
      try { guild = client.guilds.cache.get(guildIdEnv) || await client.guilds.fetch(guildIdEnv).catch(() => null); } catch (e) { guild = null; }
    }

    const userRows = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const uid = r.discord_id;
      const display = await getDisplayNameForGuild(uid, guild).catch(() => uid || "Unknown");
      const eggsCount = r.eggs || 0;
      const milkCount = r.milk || 0;
      const herdBought = r.herd_bought || 0;
      const herdSold = r.herd_sold || 0;
      const herdBuyCost = r.herd_buy_cost || 0;
      const herdSellRevenue = r.herd_sell_revenue || 0;
      const herdNet = herdSellRevenue - herdBuyCost;
      const totalItems = eggsCount + milkCount;
      userRows.push({ discord_id: uid, display, eggs: eggsCount, milk: milkCount, totalItems, herdBought, herdSold, herdBuyCost, herdSellRevenue, herdNet });
    }

    // compute overall herd net
    const overallHerdBuyCost = db.prepare(`SELECT COALESCE(SUM(value),0) AS v FROM gathers WHERE ts >= @since_ts AND item_type='herd_buy'`).get({ since_ts }).v || 0;
    const overallHerdSellRevenue = db.prepare(`SELECT COALESCE(SUM(value),0) AS v FROM gathers WHERE ts >= @since_ts AND item_type='herd_sell'`).get({ since_ts }).v || 0;
    const overallHerdNet = overallHerdSellRevenue - overallHerdBuyCost;
    const overallItems = (eggs || 0) + (milk || 0);
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
      embeds.push({
        title: `${j + 1}. ${ur.display}`,
        description: `Items collected: **${ur.totalItems}**`,
        color: 0x3498db,
        fields: [
          { name: "Eggs", value: String(ur.eggs || 0), inline: true },
          { name: "Milk", value: String(ur.milk || 0), inline: true },
          { name: "Items Rev", value: `$${personItemsRevenue.toFixed(2)}`, inline: true },
          { name: "Herd Bought", value: String(ur.herdBought || 0), inline: true },
          { name: "Herd Sold", value: String(ur.herdSold || 0), inline: true },
          { name: "Herd Net", value: `$${(ur.herdNet || 0).toFixed(2)}`, inline: false },
          { name: "Total Revenue", value: `$${personTotalRevenue.toFixed(2)}`, inline: false }
        ],
        footer: { text: "Ranch report • last 7 days" },
        timestamp: new Date().toISOString()
      });
    }

    if (reportWebhookUrl) {
      for (let i = 0; i < embeds.length; i += 10) {
        const batch = embeds.slice(i, i + 10);
        await fetch(reportWebhookUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ username: "Ranch Report", embeds: batch })
        }).catch(e => console.error("Webhook post error:", e));
        await new Promise(r => setTimeout(r, 200));
      }
    } else {
      const subs = listSubscribers.all();
      if (!subs || subs.length === 0) {
        console.log("weekly report: no subscribers to DM and no REPORT_WEBHOOK_URL set. Skipping reset.");
        return;
      }
      let dmText = `Weekly Ranch Totals (last 7 days)\nEggs: ${eggs}\nMilk: ${milk}\nTotal items: ${overallItems}\nItems Revenue: $${(overallItems*PRICE_PER_ITEM).toFixed(2)}\nHerd Net: $${overallHerdNet.toFixed(2)}\nTotal Revenue: $${overallRevenue.toFixed(2)}\n\nPer-person:\n`;
      if (userRows.length === 0) dmText += "_No collectors found in the last 7 days._\n";
      else {
        for (let i = 0; i < userRows.length; i++) {
          const ur = userRows[i];
          const personItemsRevenue = ur.totalItems * PRICE_PER_ITEM;
          const personTotalRevenue = personItemsRevenue + (ur.herdNet || 0);
          dmText += `${i+1}. ${ur.display} — Eggs:${ur.eggs} Milk:${ur.milk} ItemsRev:$${personItemsRevenue.toFixed(2)} HerdBought:${ur.herdBought} HerdSold:${ur.herdSold} HerdNet:$${(ur.herdNet||0).toFixed(2)} Total:$${personTotalRevenue.toFixed(2)}\n`;
        }
      }
      for (const s of subs) {
        try {
          const u = await client.users.fetch(s.discord_id);
          if (u) await u.send(dmText).catch(e => console.error("DM failed:", e));
        } catch (e) { console.error("Failed to DM subscriber", s.discord_id, e); }
      }
    }

    // delete last 7 days entries
    const info = deleteSince.run({ since_ts, ranch_id: null });
    console.log("Weekly reset: deleted", info.changes || 0, "rows.");
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

// ----------------- Backfill helper (manual use) -----------------
async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/**
 * backfillHistory({ sinceTs, maxMessages })
 * - sinceTs: ms timestamp. If provided, stops at older messages.
 * - maxMessages: safety cap.
 */
async function backfillHistory({ sinceTs = null, maxMessages = 200000 } = {}) {
  try {
    const channel = await client.channels.fetch(listenChannelId).catch(e => { console.error("Failed to fetch channel", e); return null; });
    if (!channel || !channel.fetch) { console.error("Channel not fetchable"); return; }

    const resume = getMeta.get({ key: "history_last_fetched_id" });
    let beforeId = resume && resume.value ? resume.value : null;
    let inserted = 0;
    const BATCH = 100;

    while (inserted < maxMessages) {
      const opts = { limit: BATCH };
      if (beforeId) opts.before = beforeId;

      let batch;
      try { batch = await channel.messages.fetch(opts); } catch (e) { console.error("fetch error, sleeping 5s", e); await sleep(5000); continue; }
      if (!batch || batch.size === 0) break;

      const msgs = Array.from(batch.values()).reverse();
      for (const m of msgs) {
        if (sinceTs && m.createdTimestamp < sinceTs) { console.log("Reached sinceTs, stopping"); inserted = maxMessages; break; }
        let text = m.content || "";
        if ((!text || text.trim() === "") && m.embeds && m.embeds.length > 0) {
          const e = m.embeds[0];
          const fields = (e.fields || []).map(f => `${f.name} ${f.value}`).join(" ");
          text = [e.title || "", e.description || "", fields].join(" ").trim();
        }
        if (!text) continue;
        const parsed = parseGather(text);
        if (!parsed) continue;
        if (!parsed.discord_id) continue;

        try {
          insertGather.run({
            ts: m.createdTimestamp,
            channel_id: m.channelId,
            message_id: m.id,
            discord_id: parsed.discord_id,
            ranch_id: parsed.ranch_id,
            item_type: parsed.item_type,
            amount: parsed.amount,
            value: parsed.value || 0,
            subtype: parsed.subtype || null
          });
          inserted++;
          if (inserted % 50 === 0) console.log("backfill: inserted", inserted);
        } catch (e) {
          if (String(e).includes("UNIQUE constraint failed")) {
            // already logged
          } else console.error("DB insert error during backfill:", e);
        }
      }

      const oldest = batch.last();
      if (oldest && oldest.id) {
        beforeId = oldest.id;
        setMeta.run({ key: "history_last_fetched_id", value: beforeId });
      } else break;

      if (batch.size < BATCH) break;
      await sleep(500);
    }

    console.log("Backfill complete. inserted approx:", inserted);
  } catch (e) {
    console.error("backfillHistory fatal:", e);
  }
}

// ----------------- Interaction handlers (slash commands) -----------------
client.on("interactionCreate", async (interaction) => {
  let isChatInput = false;
  try { isChatInput = typeof interaction.isChatInputCommand === "function" ? interaction.isChatInputCommand() : !!interaction.commandName; } catch(e) { isChatInput = !!interaction.commandName; }
  if (!isChatInput) return;

  try {
    const name = interaction.commandName;

    if (name === "totals") {
      const since = interaction.options.getString("since");
      const sinceTs = parseSinceToTs(since);
      const totals = sumTotals.get({ ranch_id: null, since_ts: sinceTs });
      await interaction.reply(`**Totals**\n🥚 Eggs: **${totals.eggs}**\n🥛 Milk: **${totals.milk}**`);
      return;
    }

    if (name === "leaderboard") {
      const since = interaction.options.getString("since");
      const limit = Math.max(1, Math.min(200, interaction.options.getInteger("limit") || 50));
      const sinceTs = parseSinceToTs(since);
      const rows = topUsers.all({ ranch_id: null, since_ts: sinceTs, limit });

      if (!rows || rows.length === 0) { await interaction.reply("No data yet."); return; }

      const embeds = [];
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const uid = r.discord_id;
        const display = await getDisplayNameForGuild(uid, interaction.guild).catch(() => uid || "Unknown");
        const eggsCount = Number(r.eggs || 0);
        const milkCount = Number(r.milk || 0);
        const herdBought = Number(r.herd_bought || 0);
        const herdSold = Number(r.herd_sold || 0);
        const herdBuyCost = Number(r.herd_buy_cost || 0);
        const herdSellRevenue = Number(r.herd_sell_revenue || 0);
        const herdNet = herdSellRevenue - herdBuyCost;
        const itemsTotal = eggsCount + milkCount;
        const itemsRevenue = itemsTotal * PRICE_PER_ITEM;
        const totalRevenue = itemsRevenue + herdNet;

        embeds.push({
          title: `${i+1}. ${display}`,
          description: `Collected Items: **${itemsTotal}**`,
          color: 0xe67e22,
          fields: [
            { name: "Eggs", value: String(eggsCount), inline: true },
            { name: "Milk", value: String(milkCount), inline: true },
            { name: "Items Rev", value: `$${itemsRevenue.toFixed(2)}`, inline: true },
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
      await interaction.followUp({ content: `Posted ${embeds.length} embeds (per-person).`, ephemeral: true });
      return;
    }

    if (name === "weekly_totals") {
      const since_ts = sevenDaysAgoTs();
      const rows = weeklyPerUser.all({ since_ts });
      if (!rows || rows.length === 0) { await interaction.reply("No data in the last 7 days."); return; }

      const overallTotals = sumTotalsSince.get({ since_ts, ranch_id: null, discord_id: null });
      const overallEggs = overallTotals.eggs || 0;
      const overallMilk = overallTotals.milk || 0;
      const overallItems = overallEggs + overallMilk;
      const overallHerdBuyCost = db.prepare(`SELECT COALESCE(SUM(value),0) AS v FROM gathers WHERE ts >= @since_ts AND item_type='herd_buy'`).get({ since_ts }).v || 0;
      const overallHerdSellRevenue = db.prepare(`SELECT COALESCE(SUM(value),0) AS v FROM gathers WHERE ts >= @since_ts AND item_type='herd_sell'`).get({ since_ts }).v || 0;
      const overallHerdNet = overallHerdSellRevenue - overallHerdBuyCost;
      const overallRevenue = (overallItems * PRICE_PER_ITEM) + overallHerdNet;

      const embeds = [];
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

      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const uid = r.discord_id;
        const display = await getDisplayNameForGuild(uid, interaction.guild).catch(() => uid || "Unknown");
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
          title: `${i+1}. ${display}`,
          description: `Total collected: **${itemsTotal}**`,
          color: 0x1abc9c,
          fields: [
            { name: "Eggs", value: String(eggsCount), inline: true },
            { name: "Milk", value: String(milkCount), inline: true },
            { name: "Items Rev", value: `$${itemsRevenue.toFixed(2)}`, inline: true },
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
      await interaction.followUp({ content: `Posted ${embeds.length - 1} per-person embeds (plus 1 summary).`, ephemeral: true });
      return;
    }

    if (name === "reset_week") {
      const memberPerms = interaction.memberPermissions;
      const allowed = memberPerms && typeof memberPerms.has === "function" && memberPerms.has(PermissionsBitField.Flags.ManageGuild);
      if (!allowed) { await interaction.reply({ content: "You need Manage Server permission.", ephemeral: true }); return; }
      const info = deleteSince.run({ since_ts: sevenDaysAgoTs(), ranch_id: null });
      await interaction.reply({ content: `✅ Weekly totals reset. Deleted ${info.changes || 0} entries.`, ephemeral: true });
      return;
    }

    if (name === "subscribe_reports") {
      addSubscriber.run({ discord_id: interaction.user.id });
      await interaction.reply({ content: "✅ Subscribed to weekly DM reports.", ephemeral: true });
      return;
    }

    if (name === "unsubscribe_reports") {
      removeSubscriber.run({ discord_id: interaction.user.id });
      await interaction.reply({ content: "✅ Unsubscribed from weekly DM reports.", ephemeral: true });
      return;
    }

    if (name === "set_report_schedule") {
      const memberPerms = interaction.memberPermissions;
      const allowed = memberPerms && typeof memberPerms.has === "function" && memberPerms.has(PermissionsBitField.Flags.ManageGuild);
      if (!allowed) { await interaction.reply({ content: "You need Manage Server permission.", ephemeral: true }); return; }
      const weekday = interaction.options.getInteger("weekday");
      const hour = interaction.options.getInteger("hour");
      const minute = interaction.options.getInteger("minute");
      if (weekday === null || hour === null || minute === null) { await interaction.reply({ content: "Invalid args.", ephemeral: true }); return; }
      setScheduleMeta(weekday, hour, minute);
      setLastReportDate(null);
      await interaction.reply({ content: `✅ Report schedule set: weekday=${weekday} hour=${hour} minute=${minute} (America/Toronto).`, ephemeral: true });
      return;
    }

    if (name === "get_report_schedule") {
      const sch = getScheduleMeta();
      await interaction.reply({ content: `Schedule: weekday=${sch.weekday} hour=${sch.hour} minute=${sch.minute} (America/Toronto).`, ephemeral: true });
      return;
    }

    if (name === "run_weekly_report_now") {
      const memberPerms = interaction.memberPermissions;
      const allowed = memberPerms && typeof memberPerms.has === "function" && memberPerms.has(PermissionsBitField.Flags.ManageGuild);
      if (!allowed) { await interaction.reply({ content: "You need Manage Server permission.", ephemeral: true }); return; }
      await interaction.reply({ content: "Running weekly report now...", ephemeral: true });
      await performWeeklyReportAndReset();
      await interaction.followUp({ content: "Weekly report completed.", ephemeral: true });
      return;
    }

    if (name === "backfill") {
      // optional admin command if added to deploy-commands
      const memberPerms = interaction.memberPermissions;
      const allowed = memberPerms && typeof memberPerms.has === "function" && memberPerms.has(PermissionsBitField.Flags.ManageGuild);
      if (!allowed) { await interaction.reply({ content: "You need Manage Server permission.", ephemeral: true }); return; }
      const days = interaction.options.getInteger("days") || 365;
      const sinceTs = Date.now() - (days * 24 * 60 * 60 * 1000);
      await interaction.reply({ content: `Starting backfill for last ${days} days. Check logs.`, ephemeral: true });
      backfillHistory({ sinceTs }).then(() => interaction.followUp({ content: "Backfill finished.", ephemeral: true })).catch(err => interaction.followUp({ content: `Backfill error: ${String(err).slice(0,200)}`, ephemeral: true }));
      return;
    }

  } catch (err) {
    console.error("interaction error:", err);
    try { if (!interaction.replied) await interaction.reply({ content: "An error occurred.", ephemeral: true }); } catch(e) {}
  }
});

// ----------------- Keep-alive HTTP server (Railway service detection) -----------------
const PORT = process.env.PORT || 3000;
http.createServer((req, res) => { res.writeHead(200); res.end("Ranch bot running"); }).listen(PORT, () => {
  console.log("Health server running on port", PORT);
});

// ----------------- Last bits: ready/start -----------------
client.once("ready", async () => {
  console.log("🤖 Logged in as", client.user.tag);
  startScheduler();

  // OPTIONAL: one-time automatic backfill on first deploy — uncomment if you want auto import
  // const oneYearAgo = Date.now() - (365 * 24 * 60 * 60 * 1000);
  // console.log("Starting one-time backfill for last 365 days...");
  // await backfillHistory({ sinceTs: oneYearAgo, maxMessages: 200000 });
  // console.log("Backfill complete (one-time). Remove this block after use.");
});

client.login(token);








