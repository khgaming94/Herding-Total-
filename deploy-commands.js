// deploy-commands.js
import { REST, Routes, SlashCommandBuilder } from "discord.js";

const token = process.env.DISCORD_TOKEN;
const clientId = process.env.CLIENT_ID;
const guildId = process.env.GUILD_ID;

if (!token || !clientId || !guildId) {
  console.error("❌ Missing DISCORD_TOKEN, CLIENT_ID, or GUILD_ID (set them in Replit Secrets)");
  process.exit(1);
}

(async () => {
  try {
    const commands = [
      new SlashCommandBuilder()
        .setName("totals")
        .setDescription("Show totals for eggs and milk")
        .addStringOption(o =>
          o.setName("since")
            .setDescription('Time filter like "24h", "7d", "30d"')
            .setRequired(false)
        ),

      new SlashCommandBuilder()
        .setName("leaderboard")
        .setDescription("Top collectors of eggs and milk (posts per-person embeds)")
        .addStringOption(o =>
          o.setName("since")
            .setDescription('Time filter like "24h", "7d", "30d"')
            .setRequired(false)
        )
        .addIntegerOption(o =>
          o.setName("limit")
            .setDescription("Limit number of collectors shown (default 50)")
            .setRequired(false)
        ),

      new SlashCommandBuilder()
        .setName("weekly_totals")
        .setDescription("Show totals for eggs and milk for the last 7 days (posts per-person embeds)"),

      new SlashCommandBuilder()
        .setName("reset_week")
        .setDescription("ADMIN: Reset (delete) all logged entries in the last 7 days"),

      new SlashCommandBuilder()
        .setName("subscribe_reports")
        .setDescription("Subscribe to weekly DM reports (Mon 09:00 America/Toronto)"),

      new SlashCommandBuilder()
        .setName("unsubscribe_reports")
        .setDescription("Unsubscribe from weekly DM reports"),

      new SlashCommandBuilder()
        .setName("set_report_schedule")
        .setDescription("ADMIN: Set the weekly report schedule (weekday 0-6, hour 0-23, minute 0-59)")
        .addIntegerOption(o =>
          o.setName("weekday")
            .setDescription("Day of week: 0=Sunday,1=Monday,...,6=Saturday")
            .setRequired(true)
        )
        .addIntegerOption(o =>
          o.setName("hour")
            .setDescription("Hour (0-23) in America/Toronto time")
            .setRequired(true)
        )
        .addIntegerOption(o =>
          o.setName("minute")
            .setDescription("Minute (0-59)")
            .setRequired(true)
        ),

      new SlashCommandBuilder()
        .setName("get_report_schedule")
        .setDescription("Show the current weekly DM report schedule"),

      new SlashCommandBuilder()
        .setName("run_weekly_report_now")
        .setDescription("ADMIN: Run the weekly report & reset immediately for testing (admin only)")
    ].map(c => c.toJSON());

    const rest = new REST({ version: "10" }).setToken(token);
    await rest.put(Routes.applicationGuildCommands(clientId, guildId), { body: commands });

    console.log("✅ Slash commands deployed successfully.");
  } catch (err) {
    console.error("Failed to deploy commands:", err);
    process.exit(1);
  }
})();






