require('dotenv').config();
const express = require('express');
const spotify = require('./spotify');
const fs = require('fs');
const path = require('path');
const tmi = require('tmi.js');

const DATA_FILE = path.join(__dirname, 'data.json');
const SAVE_INTERVAL_MS = 60_000;
const CLAIM_AMOUNT = 1000;
const CLAIM_COOLDOWN_MS = 24 * 60 * 60 * 1000;
const BONUS_AMOUNT = 20_000;
const BONUS_MIN_INTERVAL_MS = 30 * 60 * 1000;
const BONUS_MAX_INTERVAL_MS = 120 * 60 * 1000;
const WORDLE_REWARD = 1000;
const REMINDER_CHECK_INTERVAL_MS = 5_000;

const opts = {
  identity: {
    username: process.env.BOT_USERNAME,
    password: process.env.OAUTH_TOKEN
  },
  channels: ['marlon', 'pogix__', 'Alexmoderat', process.env.CHANNEL].filter(Boolean),
  connection: { reconnect: true, secure: true }
};

const client = new tmi.Client(opts);
const prefix = process.env.PREFIX || '-';

let state = {
  coins: {},
  lastClaim: {},
  lastSeen: {},
  bonusActive: false,
  bonusExpiresAt: 0,
  lastBonusWinner: null,
  channelEmotes: {},
  emoteCountsByChannel: {},
  userEmoteCountsByChannel: {},
  messageCounts: {},
  currentWord: null,
  lastFollowerId: null,
  reminders: [],
  spotifyTokens: {},
  spotifyVerifiers: {},
  dailyStats: {}
};

function loadState() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const raw = fs.readFileSync(DATA_FILE, 'utf8');
      const parsed = JSON.parse(raw);
      state = Object.assign(state, parsed);
      state.coins = state.coins || {};
      state.lastClaim = state.lastClaim || {};
      state.lastSeen = state.lastSeen || {};
      state.channelEmotes = state.channelEmotes || {};
      state.emoteCountsByChannel = state.emoteCountsByChannel || {};
      state.userEmoteCountsByChannel = state.userEmoteCountsByChannel || {};
      state.messageCounts = state.messageCounts || {};
      state.reminders = state.reminders || [];
      state.spotifyTokens = state.spotifyTokens || {};
      state.spotifyVerifiers = state.spotifyVerifiers || {};
      state.dailyStats = state.dailyStats || {};
    }
  } catch (err) {
    console.error('Failed to load state:', err);
  }
}

function saveState() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2), 'utf8');
  } catch (err) {
    console.error('Failed to save state:', err);
  }
}

function ensureUser(u) {
  const user = (u || '').toLowerCase();
  if (!state.coins[user]) state.coins[user] = 0;
  if (!state.lastClaim[user]) state.lastClaim[user] = 0;
  if (!state.lastSeen[user]) state.lastSeen[user] = { ts: 0, message: '' };
  return user;
}

function msToHms(ms) {
  if (ms <= 0) return '0s';
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h) return `${h}h ${m}m ${sec}s`;
  if (m) return `${m}m ${sec}s`;
  return `${sec}s`;
}

function todayKey() {
  const d = new Date();
  return d.toISOString().slice(0, 10);
}

function ensureChannelMaps(channel) {
  const ch = (channel || '').replace(/^#/, '').toLowerCase();
  if (!state.channelEmotes[ch]) state.channelEmotes[ch] = {};
  if (!state.emoteCountsByChannel[ch]) state.emoteCountsByChannel[ch] = {};
  if (!state.userEmoteCountsByChannel[ch]) state.userEmoteCountsByChannel[ch] = {};
  return ch;
}

function registerEmoteForChannel(token, channel) {
  if (!token) return false;
  const ch = ensureChannelMaps(channel);
  if (!state.channelEmotes[ch][token]) {
    state.channelEmotes[ch][token] = true;
    if (!state.emoteCountsByChannel[ch][token]) state.emoteCountsByChannel[ch][token] = 0;
    return true;
  }
  return false;
}

function countEmotesInMessage(message, username, channel, userstate) {
  if (!message) return;
  const ch = ensureChannelMaps(channel);
  const emotesTag = userstate.emotes || {};
  if (!emotesTag || Object.keys(emotesTag).length === 0) return;

  const detectedTokens = [];
  Object.entries(emotesTag).forEach(([emoteId, positions]) => {
    positions.forEach(posStr => {
      const [start, end] = posStr.split('-').map(Number);
      if (start >= 0 && end >= start) {
        const token = message.substring(start, end + 1).trim();
        if (token) {
          detectedTokens.push(token);
          registerEmoteForChannel(token, channel);
        }
      }
    });
  });

  const user = (username || '').toLowerCase();
  if (!state.userEmoteCountsByChannel[ch][user]) {
    state.userEmoteCountsByChannel[ch][user] = {};
  }

  detectedTokens.forEach(token => {
    state.emoteCountsByChannel[ch][token] = (state.emoteCountsByChannel[ch][token] || 0) + 1;
    state.userEmoteCountsByChannel[ch][user][token] =
      (state.userEmoteCountsByChannel[ch][user][token] || 0) + 1;
  });
}

function findEmoteKeyInChannel(query, channel) {
  if (!query) return null;
  const ch = ensureChannelMaps(channel);
  const channelMap = state.channelEmotes[ch];
  if (channelMap[query]) return query;
  const qLower = query.toLowerCase();
  for (const key of Object.keys(channelMap)) {
    if (key.toLowerCase() === qLower) return key;
  }
  return null;
}

function makeReminderId() {
  return `${Date.now()}-${Math.floor(Math.random() * 100000)}`;
}

function parseTimeToken(token) {
  if (!token) return null;
  const m = token.toString().trim().toLowerCase().match(/^(\d+)(s|m|h|d)$/);
  if (!m) return null;
  const val = parseInt(m[1], 10);
  const unit = m[2];
  if (unit === 's') return val * 1000;
  if (unit === 'm') return val * 60 * 1000;
  if (unit === 'h') return val * 60 * 60 * 1000;
  if (unit === 'd') return val * 24 * 60 * 60 * 1000;
  return null;
}

function scheduleTimedReminder(from, to, msg, delayMs) {
  const rem = {
    id: makeReminderId(),
    type: 'timed',
    from: (from || '').toLowerCase(),
    to: (to || '').toLowerCase(),
    msg,
    dueTs: Date.now() + delayMs
  };
  state.reminders.push(rem);
  saveState();
  return rem;
}

function addOnNextChatReminder(from, to, msg) {
  const rem = {
    id: makeReminderId(),
    type: 'onNextChat',
    from: (from || '').toLowerCase(),
    to: (to || '').toLowerCase(),
    msg
  };
  state.reminders.push(rem);
  saveState();
  return rem;
}

function checkTimedReminders() {
  const now = Date.now();
  const due = state.reminders.filter(r => r.type === 'timed' && r.dueTs <= now);
  if (!due.length) return;
  for (const r of due) {
    for (const ch of opts.channels) {
      client.action(ch, `@${r.to} you have a reminder from @${r.from}: ${r.msg}`);
    }
    state.reminders = state.reminders.filter(x => x.id !== r.id);
  }
  saveState();
}

function deliverOnNextChatRemindersFor(user, channel) {
  const uname = (user || '').toLowerCase();
  const pending = state.reminders.filter(r => r.type === 'onNextChat' && r.to === uname);
  if (!pending.length) return;
  for (const r of pending) {
    client.action(channel, `@${r.to} you have a reminder from @${r.from}: ${r.msg}`);
  }
  state.reminders = state.reminders.filter(r => !(r.type === 'onNextChat' && r.to === uname));
  saveState();
}

let bonusTimer = null;
function scheduleNextBonus() {
  const min = BONUS_MIN_INTERVAL_MS;
  const max = BONUS_MAX_INTERVAL_MS;
  const delay = Math.floor(Math.random() * (max - min + 1)) + min;
  if (bonusTimer) clearTimeout(bonusTimer);
  bonusTimer = setTimeout(() => {
    state.bonusActive = true;
    state.bonusExpiresAt = Date.now() + (10 * 60 * 1000);
    state.lastBonusWinner = null;
    for (const ch of opts.channels) {
      client.action(ch, `🎉 Bonus! First person to type ${prefix}bonus in chat within 10 minutes wins ${BONUS_AMOUNT.toLocaleString()} coins!`);
    }
    setTimeout(() => {
      if (state.bonusActive) {
        state.bonusActive = false;
        for (const ch of opts.channels) {
          client.action(ch, `Bonus expired — no one claimed it in time.`);
        }
      }
      scheduleNextBonus();
    }, 10 * 60 * 1000);
  }, delay);
}

function ensureDayStats(dayKey) {
  if (!state.dailyStats[dayKey]) {
    state.dailyStats[dayKey] = {
      subs: 0,
      follows: 0,
      bitsByUser: {},
      peakViewers: 0,
      sumViewers: 0,
      viewerSamples: 0
    };
  }
  return state.dailyStats[dayKey];
}

async function pollFollowers() {
  const clientId = process.env.CLIENT_ID;
  const helixToken = process.env.HELIX_TOKEN;
  const broadcasterName = process.env.BROADCASTER_NAME || (process.env.CHANNEL || '').replace(/^#/, '');
  if (!clientId || !helixToken || !broadcasterName) return;

  try {
    const userRes = await fetch(`https://api.twitch.tv/helix/users?login=${encodeURIComponent(broadcasterName)}`, {
      headers: { 'Client-ID': clientId, 'Authorization': `Bearer ${helixToken}` }
    });
    const userJson = await userRes.json();
    if (!userJson.data || !userJson.data.length) return;
    const broadcasterId = userJson.data[0].id;

    const followRes = await fetch(`https://api.twitch.tv/helix/users/follows?to_id=${broadcasterId}&first=1`, {
      headers: { 'Client-ID': clientId, 'Authorization': `Bearer ${helixToken}` }
    });
    const followJson = await followRes.json();
    if (!followJson.data || !followJson.data.length) return;
    const latest = followJson.data[0];
    if (!state.lastFollowerId) {
      state.lastFollowerId = latest.from_id;
      saveState();
      return;
    }
    if (latest.from_id !== state.lastFollowerId) {
      const followerName = latest.from_name;
      for (const ch of opts.channels) {
        client.action(ch, `Thank you for following @${followerName}`);
      }
      const dayKey = todayKey();
      const stats = ensureDayStats(dayKey);
      stats.follows += 1;
      state.lastFollowerId = latest.from_id;
      saveState();
    }
  } catch (err) {
    console.error('Follower poll error:', err);
  }
}

function isAdmin(userstate) {
  return userstate.mod || (userstate.badges && userstate.badges.broadcaster);
}

loadState();
const app = express();
const PORT = process.env.PORT || 3000;

app.get('/spotify/connect', (req, res) => {
  const username = req.query.user;
  if (!username) {
    return res.status(400).send('Missing user parameter');
  }

  const { url, codeVerifier } = spotify.getAuthorizationUrl(username);
  state.spotifyVerifiers[username] = codeVerifier;
  saveState();

  res.redirect(url);
});

app.get('/spotify/callback', async (req, res) => {
  const { code, state: username } = req.query;
  if (!code || !username) {
    return res.status(400).send('Missing code or state parameter');
  }

  const codeVerifier = state.spotifyVerifiers[username];
  if (!codeVerifier) {
    return res.status(400).send('No authorization request found for this user');
  }

  try {
    const tokenData = await spotify.exchangeCodeForToken(code, codeVerifier);
    state.spotifyTokens[username] = tokenData;
    delete state.spotifyVerifiers[username];
    saveState();

    res.send(`<h1>Success!</h1><p>Your Spotify account is now connected to your Twitch bot. You can close this window and use <strong>-song</strong> in chat!</p>`);
  } catch (err) {
    res.status(500).send(`Error connecting Spotify: ${err.message}`);
  }
});

app.listen(PORT, () => {
  console.log(`Spotify auth server running on http://127.0.0.1:${PORT}`);
});

client.connect().catch(err => console.error('Connection error:', err));
client.on('connected', (addr, port) => {
  console.log(`Connected to ${addr}:${port}`);
  scheduleNextBonus();
  setInterval(pollFollowers, 60_000);
  setInterval(checkTimedReminders, REMINDER_CHECK_INTERVAL_MS);
});

setInterval(saveState, SAVE_INTERVAL_MS);

// sub / bits tracking
client.on('subscription', (channel, username, method, message, userstate) => {
  const dayKey = todayKey();
  const stats = ensureDayStats(dayKey);
  stats.subs += 1;
  saveState();
});

client.on('resub', (channel, username, months, message, userstate, methods) => {
  const dayKey = todayKey();
  const stats = ensureDayStats(dayKey);
  stats.subs += 1;
  saveState();
});

client.on('subgift', (channel, username, streakMonths, recipient, methods, userstate) => {
  const dayKey = todayKey();
  const stats = ensureDayStats(dayKey);
  const giftCount = methods && methods['msg-param-mass-gift-count'] ? Number(methods['msg-param-mass-gift-count']) : 1;
  stats.subs += giftCount;
  saveState();
});

client.on('cheer', (channel, userstate, message) => {
  const bits = parseInt(userstate.bits || '0', 10);
  if (!bits) return;
  const dayKey = todayKey();
  const stats = ensureDayStats(dayKey);
  const user = (userstate['display-name'] || userstate.username || '').toLowerCase();
  if (!stats.bitsByUser[user]) stats.bitsByUser[user] = 0;
  stats.bitsByUser[user] += bits;
  saveState();
});

client.on('message', async (channel, userstate, message, self) => {
  if (self) return;
  const display = (userstate['display-name'] || userstate.username || '').toLowerCase();
  if (!display) return;

  state.lastSeen[display] = { ts: Date.now(), message: message };
  ensureUser(display);

  const day = todayKey();
  if (!state.messageCounts[day]) state.messageCounts[day] = {};
  if (!state.messageCounts[day][display]) state.messageCounts[day][display] = 0;
  state.messageCounts[day][display] += 1;

  countEmotesInMessage(message, display, channel, userstate);
  deliverOnNextChatRemindersFor(display, channel);

  if (!message.startsWith(prefix)) {
    saveState();
    return;
  }

  const rawArgs = message.slice(prefix.length).trim();
  if (!rawArgs) {
    saveState();
    return;
  }

  const tokens = rawArgs.split(/\s+/).filter(Boolean);
  const cmd = tokens.shift().toLowerCase();
  const reply = (txt) => client.action(channel, txt);

  if (cmd === 'ping') {
    reply(`@${display} pong`);
    saveState();
    return;
  }

  if (cmd === 'songconnect') {
    const username = display;
    const { url, codeVerifier } = spotify.getAuthorizationUrl(username);
    state.spotifyVerifiers[username] = codeVerifier;
    reply(`@${display} connect your Spotify here: http://127.0.0.1:${PORT}/spotify/connect?user=${username}`);
    saveState();
    return;
  }

  if (cmd === 'song') {
    const username = display;
    const tokenData = state.spotifyTokens[username];

    if (!tokenData) {
      reply(`@${display} you haven't connected your Spotify yet. Use ${prefix}songconnect`);
      saveState();
      return;
    }

    let accessToken = tokenData.accessToken;
    if (Date.now() >= tokenData.expiresAt) {
      try {
        const newTokenData = await spotify.refreshAccessToken(tokenData.refreshToken);
        state.spotifyTokens[username] = newTokenData;
        saveState();
        accessToken = newTokenData.accessToken;
      } catch (err) {
        reply(`@${display} error refreshing Spotify token. Try ${prefix}songconnect again`);
        saveState();
        return;
      }
    }

    try {
      const track = await spotify.getCurrentlyPlaying(accessToken);
      if (!track) {
        reply(`@${display} you're not currently playing anything on Spotify`);
      } else if (track.isPlaying) {
        reply(`@${display} is listening to: "${track.name}" by ${track.artists}`);
      } else {
        reply(`@${display} paused: "${track.name}" by ${track.artists}`);
      }
    } catch (err) {
      reply(`@${display} error getting current track: ${err.message}`);
    }
    saveState();
    return;
  }

  if (cmd === 'help') {
    reply(
      `Commands: ${prefix}ping | ${prefix}help | ${prefix}balance | ${prefix}claim | ${prefix}gamble <amount> | ${prefix}bonus | ${prefix}steal @user <amount> | ${prefix}lastseen | ${prefix}badge | ${prefix}ecount <emote> | ${prefix}mytopused | ${prefix}topemotes | ${prefix}w <word> | ${prefix}setword <word> (mod) | ${prefix}lotd | ${prefix}topchatters | ${prefix}stats | ${prefix}remindme <msg> <time> | ${prefix}remind <user> <msg> [<time>]`
    );
    saveState();
    return;
  }

  if (cmd === 'balance' || cmd === 'bal') {
    const target = (tokens[0] || display).replace(/^@/, '').toLowerCase();
    ensureUser(target);
    reply(`${target} has ${state.coins[target].toLocaleString()} coins`);
    saveState();
    return;
  }

  if (cmd === 'claim') {
    ensureUser(display);
    const last = state.lastClaim[display] || 0;
    const now = Date.now();
    const diff = now - last;
    if (diff >= CLAIM_COOLDOWN_MS) {
      state.coins[display] += CLAIM_AMOUNT;
      state.lastClaim[display] = now;
      reply(`@${display} claimed ${CLAIM_AMOUNT.toLocaleString()} coins! New balance: ${state.coins[display].toLocaleString()}`);
    } else {
      const remaining = CLAIM_COOLDOWN_MS - diff;
      reply(`@${display} you can claim again in ${msToHms(remaining)}`);
    }
    saveState();
    return;
  }

  if (cmd === 'gamble') {
    ensureUser(display);
    const raw = tokens[0];
    if (!raw) {
      reply(`Usage: ${prefix}gamble <amount>`);
      saveState();
      return;
    }
    const amount = parseInt(raw.replace(/,/g, ''), 10);
    if (!Number.isFinite(amount) || amount <= 0) {
      reply(`@${display} enter a valid positive amount to gamble.`);
      saveState();
      return;
    }
    if (state.coins[display] < amount) {
      reply(`@${display} you don't have enough coins. Your balance: ${state.coins[display].toLocaleString()}`);
      saveState();
      return;
    }
    const win = Math.random() < 0.5;
    if (win) {
      state.coins[display] += amount;
      reply(`@${display} won ${amount.toLocaleString()} coins! New balance: ${state.coins[display].toLocaleString()}`);
    } else {
      state.coins[display] -= amount;
      reply(`@${display} lost ${amount.toLocaleString()} coins. New balance: ${state.coins[display].toLocaleString()}`);
    }
    saveState();
    return;
  }

  if (cmd === 'bonus') {
    ensureUser(display);
    if (!state.bonusActive) {
      reply(`@${display} there is no active bonus right now.`);
      saveState();
      return;
    }
    if (state.lastBonusWinner) {
      reply(`@${display} bonus already claimed by ${state.lastBonusWinner}`);
      saveState();
      return;
    }
    state.coins[display] += BONUS_AMOUNT;
    state.lastBonusWinner = display;
    state.bonusActive = false;
    state.bonusExpiresAt = 0;
    reply(`🎉 @${display} claimed the bonus and won ${BONUS_AMOUNT.toLocaleString()} coins! New balance: ${state.coins[display].toLocaleString()}`);
    scheduleNextBonus();
    saveState();
    return;
  }

  if (cmd === 'steal') {
    ensureUser(display);
    const targetRaw = tokens[0] || '';
    const amountRaw = tokens[1] || tokens[0];
    let target = targetRaw.replace(/^@/, '').toLowerCase();
    let amount = parseInt((amountRaw || '').replace(/,/g, ''), 10);

    if (!target || !amount || !Number.isFinite(amount) || amount <= 0) {
      reply(`Usage: ${prefix}steal @user <amount>`);
      saveState();
      return;
    }
    if (target === display) {
      reply(`@${display} you cannot steal from yourself.`);
      saveState();
      return;
    }
    ensureUser(target);
    if (state.coins[target] < amount) {
      reply(`@${display} target ${target} does not have enough coins to steal that amount.`);
      saveState();
      return;
    }
    if (state.coins[display] < amount) {
      reply(`@${display} you don't have enough coins to attempt that steal (you need at least ${amount.toLocaleString()}).`);
      saveState();
      return;
    }
    const success = Math.random() < 0.5;
    if (success) {
      state.coins[target] -= amount;
      state.coins[display] += amount;
      reply(`@${display} successfully stole ${amount.toLocaleString()} coins from ${target}! New balance: ${state.coins[display].toLocaleString()}`);
    } else {
      state.coins[display] -= amount;
      state.coins[target] += amount;
      reply(`@${display} failed the steal and paid ${amount.toLocaleString()} coins to ${target}. New balance: ${state.coins[display].toLocaleString()}`);
    }
    saveState();
    return;
  }

  if (cmd === 'w') {
    ensureUser(display);
    const guess = (tokens[0] || '').toLowerCase();
    if (!guess) {
      reply(`Usage: ${prefix}w <word>`);
      saveState();
      return;
    }
    if (!state.currentWord) {
      reply(`No active word is set right now. Try again later.`);
      saveState();
      return;
    }
    if (guess === state.currentWord.toLowerCase()) {
      state.coins[display] += WORDLE_REWARD;
      reply(`🎉 @${display} guessed the word correctly and won ${WORDLE_REWARD.toLocaleString()} coins! New balance: ${state.coins[display].toLocaleString()}`);
      state.currentWord = null;
      saveState();
    } else {
      reply(`@${display} incorrect guess. Try again!`);
    }
    return;
  }

  if (cmd === 'setword' && isAdmin(userstate)) {
    const w = (tokens[0] || '').toLowerCase();
    if (!w) {
      reply(`Usage: ${prefix}setword <word> (mod/broadcaster only)`);
      saveState();
      return;
    }
    state.currentWord = w;
    reply(`Secret word has been set (hidden).`);
    saveState();
    return;
  }

  if (cmd === 'lastseen') {
    const target = (tokens[0] || display).replace(/^@/, '').toLowerCase();
    const rec = state.lastSeen[target];
    if (rec && rec.ts) {
      const ago = msToHms(Date.now() - rec.ts);
      reply(`${target} was last seen ${ago} ago saying: "${rec.message}"`);
    } else {
      reply(`No record for ${target}`);
    }
    saveState();
    return;
  }

  if (cmd === 'badge' || cmd === 'badges') {
    const targetRaw = tokens[0] || display;
    const targetName = targetRaw.replace(/^@/, '').toLowerCase();

    if (targetName !== display) {
      reply(`@${display} I can only show your own badges right now. Use ${prefix}badge with no arguments.`);
      saveState();
      return;
    }

    const badges = userstate.badges || {};
    if (!badges || Object.keys(badges).length === 0) {
      reply(`@${display} you are not showing any badges right now.`);
      saveState();
      return;
    }

    const badgeNames = {
      broadcaster: 'Broadcaster',
      moderator: 'Moderator',
      mod: 'Moderator',
      vip: 'VIP',
      subscriber: 'Subscriber',
      founder: 'Founder',
      bits: 'Bits',
      bitsleader: 'Bits Leader',
      premium: 'Prime Gaming',
      partner: 'Partner',
      staff: 'Twitch Staff',
      admin: 'Twitch Admin',
      global_mod: 'Global Moderator',
      artist: 'Artist',
      turbo: 'Turbo',
      sub_gifter: 'Sub Gifter',
      predictions: 'Predictions',
      predictions_blue: 'Predictions Blue',
      predictions_pink: 'Predictions Pink',
      no_audio: 'No Audio',
      no_video: 'No Video'
    };

    const parts = Object.entries(badges).map(([key, value]) => {
      const nice = badgeNames[key] || key;
      return `${nice}${value ? ` (tier ${value})` : ''}`;
    });

    reply(`@${display} your active badges: ${parts.join(', ')}`);
    saveState();
    return;
  }

  if (cmd === 'ecount') {
    const emoteQuery = (tokens[0] || '').trim();
    if (!emoteQuery) {
      reply(`Usage: ${prefix}ecount <emote>`);
      saveState();
      return;
    }
    const foundKey = findEmoteKeyInChannel(emoteQuery, channel);
    const ch = ensureChannelMaps(channel);
    if (!foundKey) {
      reply(`Emote "${emoteQuery}" has been used 0 times in this channel (or is not tracked).`);
      saveState();
      return;
    }
    const count = state.emoteCountsByChannel[ch][foundKey] || 0;
    reply(`Emote "${foundKey}" has been used ${count.toLocaleString()} times in this channel.`);
    saveState();
    return;
  }

  if (cmd === 'mytopused') {
    const ch = ensureChannelMaps(channel);
    const user = display.toLowerCase();
    const userMap = state.userEmoteCountsByChannel[ch][user] || {};
    const entries = Object.entries(userMap).filter(([emote]) => state.channelEmotes[ch][emote]);
    if (!entries.length) {
      reply(`@${display} you have no tracked emote usage in this channel yet.`);
      saveState();
      return;
    }
    entries.sort((a, b) => b[1] - a[1]);
    const top = entries.slice(0, 5).map(([emote, cnt]) => `${emote} (${cnt})`);
    reply(`@${display} your top emotes in this channel: ${top.join(', ')}`);
    saveState();
    return;
  }

  if (cmd === 'topemotes') {
    const ch = ensureChannelMaps(channel);
    const entries = Object.entries(state.emoteCountsByChannel[ch] || {}).filter(
      ([emote]) => state.channelEmotes[ch][emote]
    );
    if (!entries.length) {
      reply(`No emote usage recorded for this channel yet.`);
      saveState();
      return;
    }
    entries.sort((a, b) => b[1] - a[1]);
    const top = entries.slice(0, 5).map(([emote, cnt]) => `${emote} (${cnt})`);
    reply(`Top emotes in this channel: ${top.join(', ')}`);
    saveState();
    return;
  }

  if (cmd === 'lotd') {
    const dayKey = todayKey();
    const counts = state.messageCounts[dayKey] || {};
    let topUser = null;
    let topCount = 0;
    for (const [u, c] of Object.entries(counts)) {
      if (c > topCount) {
        topCount = c;
        topUser = u;
      }
    }
    if (!topUser) {
      reply(`No messages recorded for today yet.`);
    } else {
      reply(`Loser of the day is ${topUser} with ${topCount.toLocaleString()} messages!`);
    }
    saveState();
    return;
  }

  if (cmd === 'topchatters') {
    const dayKey = todayKey();
    const counts = state.messageCounts[dayKey] || {};
    const entries = Object.entries(counts);
    if (!entries.length) {
      reply(`No chat messages recorded for today yet.`);
      saveState();
      return;
    }
    entries.sort((a, b) => b[1] - a[1]);
    const top = entries.slice(0, 5).map(([user, count], idx) =>
      `${idx + 1}) ${user} (${count.toLocaleString()})`
    );
    reply(`Top chatters today: ${top.join(', ')}`);
    saveState();
    return;
  }

  if (cmd === 'stats') {
    const dayKey = todayKey();
    const stats = ensureDayStats(dayKey);
    const counts = state.messageCounts[dayKey] || {};

    let topUser = null;
    let topCount = 0;
    for (const [u, c] of Object.entries(counts)) {
      if (c > topCount) {
        topCount = c;
        topUser = u;
      }
    }

    let topBitsUser = null;
    let topBits = 0;
    for (const [u, bits] of Object.entries(stats.bitsByUser || {})) {
      if (bits > topBits) {
        topBits = bits;
        topBitsUser = u;
      }
    }

    const subsMsg = `Subs today: ${stats.subs || 0}`;
    const followsMsg = `Follows today: ${stats.follows || 0}`;
    const topChatterMsg = topUser
      ? `Top chatter: ${topUser} (${topCount.toLocaleString()} messages)`
      : `Top chatter: none`;
    const topBitsMsg = topBitsUser
      ? `Top bits: ${topBitsUser} (${topBits.toLocaleString()} bits)`
      : `Top bits: none`;

    let viewersMsg = '';
    if (stats.viewerSamples > 0) {
      const avg = Math.round(stats.sumViewers / stats.viewerSamples);
      viewersMsg = ` | Peak viewers: ${stats.peakViewers} | Avg viewers: ${avg}`;
    }

    reply(`${subsMsg} | ${followsMsg} | ${topChatterMsg} | ${topBitsMsg}${viewersMsg}`);
    saveState();
    return;
  }

  if (cmd === 'remindme') {
    if (tokens.length < 2) {
      reply(`Usage: ${prefix}remindme <msg> <time> (e.g., ${prefix}remindme take a break 10m)`);
      saveState();
      return;
    }
    const lastTok = tokens[tokens.length - 1];
    const delayMs = parseTimeToken(lastTok);
    if (delayMs === null) {
      reply(`Invalid time format. Use s,m,h,d (e.g., 30s, 10m, 1h)`);
      saveState();
      return;
    }
    const msgText = tokens.slice(0, tokens.length - 1).join(' ');
    scheduleTimedReminder(display, display, msgText, delayMs);
    reply(`@${display} reminder set in ${msToHms(delayMs)}: "${msgText}"`);
    saveState();
    return;
  }

  if (cmd === 'remind') {
    if (tokens.length < 2) {
      reply(`Usage: ${prefix}remind <user> <msg> [<time>] (e.g., ${prefix}remind @bob check DMs 5m)`);
      saveState();
      return;
    }
    const targetRaw = tokens[0];
    const target = targetRaw.replace(/^@/, '').toLowerCase();
    if (!target) {
      reply(`Invalid target user.`);
      saveState();
      return;
    }
    const possibleTime = tokens[tokens.length - 1];
    let delayMs = parseTimeToken(possibleTime);
    let msgTokens;
    if (delayMs !== null && tokens.length >= 3) {
      msgTokens = tokens.slice(1, tokens.length - 1);
    } else {
      delayMs = null;
      msgTokens = tokens.slice(1);
    }
    const msgText = msgTokens.join(' ');
    if (!msgText) {
      reply(`Please provide a message for the reminder.`);
      saveState();
      return;
    }
    if (delayMs !== null) {
      scheduleTimedReminder(display, target, msgText, delayMs);
      reply(`Reminder set for @${target} in ${msToHms(delayMs)}: "${msgText}"`);
      saveState();
      return;
    }
    addOnNextChatReminder(display, target, msgText);
    reply(`@${display} I will remind @${target} the next time they chat: "${msgText}"`);
    saveState();
    return;
  }

  if (cmd === 'give' && isAdmin(userstate)) {
    const who = (tokens[0] || '').replace(/^@/, '').toLowerCase();
    const amt = parseInt((tokens[1] || '').replace(/,/g, ''), 10);
    if (!who || !amt || amt <= 0) {
      reply(`Usage: ${prefix}give @user <amount> (mod only)`);
      saveState();
      return;
    }
    ensureUser(who);
    state.coins[who] += amt;
    reply(`${who} received ${amt.toLocaleString()} coins (new balance: ${state.coins[who].toLocaleString()})`);
    saveState();
    return;
  }

  if (cmd === 'take' && isAdmin(userstate)) {
    const who = (tokens[0] || '').replace(/^@/, '').toLowerCase();
    const amt = parseInt((tokens[1] || '').replace(/,/g, ''), 10);
    if (!who || !amt || amt <= 0) {
      reply(`Usage: ${prefix}take @user <amount> (mod only)`);
      saveState();
      return;
    }
    ensureUser(who);
    state.coins[who] = Math.max(0, state.coins[who] - amt);
    reply(`${who} lost ${amt.toLocaleString()} coins (new balance: ${state.coins[who].toLocaleString()})`);
    saveState();
    return;
  }

  if (cmd === 'join' && isAdmin(userstate)) {
    const chName = (tokens[0] || '').replace(/^#/, '').toLowerCase();
    if (!chName) {
      reply(`Usage: ${prefix}join <channel> (mod/broadcaster only)`);
    } else {
      client.join(chName).then(() => {
        reply(`Joined channel ${chName}`);
      }).catch(err => {
        reply(`Failed to join ${chName}: ${err.message}`);
      });
    }
    saveState();
    return;
  }

  if (cmd === 'part' && isAdmin(userstate)) {
    const chName = (tokens[0] || '').replace(/^#/, '').toLowerCase();
    if (!chName) {
      reply(`Usage: ${prefix}part <channel> (mod/broadcaster only)`);
    } else {
      client.part(chName).then(() => {
        reply(`Left channel ${chName}`);
      }).catch(err => {
        reply(`Failed to part ${chName}: ${err.message}`);
      });
    }
    saveState();
    return;
  }

  reply(`Unknown command: ${prefix}${cmd}. Try ${prefix}help`);
  saveState();
});

process.on('SIGINT', () => {
  console.log('Shutting down, saving state...');
  saveState();
  process.exit();
});
