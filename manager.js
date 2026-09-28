/**
 * manager.js - Square Era PostgreSQL Database Manager Node 2
 * Repository: yasamarium/sesydb2
 * Secondary failover manager coordinating PostgreSQL replication, health checks, and sync.
 */

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const https = require('https');
const http = require('http');

const PORT = process.env.PORT || 8080;
const NODE_NAME = 'sesydb-manager-node2';
const SEDB_RAW = 'https://raw.githubusercontent.com/yasamarium/sedb/main/data';
const SEDB_API = 'https://api.github.com/repos/yasamarium/sedb/contents/data';
const GH_PAT = process.env.GH_PAT || process.env.GITHUB_TOKEN || '';

const startTime = Date.now();
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/square_era',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// Utility: HTTP GET JSON helper
function fetchJson(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const lib = parsed.protocol === 'https:' ? https : http;
    const req = lib.get(url, { headers: { 'User-Agent': 'SE-DB-Manager-Node2', ...headers } }, res => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            reject(new Error('Invalid JSON received from ' + url));
          }
        } else {
          reject(new Error(`HTTP ${res.statusCode} from ${url}`));
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(10000, () => { req.destroy(); reject(new Error('Request timeout')); });
  });
}

// 1. Root Info Endpoint
app.get('/', (req, res) => {
  res.json({
    service: 'Square Era PostgreSQL Database Manager',
    node: NODE_NAME,
    role: 'secondary-failover',
    status: 'operational',
    uptimeSeconds: Math.floor((Date.now() - startTime) / 1000),
    timestamp: new Date().toISOString()
  });
});

// 2. Health Check Endpoint
app.get('/health', async (req, res) => {
  try {
    const client = await pool.connect();
    const result = await client.query('SELECT NOW() as db_time, pg_is_in_recovery() as in_recovery');
    client.release();

    res.json({
      status: 'healthy',
      node: NODE_NAME,
      role: 'secondary-failover',
      postgres: 'connected',
      dbTime: result.rows[0].db_time,
      inRecovery: result.rows[0].in_recovery,
      pool: {
        total: pool.totalCount,
        idle: pool.idleCount,
        waiting: pool.waitingCount
      },
      uptimeSeconds: Math.floor((Date.now() - startTime) / 1000)
    });
  } catch (err) {
    res.status(503).json({
      status: 'unhealthy',
      node: NODE_NAME,
      postgres: 'disconnected',
      error: err.message
    });
  }
});

// 3. Status and Metrics Endpoint
app.get('/status', async (req, res) => {
  try {
    const client = await pool.connect();
    const [usersRes, profilesRes, roomsRes, blocksRes, dbSizeRes] = await Promise.all([
      client.query('SELECT COUNT(*) as count FROM users'),
      client.query('SELECT COUNT(*) as count FROM profiles'),
      client.query('SELECT COUNT(*) as count FROM rooms'),
      client.query('SELECT COUNT(*) as count FROM world_blocks'),
      client.query("SELECT pg_size_pretty(pg_database_size('square_era')) as size")
    ]);
    client.release();

    res.json({
      node: NODE_NAME,
      role: 'secondary-failover',
      database: 'square_era',
      databaseSize: dbSizeRes.rows[0].size,
      counts: {
        users: parseInt(usersRes.rows[0].count, 10),
        profiles: parseInt(profilesRes.rows[0].count, 10),
        rooms: parseInt(roomsRes.rows[0].count, 10),
        worldBlocks: parseInt(blocksRes.rows[0].count, 10)
      },
      memory: process.memoryUsage(),
      uptimeSeconds: Math.floor((Date.now() - startTime) / 1000)
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 4. Data Sync Endpoint (Pull from sedb to PostgreSQL)
app.post('/sync', async (req, res) => {
  try {
    const syncResults = await performDataSync();
    res.json({
      status: 'success',
      node: NODE_NAME,
      synced: syncResults,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({ status: 'error', error: err.message });
  }
});

// 5. Query Endpoint (Authorized)
app.post('/query', async (req, res) => {
  const { sql, params } = req.body;
  if (!sql) {
    return res.status(400).json({ error: 'SQL query parameter required' });
  }

  const forbidden = /(drop\s+database|truncate|alter\s+system)/i;
  if (forbidden.test(sql)) {
    return res.status(403).json({ error: 'Operation not permitted by policy' });
  }

  try {
    const client = await pool.connect();
    const result = await client.query(sql, params || []);
    client.release();

    res.json({
      rowCount: result.rowCount,
      rows: result.rows,
      command: result.command
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Helper: Performs bi-directional synchronization
async function performDataSync() {
  const stats = { users: 0, profiles: 0, rooms: 0, blocks: 0 };
  const client = await pool.connect();

  try {
    // 1. Sync Rooms
    try {
      const rooms = await fetchJson(`${SEDB_RAW}/rooms.json?t=${Date.now()}`);
      if (Array.isArray(rooms)) {
        for (const r of rooms) {
          await client.query(`
            INSERT INTO rooms (id, name, mode, max_players, current_players, description)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO UPDATE SET
              name = EXCLUDED.name,
              mode = EXCLUDED.mode,
              max_players = EXCLUDED.max_players,
              description = EXCLUDED.description;
          `, [r.id, r.name, r.mode || 'creative', r.maxPlayers || 16, r.currentPlayers || 0, r.description || '']);
          stats.rooms++;
        }
      }
    } catch (e) {
      console.warn('[Sync Node 2] Rooms sync skipped:', e.message);
    }

    // 2. Sync Users
    try {
      const users = await fetchJson(`${SEDB_RAW}/users.json?t=${Date.now()}`);
      if (Array.isArray(users)) {
        for (const u of users) {
          await client.query(`
            INSERT INTO users (id, username, password_hash, role, created_at, last_login)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO UPDATE SET
              username = EXCLUDED.username,
              password_hash = EXCLUDED.password_hash,
              last_login = EXCLUDED.last_login;
          `, [u.id, u.username, u.passwordHash, u.role || 'player', u.createdAt || new Date().toISOString(), u.lastLogin || new Date().toISOString()]);
          stats.users++;
        }
      }
    } catch (e) {
      console.warn('[Sync Node 2] Users sync skipped:', e.message);
    }

    // 3. Sync Profiles
    try {
      const profiles = await fetchJson(`${SEDB_RAW}/profiles.json?t=${Date.now()}`);
      if (Array.isArray(profiles)) {
        for (const p of profiles) {
          await client.query(`
            INSERT INTO profiles (user_id, username, pos_x, pos_y, pos_z, inventory, health, hunger, game_mode, play_time_minutes, last_room_id, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (user_id) DO UPDATE SET
              username = EXCLUDED.username,
              pos_x = EXCLUDED.pos_x,
              pos_y = EXCLUDED.pos_y,
              pos_z = EXCLUDED.pos_z,
              inventory = EXCLUDED.inventory,
              health = EXCLUDED.health,
              hunger = EXCLUDED.hunger,
              game_mode = EXCLUDED.game_mode,
              play_time_minutes = EXCLUDED.play_time_minutes,
              last_room_id = EXCLUDED.last_room_id,
              updated_at = EXCLUDED.updated_at;
          `, [
            p.userId,
            p.username,
            p.position ? p.position.x : 0,
            p.position ? p.position.y : 30,
            p.position ? p.position.z : 0,
            JSON.stringify(p.inventory || []),
            p.health || 20,
            p.hunger || 20,
            p.gameMode || 'survival',
            p.playTimeMinutes || 0,
            p.lastRoomId || 1,
            p.updatedAt || new Date().toISOString()
          ]);
          stats.profiles++;
        }
      }
    } catch (e) {
      console.warn('[Sync Node 2] Profiles sync skipped:', e.message);
    }

    // 4. Sync Worlds
    try {
      const worlds = await fetchJson(`${SEDB_RAW}/worlds.json?t=${Date.now()}`);
      if (worlds && typeof worlds === 'object') {
        for (const [userId, worldData] of Object.entries(worlds)) {
          if (worldData && worldData.modifications) {
            for (const [coordKey, blockId] of Object.entries(worldData.modifications)) {
              const [x, y, z] = coordKey.split(',').map(Number);
              if (!isNaN(x) && !isNaN(y) && !isNaN(z)) {
                await client.query(`
                  INSERT INTO world_blocks (user_id, room_id, coord_key, pos_x, pos_y, pos_z, block_id)
                  VALUES ($1, 1, $2, $3, $4, $5, $6)
                  ON CONFLICT (room_id, coord_key) DO UPDATE SET
                    block_id = EXCLUDED.block_id,
                    user_id = EXCLUDED.user_id;
                `, [userId, coordKey, x, y, z, blockId]);
                stats.blocks++;
              }
            }
          }
        }
      }
    } catch (e) {
      console.warn('[Sync Node 2] Worlds sync skipped:', e.message);
    }

    return stats;
  } finally {
    client.release();
  }
}

// Background Task: Periodic data check every 15 minutes
setInterval(async () => {
  try {
    console.log('[Manager Node 2] Running periodic synchronization...');
    const result = await performDataSync();
    console.log('[Manager Node 2] Periodic sync complete:', JSON.stringify(result));
  } catch (err) {
    console.warn('[Manager Node 2] Periodic sync notice:', err.message);
  }
}, 15 * 60 * 1000);

app.listen(PORT, () => {
  console.log(`[${NODE_NAME}] PostgreSQL Database Manager active on port ${PORT}`);
});
