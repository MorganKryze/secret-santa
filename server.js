const express = require('express');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const cors = require('cors');
const fs = require('fs').promises;

// Logging utility
function log(level, message, data = null) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] [${level.toUpperCase()}] ${message}`;
  if (data) {
    console.log(logMessage, JSON.stringify(data, null, 2));
  } else {
    console.log(logMessage);
  }
}

// Set timezone from environment variable
const TZ = process.env.TZ || 'Australia/Sydney';
process.env.TZ = TZ;

const app = express();
const PORT = process.env.PORT || 8003;
const BASE_URL = process.env.BASE_URL || 'http://localhost:8003';

// Log startup configuration
log('info', '=== Secret Santa Application Starting ===');
log('info', 'Configuration:', {
  NODE_ENV: process.env.NODE_ENV || 'development',
  PORT: PORT,
  BASE_URL: BASE_URL,
  TZ: TZ,
  CWD: process.cwd(),
  __dirname: __dirname
});

// Persistent storage using JSON files
const DATA_DIR = path.join(__dirname, 'data');
const PARTIES_FILE = path.join(DATA_DIR, 'parties.json');
const ASSIGNMENTS_FILE = path.join(DATA_DIR, 'assignments.json');
const GUEST_LINKS_FILE = path.join(DATA_DIR, 'guest_links.json');

log('info', 'Data paths configured:', {
  DATA_DIR: DATA_DIR,
  PARTIES_FILE: PARTIES_FILE,
  ASSIGNMENTS_FILE: ASSIGNMENTS_FILE,
  GUEST_LINKS_FILE: GUEST_LINKS_FILE
});

// In-memory storage
const parties = new Map();
const assignments = new Map();
const guestLinks = new Map(); // guestId -> {partyId, guestName}

// Load data from files on startup
async function loadData() {
  try {
    log('info', 'Ensuring data directory exists:', { path: DATA_DIR });
    try {
      await fs.mkdir(DATA_DIR, { recursive: true });
      log('info', 'Data directory created/verified');
    } catch (mkdirError) {
      log('error', 'Failed to create data directory:', {
        error: mkdirError.message,
        code: mkdirError.code
      });
      throw mkdirError;
    }
    
    // Check directory permissions - CRITICAL: fail if not writable
    try {
      const stats = await fs.stat(DATA_DIR);
      log('info', 'Data directory stats:', {
        exists: true,
        isDirectory: stats.isDirectory(),
        mode: stats.mode.toString(8),
        uid: stats.uid,
        gid: stats.gid
      });

      // Test write permissions - MANDATORY
      const testFile = path.join(DATA_DIR, '.write-test');
      await fs.writeFile(testFile, 'test');
      await fs.unlink(testFile);
      log('info', 'Data directory is writable ✓');
    } catch (error) {
      log('error', 'FATAL: Data directory is not writable!', {
        error: error.message,
        code: error.code,
        path: DATA_DIR,
        processUid: process.getuid?.(),
        processGid: process.getgid?.()
      });
      log('error', 'Cannot continue without writable data directory. Exiting.');
      process.exit(1);
    }

    // Initialize empty data files if they don't exist
    try {
      await fs.access(PARTIES_FILE);
      log('info', 'parties.json exists');
    } catch {
      log('info', 'Creating empty parties.json');
      await fs.writeFile(PARTIES_FILE, '{}');
    }

    try {
      await fs.access(ASSIGNMENTS_FILE);
      log('info', 'assignments.json exists');
    } catch {
      log('info', 'Creating empty assignments.json');
      await fs.writeFile(ASSIGNMENTS_FILE, '{}');
    }

    try {
      await fs.access(GUEST_LINKS_FILE);
      log('info', 'guest_links.json exists');
    } catch {
      log('info', 'Creating empty guest_links.json');
      await fs.writeFile(GUEST_LINKS_FILE, '{}');
    }

    log('info', 'Loading data files...');
    const [partiesData, assignmentsData, guestLinksData] = await Promise.all([
      fs.readFile(PARTIES_FILE, 'utf8').catch(err => {
        log('warn', 'Could not read parties.json:', err.code);
        return '{}';
      }),
      fs.readFile(ASSIGNMENTS_FILE, 'utf8').catch(err => {
        log('warn', 'Could not read assignments.json:', err.code);
        return '{}';
      }),
      fs.readFile(GUEST_LINKS_FILE, 'utf8').catch(err => {
        log('warn', 'Could not read guest_links.json:', err.code);
        return '{}';
      })
    ]);
    
    // Safe JSON parsing with fallback
    let partiesObj = {};
    let assignmentsObj = {};
    let guestLinksObj = {};
    
    try {
      partiesObj = JSON.parse(partiesData);
      log('info', `Loaded ${Object.keys(partiesObj).length} parties`);
    } catch (e) {
      log('error', 'Corrupted parties.json, starting fresh:', e.message);
    }
    
    try {
      assignmentsObj = JSON.parse(assignmentsData);
      log('info', `Loaded ${Object.keys(assignmentsObj).length} assignments`);
    } catch (e) {
      log('error', 'Corrupted assignments.json, starting fresh:', e.message);
    }
    
    try {
      guestLinksObj = JSON.parse(guestLinksData);
      log('info', `Loaded ${Object.keys(guestLinksObj).length} guest links`);
    } catch (e) {
      log('error', 'Corrupted guest_links.json, starting fresh:', e.message);
    }
    
    Object.entries(partiesObj).forEach(([key, value]) => parties.set(key, value));
    Object.entries(assignmentsObj).forEach(([key, value]) => assignments.set(key, value));
    Object.entries(guestLinksObj).forEach(([key, value]) => guestLinks.set(key, value));
    
    log('info', 'Data loaded successfully to memory');
  } catch (error) {
    log('error', 'Error loading data:', error.message);
    log('error', 'Stack trace:', error.stack);
  }
}

// Save data to files with atomic writes and backup
let saveInProgress = false;
async function saveData() {
  if (saveInProgress) {
    log('warn', 'Save already in progress, skipping');
    return;
  }
  
  saveInProgress = true;
  const saveStartTime = Date.now();
  try {
    log('info', 'Starting save operation...');
    // Create backup before overwriting
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupDir = path.join(DATA_DIR, 'backups');
    await fs.mkdir(backupDir, { recursive: true });
    
    // Backup existing files if they exist
    for (const [file, filename] of [
      [PARTIES_FILE, 'parties.json'],
      [ASSIGNMENTS_FILE, 'assignments.json'],
      [GUEST_LINKS_FILE, 'guest_links.json']
    ]) {
      try {
        const data = await fs.readFile(file, 'utf8');
        await fs.writeFile(path.join(backupDir, `${filename}.${timestamp}.backup`), data);
      } catch (e) {
        // File doesn't exist, skip backup
      }
    }
    
    // Write new data atomically
    const partiesData = JSON.stringify(Object.fromEntries(parties), null, 2);
    const assignmentsData = JSON.stringify(Object.fromEntries(assignments), null, 2);
    const guestLinksData = JSON.stringify(Object.fromEntries(guestLinks), null, 2);
    
    log('info', 'Writing data files:', {
      partiesSize: partiesData.length,
      assignmentsSize: assignmentsData.length,
      guestLinksSize: guestLinksData.length
    });

    await Promise.all([
      fs.writeFile(PARTIES_FILE, partiesData).then(() =>
        log('info', `Wrote ${PARTIES_FILE}`)
      ).catch(err => {
        log('error', `Failed to write ${PARTIES_FILE}:`, err.message);
        throw err;
      }),
      fs.writeFile(ASSIGNMENTS_FILE, assignmentsData).then(() =>
        log('info', `Wrote ${ASSIGNMENTS_FILE}`)
      ).catch(err => {
        log('error', `Failed to write ${ASSIGNMENTS_FILE}:`, err.message);
        throw err;
      }),
      fs.writeFile(GUEST_LINKS_FILE, guestLinksData).then(() =>
        log('info', `Wrote ${GUEST_LINKS_FILE}`)
      ).catch(err => {
        log('error', `Failed to write ${GUEST_LINKS_FILE}:`, err.message);
        throw err;
      })
    ]);

    // Verify files were actually written
    const [p, a, g] = await Promise.all([
      fs.stat(PARTIES_FILE).catch(() => null),
      fs.stat(ASSIGNMENTS_FILE).catch(() => null),
      fs.stat(GUEST_LINKS_FILE).catch(() => null)
    ]);
    
    log('info', 'Files verification:', {
      parties: p ? `${p.size} bytes` : 'MISSING!',
      assignments: a ? `${a.size} bytes` : 'MISSING!',
      guestLinks: g ? `${g.size} bytes` : 'MISSING!'
    });

    const saveTime = Date.now() - saveStartTime;
    log('info', `Data saved successfully in ${saveTime}ms`);
  } catch (error) {
    log('error', 'Error saving data:', error.message);
    log('error', 'Stack trace:', error.stack);
  } finally {
    saveInProgress = false;
  }
}

// Validation helpers
function sanitizeString(str, maxLength = 100) {
  if (typeof str !== 'string') return '';
  return str.trim().slice(0, maxLength).replace(/[<>"'&]/g, '');
}

function validateGuestName(name) {
  const sanitized = sanitizeString(name, 50);
  if (!sanitized || sanitized.length < 1) {
    throw new Error('Guest name cannot be empty');
  }
  return sanitized;
}

function validatePartyName(name) {
  const sanitized = sanitizeString(name, 100);
  if (!sanitized || sanitized.length < 1) {
    throw new Error('Party name cannot be empty');
  }
  return sanitized;
}

// Rate limiting (simple in-memory)
const rateLimitMap = new Map();
const RATE_LIMIT_WINDOW = 60000; // 1 minute
const MAX_REQUESTS_PER_WINDOW = 10;

function isRateLimited(clientId) {
  const now = Date.now();
  const requests = rateLimitMap.get(clientId) || [];
  const recentRequests = requests.filter(time => now - time < RATE_LIMIT_WINDOW);
  
  if (recentRequests.length >= MAX_REQUESTS_PER_WINDOW) {
    return true;
  }
  
  recentRequests.push(now);
  rateLimitMap.set(clientId, recentRequests);
  return false;
}

// Request logging middleware
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const duration = Date.now() - start;
    log('info', `${req.method} ${req.path} ${res.statusCode} ${duration}ms`, {
      ip: req.ip,
      userAgent: req.get('user-agent')?.substring(0, 50)
    });
  });
  next();
});

// Security headers
app.use((req, res, next) => {
  // Content Security Policy
  res.setHeader(
    'Content-Security-Policy',
    "default-src 'self'; " +
    "script-src 'self' 'unsafe-inline' https://cdn.tailwindcss.com; " +
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.tailwindcss.com; " +
    "font-src 'self' https://fonts.gstatic.com; " +
    "img-src 'self' data:; " +
    "connect-src 'self'; " +
    "frame-ancestors 'none'; " +
    "base-uri 'self'; " +
    "form-action 'self'"
  );
  // Other security headers
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'geolocation=(), microphone=(), camera=()');
  next();
});

app.use(cors());
app.use(express.json({ limit: '10kb' })); // Reduced from 10mb

// Log static file directory
const publicDir = path.join(__dirname, 'public');
log('info', 'Public directory path:', { publicDir });
fs.stat(publicDir).then(stats => {
  log('info', 'Public directory exists:', { isDirectory: stats.isDirectory() });
  return fs.readdir(publicDir);
}).then(files => {
  log('info', 'Public directory contents:', files);
}).catch(err => {
  log('error', 'Public directory not accessible:', err.message);
});

app.use(express.static(publicDir, {
  maxAge: '1d',
  etag: true,
  lastModified: true
}));

// Serve main page
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Create a new party
app.post('/api/parties', async (req, res) => {
  try {
    log('info', 'Party creation request received');
    const clientId = req.ip || req.connection.remoteAddress;
    if (isRateLimited(clientId)) {
      log('warn', 'Rate limit exceeded for client:', { clientId });
      return res.status(429).json({ error: 'Too many requests. Please wait a minute.' });
    }

    const { name, budget, criteria, guests } = req.body;
    log('info', 'Party details:', { name, guestCount: guests?.length });
    
    if (!name || !guests || !Array.isArray(guests) || guests.length < 2) {
      return res.status(400).json({ error: 'Party name and at least 2 guests are required' });
    }

    if (guests.length > 50) {
      return res.status(400).json({ error: 'Maximum 50 guests allowed' });
    }

    // Validate and sanitize inputs
    const sanitizedName = validatePartyName(name);
    const sanitizedBudget = sanitizeString(budget || '', 50);
    const sanitizedCriteria = sanitizeString(criteria || '', 500);
    
    const sanitizedGuests = guests.map(g => {
      try {
        return validateGuestName(g);
      } catch (e) {
        throw new Error(`Invalid guest name: ${g}`);
      }
    });
    
    // Check for duplicate guest names
    const uniqueGuests = [...new Set(sanitizedGuests)];
    if (uniqueGuests.length !== sanitizedGuests.length) {
      return res.status(400).json({ error: 'Guest names must be unique' });
    }

    const partyId = uuidv4();
    const party = {
      id: partyId,
      name: sanitizedName,
      budget: sanitizedBudget,
      criteria: sanitizedCriteria,
      guests: sanitizedGuests,
      createdAt: new Date().toLocaleString('en-AU', { 
        timeZone: TZ,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
      })
    };

    parties.set(partyId, party);
    
    // Generate unique links for each guest
    const guestUrls = {};
    party.guests.forEach(guest => {
      const guestId = uuidv4();
      guestLinks.set(guestId, {
        partyId: partyId,
        guestName: guest
      });
      guestUrls[guest] = `${BASE_URL}/guest/${guestId}`;
    });
    
    log('info', 'Party created successfully:', { partyId, guestCount: party.guests.length });
    await saveData();
    
    res.json({ 
      partyId, 
      guestUrls,
      party 
    });
  } catch (error) {
    log('error', 'Error creating party:', error.message);
    res.status(400).json({ error: error.message || 'Invalid request data' });
  }
});

// Get party details (for reference only)
app.get('/api/parties/:id', (req, res) => {
  const party = parties.get(req.params.id);
  if (!party) {
    return res.status(404).json({ error: 'Party not found' });
  }
  res.json(party);
});

// Assign Secret Santa
app.post('/api/parties/:id/assign', async (req, res) => {
  try {
    const clientId = req.ip || req.connection.remoteAddress;
    if (isRateLimited(clientId)) {
      return res.status(429).json({ error: 'Too many requests. Please wait a minute.' });
    }

    const { guestName } = req.body;
    const partyId = req.params.id;
    
    if (!guestName || typeof guestName !== 'string') {
      return res.status(400).json({ error: 'Guest name is required' });
    }

    const sanitizedGuestName = sanitizeString(guestName, 50);
    if (!sanitizedGuestName) {
      return res.status(400).json({ error: 'Invalid guest name' });
    }

    const party = parties.get(partyId);
    
    if (!party) {
      return res.status(404).json({ error: 'Party not found' });
    }

    if (!party.guests.includes(sanitizedGuestName)) {
      return res.status(400).json({ error: 'Guest not found in party' });
    }

    // Check if already assigned
    const existingAssignment = assignments.get(`${partyId}-${sanitizedGuestName}`);
    if (existingAssignment) {
      return res.json({ assignment: existingAssignment });
    }

    // Get or create assignments for this party
    if (!assignments.has(partyId)) {
      const shuffled = [...party.guests];
      for (let i = shuffled.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
      }
      
      // Ensure no one gets themselves
      for (let i = 0; i < shuffled.length; i++) {
        if (shuffled[i] === party.guests[i]) {
          if (i === shuffled.length - 1) {
            [shuffled[i], shuffled[i - 1]] = [shuffled[i - 1], shuffled[i]];
          } else {
            [shuffled[i], shuffled[i + 1]] = [shuffled[i + 1], shuffled[i]];
          }
        }
      }

      const partyAssignments = {};
      party.guests.forEach((guest, index) => {
        partyAssignments[guest] = shuffled[index];
      });
      
      assignments.set(partyId, partyAssignments);
    }

    const partyAssignments = assignments.get(partyId);
    const assignment = partyAssignments[sanitizedGuestName];
    
    await saveData();
    
    res.json({ assignment });
  } catch (error) {
    console.error('Error assigning Secret Santa:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get guest assignment by guest ID
app.get('/api/guest/:id/assignment', async (req, res) => {
  try {
    const clientId = req.ip || req.connection.remoteAddress;
    if (isRateLimited(clientId)) {
      return res.status(429).json({ error: 'Too many requests. Please wait a minute.' });
    }

    const guestId = req.params.id;
    
    if (!guestId || typeof guestId !== 'string' || guestId.length !== 36) {
      return res.status(400).json({ error: 'Invalid guest ID' });
    }

    const guestLink = guestLinks.get(guestId);
    
    if (!guestLink) {
      return res.status(404).json({ error: 'Guest link not found' });
    }
    
    const party = parties.get(guestLink.partyId);
    if (!party) {
      return res.status(404).json({ error: 'Party not found' });
    }
    
    // Get or create assignments for this party
    if (!assignments.has(guestLink.partyId)) {
      const shuffled = [...party.guests];
      for (let i = shuffled.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
      }
      
      // Ensure no one gets themselves
      for (let i = 0; i < shuffled.length; i++) {
        if (shuffled[i] === party.guests[i]) {
          if (i === shuffled.length - 1) {
            [shuffled[i], shuffled[i - 1]] = [shuffled[i - 1], shuffled[i]];
          } else {
            [shuffled[i], shuffled[i + 1]] = [shuffled[i + 1], shuffled[i]];
          }
        }
      }

      const partyAssignments = {};
      party.guests.forEach((guest, index) => {
        partyAssignments[guest] = shuffled[index];
      });
      
      assignments.set(guestLink.partyId, partyAssignments);
      await saveData();
    }

    const partyAssignments = assignments.get(guestLink.partyId);
    const assignment = partyAssignments[guestLink.guestName];
    
    res.json({ 
      party: {
        name: party.name,
        budget: party.budget,
        criteria: party.criteria
      },
      guestName: guestLink.guestName,
      assignment 
    });
  } catch (error) {
    console.error('Error getting guest assignment:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Serve guest page
app.get('/guest/:id', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'guest.html'));
});

// 404 handler for debugging
app.use((req, res) => {
  log('warn', `404 Not Found: ${req.method} ${req.url}`, {
    ip: req.ip,
    headers: req.headers
  });
  res.status(404).json({ error: 'Not found' });
});

// Error handler
app.use((err, req, res, next) => {
  log('error', 'Unhandled error:', {
    message: err.message,
    stack: err.stack,
    url: req.url
  });
  res.status(500).json({ error: 'Internal server error' });
});

// Pre-startup check
(async () => {
  try {
    log('info', 'Pre-startup check: verifying data directory...');
    await fs.stat(DATA_DIR).catch(async () => {
      log('warn', 'Data directory does not exist yet, will be created on startup');
    });
  } catch (error) {
    log('warn', 'Pre-startup check warning:', error.message);
  }
})();

app.listen(PORT, '0.0.0.0', async () => {
  log('info', '=== Server Started ===');
  log('info', `Listening on http://0.0.0.0:${PORT}`);
  log('info', `Access URL: ${BASE_URL}`);
  log('info', 'Process info:', {
    pid: process.pid,
    uid: process.getuid?.(),
    gid: process.getgid?.(),
    platform: process.platform,
    nodeVersion: process.version
  });

  await loadData(); log('info', '=== Server Ready ===');
  log('info', 'Available routes:');
  log('info', '  GET  /');
  log('info', '  GET  /guest/:id');
  log('info', '  POST /api/parties');
  log('info', '  GET  /api/parties/:id');
  log('info', '  POST /api/parties/:id/assign');
  log('info', '  GET  /api/guest/:id/assignment');
});
