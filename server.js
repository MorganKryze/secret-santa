const express = require('express');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const cors = require('cors');
const fs = require('fs').promises;

// Set timezone to Australia/Sydney
process.env.TZ = 'Australia/Sydney';

const app = express();
const PORT = process.env.PORT || 8003;
const BASE_URL = process.env.BASE_URL || 'http://localhost:8003';

// Persistent storage using JSON files
const DATA_DIR = path.join(__dirname, 'data');
const PARTIES_FILE = path.join(DATA_DIR, 'parties.json');
const ASSIGNMENTS_FILE = path.join(DATA_DIR, 'assignments.json');
const GUEST_LINKS_FILE = path.join(DATA_DIR, 'guest_links.json');

// In-memory storage
const parties = new Map();
const assignments = new Map();
const guestLinks = new Map(); // guestId -> {partyId, guestName}

// Load data from files on startup
async function loadData() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    
    const [partiesData, assignmentsData, guestLinksData] = await Promise.all([
      fs.readFile(PARTIES_FILE, 'utf8').catch(() => '{}'),
      fs.readFile(ASSIGNMENTS_FILE, 'utf8').catch(() => '{}'),
      fs.readFile(GUEST_LINKS_FILE, 'utf8').catch(() => '{}')
    ]);
    
    // Safe JSON parsing with fallback
    let partiesObj = {};
    let assignmentsObj = {};
    let guestLinksObj = {};
    
    try {
      partiesObj = JSON.parse(partiesData);
    } catch (e) {
      console.error('Corrupted parties.json, starting fresh');
    }
    
    try {
      assignmentsObj = JSON.parse(assignmentsData);
    } catch (e) {
      console.error('Corrupted assignments.json, starting fresh');
    }
    
    try {
      guestLinksObj = JSON.parse(guestLinksData);
    } catch (e) {
      console.error('Corrupted guest_links.json, starting fresh');
    }
    
    Object.entries(partiesObj).forEach(([key, value]) => parties.set(key, value));
    Object.entries(assignmentsObj).forEach(([key, value]) => assignments.set(key, value));
    Object.entries(guestLinksObj).forEach(([key, value]) => guestLinks.set(key, value));
    
    console.log('Data loaded successfully');
  } catch (error) {
    console.error('Error loading data:', error);
  }
}

// Save data to files with atomic writes and backup
let saveInProgress = false;
async function saveData() {
  if (saveInProgress) {
    console.log('Save already in progress, skipping');
    return;
  }
  
  saveInProgress = true;
  try {
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
    
    await Promise.all([
      fs.writeFile(PARTIES_FILE, partiesData),
      fs.writeFile(ASSIGNMENTS_FILE, assignmentsData),
      fs.writeFile(GUEST_LINKS_FILE, guestLinksData)
    ]);
    
    console.log('Data saved successfully');
  } catch (error) {
    console.error('Error saving data:', error);
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
app.use(express.static('public', {
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
    const clientId = req.ip || req.connection.remoteAddress;
    if (isRateLimited(clientId)) {
      return res.status(429).json({ error: 'Too many requests. Please wait a minute.' });
    }

    const { name, budget, criteria, guests } = req.body;
    
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
        timeZone: 'Australia/Sydney',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
      }) + ' AEDT'
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
    
    await saveData();
    
    res.json({ 
      partyId, 
      guestUrls,
      party 
    });
  } catch (error) {
    console.error('Error creating party:', error);
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

app.listen(PORT, '0.0.0.0', async () => {
  console.log(`Secret Santa app running on http://0.0.0.0:${PORT}`);
  await loadData();
});
