# Build stage - install dependencies
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install only production dependencies (use npm install since no package-lock.json)
RUN npm install --omit=dev --ignore-scripts && \
    npm cache clean --force

# Production stage - minimal runtime image
FROM node:20-alpine AS production

# Build arguments with defaults
ARG TZ=Europe/Paris
ARG PUID=1000
ARG PGID=1000
ARG PORT=8003

# Set timezone
ENV TZ=${TZ} \
    NODE_ENV=production

RUN apk add --no-cache tzdata && \
    # Remove apk cache
    rm -rf /var/cache/apk/* && \
    # Note: node:alpine already has node user (uid=1000) and node group (gid=1000)
    # Only create custom user/group if different from defaults
    if [ "${PGID}" != "1000" ]; then addgroup -g ${PGID} -S appgroup; fi && \
    if [ "${PUID}" != "1000" ]; then adduser -S appuser -u ${PUID} -G $(getent group ${PGID} | cut -d: -f1); fi

WORKDIR /app

# Copy dependencies from builder
COPY --from=builder --chown=root:root /app/node_modules ./node_modules

# Copy only necessary application files
COPY --chown=root:root ../package*.json ./
COPY --chown=root:root ../server.js ./
COPY --chown=root:root ../public ./public

# Create data directory with correct permissions
RUN mkdir -p data && \
    chmod 755 /app && \
    chmod 1777 /app/data

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD node -e "require('http').get('http://localhost:${PORT}/', (r) => {process.exit(r.statusCode === 200 ? 0 : 1)})"

# Note: USER directive intentionally omitted - set at runtime via compose user: directive
# This allows flexibility to run as any UID/GID specified in environment variables

# Expose port
EXPOSE ${PORT}

# Run the application
CMD ["node", "server.js"]
