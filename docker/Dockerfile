FROM node:20-alpine

# Build arguments with defaults
ARG TZ=Europe/Paris
ARG PUID=1000
ARG PGID=1000
ARG PORT=8003

# Set timezone
ENV TZ=${TZ}
RUN apk add --no-cache tzdata

# Create non-root user
RUN addgroup -g ${PGID} -S nodejs && \
    adduser -S nodejs -u ${PUID}

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install --production && \
    npm cache clean --force

# Copy application code
COPY ../ .

# Create data directory with correct permissions for any non-root user
RUN mkdir -p data public && \
    chmod -R 755 /app && \
    chmod -R 777 /app/data

# Note: USER directive intentionally omitted - set at runtime via compose user: directive
# This allows flexibility to run as any UID/GID specified in environment variables

# Expose port
EXPOSE ${PORT}

# Run the application
CMD ["node", "server.js"]
