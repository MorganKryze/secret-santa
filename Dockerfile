FROM node:18-alpine

WORKDIR /app

# Set timezone to Australia/Sydney
ENV TZ=Australia/Sydney
RUN apk add --no-cache tzdata

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install --production

# Copy application code
COPY . .

# Create public directory if it doesn't exist
RUN mkdir -p public

# Expose port
EXPOSE 8003

# Run the application
CMD ["npm", "start"]
