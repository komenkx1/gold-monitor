FROM node:20-alpine

WORKDIR /app

COPY --chown=node:node package.json ./
RUN npm install --omit=dev && npm cache clean --force

COPY --chown=node:node bot.js ./
RUN chown -R node:node /app

USER node

CMD ["npm", "start"]
