FROM node:10
ADD package.json /app/package.json
RUN cd /app && npm i
ADD . /app

