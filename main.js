#!/usr/bin/env node

var crawler = null;

process.on("SIGINT", async () => {
  console.log("SIGINT received, exiting");
  if (crawler) {
    try {
      if (!crawler.crawlState.drain) {
        crawler.cluster.allTargetCount -= (await crawler.crawlState.size());// - crawler.cluster.workersBusy.length;
        crawler.crawlState.drain = true;
      } else {
        console.log(crawler.crawlState.serialize());
        process.exit(1);
      }
    } catch (e) {
      console.log(e);
    }
  }
});

process.on("SIGTERM", () => {
  console.log("SIGTERM received, exiting");
});



const { Crawler } = require("./crawler");

crawler = new Crawler();
crawler.run();


