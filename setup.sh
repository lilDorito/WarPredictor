#!/bin/bash

mkdir -p datasets/{alarms,isw,telegram,reddit,weather}
mkdir -p logs/{alarms,isw,telegram,reddit,weather}
cp .env.example .env
echo "Done. Fill in your .env file."