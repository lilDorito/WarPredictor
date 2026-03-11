#!/bin/bash

mkdir -p datasets/{alarms,isw,telegram,reddit,weather}
mkdir -p logs/{alarms,isw,telegram,reddit,weather}
mkdir -p notebooks/{}
mkdir -p scratch/{}
cp .env.example .env
echo "Done. Fill in your .env file."