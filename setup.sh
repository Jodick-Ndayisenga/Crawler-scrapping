#!/bin/bash

# Create a Python virtual environment
python -m venv .venv

# Activate the virtual environment
source .venv/Scripts/activate

# Install requirements if requirements.txt exists
if [ -f "requirements.txt" ]; then
  pip install -r requirements.txt
else
  echo "requirements.txt not found. Creating an empty one."
  touch requirements.txt
fi

echo "Virtual environment setup complete and activated. Requirements installed." 

# Install playwright
playwright install

echo "Playwright installed."