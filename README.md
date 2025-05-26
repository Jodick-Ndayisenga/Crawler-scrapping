# Google Maps Scraper

A Python-based scraper for Google Maps using Playwright and Crawlee.

## Prerequisites

- Python 3.x
- Git
- Bash (for running shell scripts)

## Setup Instructions

1. Clone the repository:
```bash
git clone <repository-url>
cd supernova-google-maps-scrapper
```

2. Run the setup script to create a virtual environment and install dependencies:
```bash
chmod +x setup.sh
./setup.sh
```

3. Create a `.env` file in the root directory with the following variables:
```env
# Required
TASK_SPREADER_API_URL=<your-api-url>

# Optional (defaults shown)
COUNTRY=usa
MACHINE_ID=None
FETCHER_MIN_CONCURRENCY=5
```

### Environment Variables Explanation

- `TASK_SPREADER_API_URL`: (Required) The URL of your task spreader API endpoint
- `COUNTRY`: (Optional) The country code for queries. Defaults to "usa"
- `MACHINE_ID`: (Optional) The name of the machine. Defaults to None
- `FETCHER_MIN_CONCURRENCY`: (Optional) The minimum number of concurrent requests. Defaults to 5
## Running the Scraper

To run the fetcher script:
```bash
chmod +x run-fetcher.sh
./run-fetcher.sh
```

## Project Structure

- `fetcher.py`: Main script for fetching Google Maps data
- `utils/`: Utility functions and helpers
- `storage/`: Directory for storing temporary data (gitignored)
- `queries_cache.json`: Cache file for queries (gitignored)

## Dependencies

The project uses the following main dependencies:
- crawlee==0.6.5
- playwright==1.51.0
- python-dotenv==1.1.0
- requests==2.32.3

All dependencies are automatically installed during setup.
