# Basketball Data Scripts & API

Scripts to fetch and process basketball data from ESPN APIs, with a FastAPI wrapper for easy access.

## Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Edit `config.json` to change the output directory:

```json
{
  "output_dir": "./output",
  "schedules_script": "schedules.py",
  "scoreboard_script": "scoreboard.py"
}
```

## Running Scripts Directly

### Schedules Script

Generate records for women's and men's basketball:

```bash
python schedules.py [output_path]
```

Outputs:
- `wbb_records.json` - Women's basketball records
- `mbb_records.json` - Men's basketball records

### Scoreboard Script

Fetch current scoreboard data:

```bash
python scoreboard.py [output_path]
```

Outputs:
- `wbb_scoreboard.json` - Women's basketball scoreboard
- `mbb_scoreboard.json` - Men's basketball scoreboard

## Running the API

Start the FastAPI server:

```bash
python api.py
```

Or with uvicorn directly:

```bash
uvicorn api:app --reload
```

The API will be available at `http://localhost:8000`

### API Endpoints

#### `GET /`
Get API information and available endpoints.

#### `POST /schedules`
Run the schedules script to generate records for both women's and men's basketball.

**Example:**
```bash
curl -X POST http://localhost:8000/schedules
```

#### `POST /scoreboard`
Run the scoreboard script to fetch current game data.

**Example:**
```bash
curl -X POST http://localhost:8000/scoreboard
```

#### `GET /data/{sport}/{data_type}`
Retrieve generated JSON data.

**Parameters:**
- `sport`: `wbb` (women's) or `mbb` (men's)
- `data_type`: `records` or `scoreboard`

**Examples:**
```bash
# Get women's basketball records
curl http://localhost:8000/data/wbb/records

# Get men's basketball scoreboard
curl http://localhost:8000/data/mbb/scoreboard
```

#### `GET /health`
Health check endpoint to verify API status and output directory.

**Example:**
```bash
curl http://localhost:8000/health
```

### API Documentation

Interactive API documentation is available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Output Files

All generated files are stored in the directory specified in `config.json` (default: `./output/`):

- `wbb_records.json` - CAA women's basketball team records
- `mbb_records.json` - CAA men's basketball team records
- `wbb_scoreboard.json` - Current women's basketball games
- `mbb_scoreboard.json` - Current men's basketball games
