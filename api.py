import json
import subprocess
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI(title="Basketball Data API", version="1.0.0")

# Load configuration
config_path = Path(__file__).parent / "config.json"
with open(config_path) as f:
    config = json.load(f)

OUTPUT_DIR = Path(config["output_dir"])
SCHEDULES_SCRIPT = config["schedules_script"]
SCOREBOARD_SCRIPT = config["scoreboard_script"]


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Basketball Data API",
        "endpoints": {
            "/schedules": "Run schedules script to generate records",
            "/scoreboard": "Run scoreboard script to fetch current games",
            "/data/{sport}/{data_type}": "Get generated JSON data",
        },
    }


@app.post("/schedules")
async def run_schedules():
    """
    Run the schedules.py script to generate WBB and MBB records.

    Returns status and output file paths.
    """
    try:
        # Ensure output directory exists
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # Run the schedules script
        result = subprocess.run(
            ["python", SCHEDULES_SCRIPT, str(OUTPUT_DIR)],
            capture_output=True,
            text=True,
            check=True,
        )

        return {
            "status": "success",
            "message": "Schedules processed successfully",
            "output": result.stdout,
            "files": [
                str(OUTPUT_DIR / "wbb_records.json"),
                str(OUTPUT_DIR / "mbb_records.json"),
            ],
        }
    except subprocess.CalledProcessError as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Script execution failed",
                "stdout": e.stdout,
                "stderr": e.stderr,
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/scoreboard")
async def run_scoreboard():
    """
    Run the scoreboard.py script to fetch current WBB and MBB games.

    Returns status and output file paths.
    """
    try:
        # Ensure output directory exists
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # Run the scoreboard script
        result = subprocess.run(
            ["python", SCOREBOARD_SCRIPT, str(OUTPUT_DIR)],
            capture_output=True,
            text=True,
            check=True,
        )

        return {
            "status": "success",
            "message": "Scoreboard data fetched successfully",
            "output": result.stdout,
            "files": [
                str(OUTPUT_DIR / "wbb_scoreboard.json"),
                str(OUTPUT_DIR / "mbb_scoreboard.json"),
            ],
        }
    except subprocess.CalledProcessError as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Script execution failed",
                "stdout": e.stdout,
                "stderr": e.stderr,
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/{sport}/{data_type}")
async def get_data(
    sport: Literal["wbb", "mbb"],
    data_type: Literal["records", "scoreboard"],
):
    """
    Get the generated JSON data for a specific sport and data type.

    Args:
        sport: Either 'wbb' (women's) or 'mbb' (men's)
        data_type: Either 'records' or 'scoreboard'

    Returns the JSON data from the generated files.
    """
    filename = OUTPUT_DIR / f"{sport}_{data_type}.json"

    if not filename.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Data file not found: {filename}. Run the corresponding endpoint first.",
        )

    try:
        with open(filename) as f:
            data = json.load(f)
        return JSONResponse(content=data)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=500,
            detail="Invalid JSON data in file",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "output_dir": str(OUTPUT_DIR),
        "output_dir_exists": OUTPUT_DIR.exists(),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
