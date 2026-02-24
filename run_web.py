"""Start the Druck ETF web server.

Usage:
    python run_web.py              # default: 0.0.0.0:8000
    python run_web.py --port 9000  # custom port
"""
import argparse
import uvicorn

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Druck ETF Web Server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    uvicorn.run("druck.web.app:app", host=args.host, port=args.port, reload=False)
