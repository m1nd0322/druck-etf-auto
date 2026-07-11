"""Start the Druck ETF web server.

Usage:
    python run_web.py              # default: 127.0.0.1:8000
    python run_web.py --host 0.0.0.0
    python run_web.py --port 9000  # custom port
"""
import argparse
import uvicorn


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Druck ETF Web Server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    uvicorn.run("druck.web.app:app", host=args.host, port=args.port, reload=False)


if __name__ == "__main__":
    main()
