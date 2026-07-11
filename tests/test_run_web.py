from run_web import build_parser


def test_web_cli_defaults_to_loopback():
    args = build_parser().parse_args([])

    assert args.host == "127.0.0.1"
    assert args.port == 8000
