"""Entry point: ``python -m vortis`` starts the RESP server."""
from vortis.async_tcp import serve


def main() -> None:  # pragma: no cover - process entrypoint / real socket IO
    serve()


if __name__ == "__main__":  # pragma: no cover - process entrypoint
    main()
