from setuptools import setup


if __name__ == "__main__":
    # Minimal passthrough setup.py to satisfy legacy tooling that runs
    # `python setup.py sdist` or similar. The actual build backend is
    # declared in `pyproject.toml`.
    setup()
