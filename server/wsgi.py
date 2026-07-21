"""WSGI entry point for the cloud API service."""

from server import create_app


app = create_app()
