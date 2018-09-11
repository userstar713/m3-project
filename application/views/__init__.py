from flask import Flask
from .helpers import seller_integration_bp
from .routes import *

def init_app(app: Flask) -> None:
    """Initialize application
    :param app: app's object to initialize
    """
    app.register_blueprint(seller_integration_bp, url_prefix='/api/1/')
