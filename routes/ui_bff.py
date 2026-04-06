"""
UI BFF routes.
"""

from aiohttp import web
from aiohttp_cors import CorsConfig

from handlers.ui_bff import UIBffHandler


def setup_ui_bff_routes(app: web.Application, cors: CorsConfig = None):
    handler = UIBffHandler()
    handler._app = app
    methods = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"]
    routes = []
    for method in methods:
        route = app.router.add_route(method, "/api/v1/ui/{tail:.*}", handler.proxy)
        routes.append(route)
    if cors:
        for route in routes:
            cors.add(route)
