from flask import Flask
from flask_cors import CORS
from flask_smorest import Api
from .routes.health import blp as health_blp
from .routes.system import blp as system_blp

# Initialize Flask app and API docs
app = Flask(__name__)
app.url_map.strict_slashes = False
CORS(app, resources={r"/*": {"origins": "*"}})

app.config["API_TITLE"] = "DU Health Check Service"
app.config["API_VERSION"] = "v1"
app.config["OPENAPI_VERSION"] = "3.0.3"
app.config["OPENAPI_URL_PREFIX"] = "/docs"
app.config["OPENAPI_SWAGGER_UI_PATH"] = ""
app.config["OPENAPI_SWAGGER_UI_URL"] = "https://cdn.jsdelivr.net/npm/swagger-ui-dist/"

openapi_tags = [
    {"name": "Healt Check", "description": "Health check route"},
    {"name": "System", "description": "System integration endpoints for DU health checks"},
]
app.config["API_SPEC_OPTIONS"] = {"tags": openapi_tags}

api = Api(app)
api.register_blueprint(health_blp)
api.register_blueprint(system_blp)
