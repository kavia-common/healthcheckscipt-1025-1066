from flask_smorest import Blueprint
from flask.views import MethodView

blp = Blueprint("Healt Check", "health check", url_prefix="/", description="Health check route")

@blp.route("/")
class HealthCheck(MethodView):
    # PUBLIC_INTERFACE
    def get(self):
        """
        Basic liveness endpoint for the service.

        Returns:
          JSON: { "message": "Healthy" }
        """
        return {"message": "Healthy"}
