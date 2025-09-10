"""Routes package initializer for HealthCheckScriptContainer.

Exports the available blueprints for import in the Flask app.
"""
# PUBLIC_INTERFACE
def __all__():
    """This package exposes route modules for blueprints."""
    return ["health", "system"]
