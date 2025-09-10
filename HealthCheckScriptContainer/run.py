from app import app

if __name__ == "__main__":
    # Start Flask app exposing system endpoints for integration/scheduling triggers
    app.run(host="0.0.0.0", port=8080)
