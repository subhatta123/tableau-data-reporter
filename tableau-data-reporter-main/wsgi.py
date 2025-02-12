import os
import subprocess
import sys

def app(environ, start_response):
    """WSGI application that starts Streamlit."""
    # Get the port from environment or use default
    port = int(os.environ.get('PORT', 8501))
    
    # Start Streamlit as a subprocess
    process = subprocess.Popen([
        sys.executable,
        "-m", "streamlit",
        "run",
        "tableau_streamlit_app.py",
        "--server.port", str(port),
        "--server.address", "0.0.0.0"
    ])
    
    # Return a simple response
    status = '200 OK'
    headers = [('Content-type', 'text/plain; charset=utf-8')]
    start_response(status, headers)
    
    return [b"Application started"]

# For local development
if __name__ == "__main__":
    from wsgiref.simple_server import make_server
    port = int(os.environ.get('PORT', 8501))
    httpd = make_server('', port, app)
    print(f"Serving on port {port}...")
    httpd.serve_forever() 