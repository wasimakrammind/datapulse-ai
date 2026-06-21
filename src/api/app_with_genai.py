"""FastAPI app with GenAI routes registered."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.api.main import app
from src.genai.root_cause_agent import register_genai_routes

register_genai_routes(app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
