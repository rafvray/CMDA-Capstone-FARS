from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
from sql_query_chain import ask_fars_database
import pandas as pd

def create_app():
    app = Flask(__name__)
    CORS(app)

    @app.route("/health", methods=["GET"])
    def health():
        return {"status": "ok", "backend": "databricks-sql"}, 200

    @app.route("/query", methods=["POST"])
    def query():
        try:
            payload = request.get_json(force=True)
            question = payload.get("query") or payload.get("question")

            if not question:
                return jsonify({"error": "Missing 'query' field"}), 400

            try:
                # Run your SQL chain
                df = ask_fars_database(question)

                # Convert results to frontend friendly format
                json_output = {
                    "query": question,
                    "columns": list(df.columns),
                    "rows": df.fillna("").astype(str).values.tolist()
                }

                return jsonify(json_output), 200

            except RuntimeError as e:
                # Catch LLM or SQL execution errors
                return jsonify({"error": str(e)}), 500

        except Exception as e:
            logging.exception("Unexpected server error")
            return jsonify({"error": "Internal server error"}), 500

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app = create_app()
    # disable reloader to avoid double LLM initialization and high memory usage
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)