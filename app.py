from flask import Flask, send_file, jsonify
import csv
import os

app = Flask(__name__)

DATA_DIR = "/data"


@app.route("/")
def index():
    return jsonify(
        {
            "message": "Subscription Data API",
            "endpoints": {
                "/apfel/subscriptions": "Apfel platform subscription events (JSON)",
                "/fenster/subscriptions": "Fenster platform subscription events (CSV)",
                "/exchange-rates": "USD to EUR exchange rates (CSV)",
                "/health": "Health check",
            },
        }
    )


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


@app.route("/apfel/subscriptions")
def apfel_subscriptions():
    filepath = os.path.join(DATA_DIR, "apfel_subscriptions.csv")

    events = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert numeric fields
            if row.get("amount"):
                try:
                    row["amount"] = float(row["amount"])
                except (ValueError, TypeError):
                    pass
            if row.get("tax_amount"):
                try:
                    row["tax_amount"] = float(row["tax_amount"])
                except (ValueError, TypeError):
                    pass
            events.append(row)

    return jsonify({"events": events, "count": len(events)})


@app.route("/fenster/subscriptions")
def fenster_subscriptions():
    filepath = os.path.join(DATA_DIR, "fenster_subscriptions.csv")
    return send_file(filepath, mimetype="text/csv")


@app.route("/exchange-rates")
def exchange_rates():
    filepath = os.path.join(DATA_DIR, "exchange_rates.csv")
    return send_file(filepath, mimetype="text/csv")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
