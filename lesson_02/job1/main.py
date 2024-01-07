import os
from flask import Flask, request, Response

from lesson_02.job1.bll.services.get_sales_job import SalesJob

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])


def fetch_sales_data():
    if request.method == "POST":
        try:
            request_data = request.get_json()
            raw_dir: str = request_data["raw_dir"]
            date: str = request_data["date"]
            SalesJob().get_sales_job(raw_dir, date)
            return "Success", 200
        except:
            return "Server error", 500
    else:
        return 404


if __name__ == "__main__":
    app.run(debug=True, port=5000)
