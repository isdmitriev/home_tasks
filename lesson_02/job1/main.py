import os
from flask import Flask, request, Response

from lesson_02.job1.bll.services.get_sales_job import SalesJob

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
async def fetch_sales_data():
    if request.method == "POST":
        request_data = request.get_json()
        raw_dir: str = request_data["raw_dir"]
        date: str = request_data["date"]
        SalesJob().get_sales_job(raw_dir, date)

        return Response(status=200)
    else:
        return Response(status=200)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
