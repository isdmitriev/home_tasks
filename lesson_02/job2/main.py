import os
from flask import Flask, request, Response

from lesson_02.job2.restore_sales_job import RestoreSalesJob

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])


def restore_sales_data():
    if request.method == "POST":
        request_data = request.get_json()
        raw_dir: str = request_data["raw_dir"]
        stg_dir: str = request_data["stg_dir"]
        RestoreSalesJob().restore_sales(raw_dir, stg_dir)

        return Response(status=200)
    else:
        return Response(status=200)


if __name__ == "__main__":
    app.run(debug=True, port=5001)
