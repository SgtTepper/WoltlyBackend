import logging
import sqlalchemy as db
import azure.functions as func
import json

term_keys = ["index", "id", "city", "title", "name", "description", "price", "image", "url"]
stat_keys = ["city", "min", "average", "max", "count"]
engine = db.create_engine("sqlite:///items.db")

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    term = req.params.get("term")
    city = req.params.get("city")
    search_description = req.params.get("searchdesc", "").lower() == "true"

    stat_clause = engine.execute(
        f"SELECT city, min(price), avg(price), max(price), count(price) FROM items WHERE name LIKE ? AND price > 0 GROUP BY city",
        ("%" + term + "%",),
    )

    term_clause = engine.execute(
        f"SELECT * FROM items WHERE name LIKE ? AND city = ? AND price > 0 LIMIT 500",
        ("%" + term + "%", city),
    )

    if search_description:
        term_clause = engine.execute(
            f"SELECT * FROM items WHERE (name LIKE ? OR description LIKE ?) AND city = ? AND price > 0 LIMIT 500",
            ("%" + term + "%", "%" + term + "%", city),
        )

        stat_clause = engine.execute(
            f"SELECT city, min(price), avg(price), max(price), count(price) FROM items WHERE name LIKE ? OR description LIKE ? AND price > 0 GROUP BY city",
            (
                "%" + term + "%",
                "%" + term + "%",
            ),
        )

    return func.HttpResponse(
        json.dumps(
            {
                "stats": [dict(zip(stat_keys, row)) for row in stat_clause.fetchall()],
                "data": [dict(zip(term_keys, row)) for row in term_clause.fetchall()],
            }
        ),
        status_code=200,
        mimetype="application/json",
    )
