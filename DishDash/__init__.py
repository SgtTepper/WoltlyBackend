import logging
import sqlalchemy as db
import azure.functions as func
import json

term_keys = ["index", "city", "title", "name", "description", "price", "image", "url"]
stat_keys = ["city", "average"]
engine = db.create_engine("sqlite:///DishDash/items.db")


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    term = req.params.get("term")
    city = req.params.get("city")
    search_description = req.params.get("searchdesc", "").lower() == "true"

    stat_clause = engine.execute(
        f"SELECT city, avg(price) FROM items WHERE name LIKE ? GROUP BY city",
        ("%" + term + "%",),
    )

    term_clause = engine.execute(
        f"SELECT * FROM items WHERE name LIKE ? AND city = ? LIMIT 500",
        ("%" + term + "%", city),
    )

    if search_description:
        term_clause = engine.execute(
            f"SELECT * FROM items WHERE (name LIKE ? OR description LIKE ?) AND city = ? LIMIT 500",
            ("%" + term + "%", "%" + term + "%", city),
        )

        stat_clause = engine.execute(
            f"SELECT city, avg(price) FROM items WHERE name LIKE ? GROUP BY city",
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
