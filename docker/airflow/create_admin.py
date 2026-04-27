import os
from airflow.www.app import create_app

app = create_app()
with app.app_context():
    sm = app.appbuilder.sm
    username = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
    if not sm.find_user(username=username):
        role = sm.find_role("Admin")
        sm.add_user(
            username=username,
            first_name="Admin",
            last_name="User",
            email=os.environ.get("AIRFLOW_ADMIN_EMAIL", "admin@example.com"),
            role=role,
            password=os.environ.get("AIRFLOW_ADMIN_PASSWORD", "admin"),
        )
        print(f"Admin user '{username}' created.")
    else:
        print(f"Admin user '{username}' already exists.")
