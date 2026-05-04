"""
Deploy de VOLLEDIGE Streamlit app op Modal.

Deploy:  python -m modal deploy modal_streamlit_full.py
URL:     https://chef--seokitchen-streamlit-run.modal.run

De app is daarna embedded als iframe in het SEO Kitchen dashboard.
Project context wordt meegegeven via ?project_id=... in de URL.
"""

import modal

app = modal.App("seokitchen-streamlit")

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "streamlit==1.45.0",
        "supabase==2.15.1",
        "anthropic==0.49.0",
        "pandas==2.2.3",
        "openpyxl==3.1.5",
        "Pillow==11.2.1",
        "python-dotenv==1.1.0",
        "psycopg2-binary==2.9.10",
        "watchdog==6.0.0",
    )
    .add_local_dir(".", remote_path="/app")
)

secret = modal.Secret.from_name("seokitchen-shopify")


@app.function(
    image=image,
    secrets=[secret],
    min_containers=1,       # altijd warm houden — geen cold start
    timeout=3600,           # 1 uur max per sessie
)
@modal.web_server(8501)
def run():
    import subprocess
    subprocess.Popen([
        "streamlit", "run", "/app/tool/app.py",
        "--server.port", "8501",
        "--server.address", "0.0.0.0",
        "--server.headless", "true",
        "--server.enableXsrfProtection", "false",
        "--server.enableCORS", "false",
        "--browser.gatherUsageStats", "false",
    ])
