import os
import schedule
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging
from datetime import datetime
import io
import paramiko
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

# --- Configuration Loading & Logging Setup ---
load_dotenv() # Load environment variables from .env file

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Functions ---

def get_connection():
    """Establishes a database connection using the DB_URL from .env."""
    db_url = os.getenv("DB_URL")
    if not db_url:
        logging.error("DB_URL not found in .env file.")
        return None
    try:
        engine = create_engine(db_url)
        connection = engine.connect()
        logging.info("Database connection established successfully.")
        return connection
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        return None

def fetch_data(connection, query):
    """Fetches data from the database using the provided query."""
    if not connection:
        return None
    try:
        result = connection.execute(text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        logging.info(f"Successfully fetched {len(df)} rows.")
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return None

def format_filename(pattern):
    """Formats the filename based on the pattern and current timestamp."""
    now = datetime.now()
    # Handle timestamp formatting
    formatted_pattern = pattern
    while '{timestamp:' in formatted_pattern:
        start_index = formatted_pattern.find('{timestamp:') + len('{timestamp:')
        end_index = formatted_pattern.find('}', start_index)
        if end_index == -1:
            logging.warning("Invalid timestamp format in filename pattern. Missing '}'.")
            break # Avoid infinite loop
        format_code = formatted_pattern[start_index:end_index]
        try:
            timestamp_str = now.strftime(format_code)
            formatted_pattern = formatted_pattern.replace(f'{{timestamp:{format_code}}}', timestamp_str)
        except ValueError as e:
            logging.warning(f"Invalid strftime format code '{format_code}': {e}")
            # Replace with a default or remove the placeholder to avoid errors
            formatted_pattern = formatted_pattern.replace(f'{{timestamp:{format_code}}}', 'invalid_timestamp_format')

    # Handle extension placeholder (filled later based on format)
    return formatted_pattern

def create_pdf(df, include_header):
    """Creates a PDF document from the DataFrame."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter))
    elements = []
    styles = getSampleStyleSheet()

    if not df.empty:
        # Convert DataFrame to list of lists for ReportLab Table
        data = []
        if include_header:
            data.append(list(df.columns))
        data.extend(df.values.tolist())

        # Create Table object
        table = Table(data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey), # Header row background
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke), # Header text color
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), # Header font
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige), # Data rows background
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(table)
    else:
        elements.append(Paragraph("No data returned by the query.", styles['Normal']))

    doc.build(elements)
    buffer.seek(0)
    return buffer

def save_local(data, filename, path):
    """Saves data (string or bytes) to a local file."""
    os.makedirs(path, exist_ok=True) # Ensure the directory exists
    full_path = os.path.join(path, filename)
    mode = 'wb' if isinstance(data, io.BytesIO) else 'w'
    try:
        with open(full_path, mode) as f:
            if isinstance(data, io.BytesIO):
                f.write(data.getvalue())
            else:
                f.write(data)
        logging.info(f"File saved locally: {full_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to save file locally to {full_path}: {e}")
        return False

def upload_sftp(data, remote_filename, remote_path):
    """Uploads data (string or bytes) to an SFTP server."""
    host = os.getenv("SFTP_HOST")
    port = int(os.getenv("SFTP_PORT", 22))
    user = os.getenv("SFTP_USER")
    password = os.getenv("SFTP_PASSWORD")

    if not all([host, user, password, remote_path]):
        logging.error("Missing SFTP configuration (HOST, USER, PASSWORD, REMOTE_PATH).")
        return False

    transport = None
    sftp = None
    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=user, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        logging.info(f"Connected to SFTP server: {host}")

        remote_full_path = f"{remote_path.rstrip('/')}/{remote_filename}"

        # Create remote directory if it doesn't exist (optional, be careful with permissions)
        try:
            sftp.stat(remote_path)
        except FileNotFoundError:
            logging.info(f"Remote path '{remote_path}' not found, attempting to create.")
            try:
                 # Create intermediate directories if necessary (walk up the path)
                current_dir = ''
                for part in remote_path.strip('/').split('/'):
                    current_dir += '/' + part
                    try:
                        sftp.stat(current_dir)
                    except FileNotFoundError:
                        sftp.mkdir(current_dir)
                        logging.info(f"Created remote directory: {current_dir}")
            except Exception as mkdir_e:
                 logging.warning(f"Could not create remote directory {remote_path}: {mkdir_e}. Upload might fail.")


        file_obj = data if isinstance(data, io.BytesIO) else io.StringIO(data)
        if isinstance(file_obj, io.StringIO):
            # SFTP putfo expects bytes, encode string data
            file_bytes_obj = io.BytesIO(file_obj.getvalue().encode('utf-8'))
            sftp.putfo(file_bytes_obj, remote_full_path)
        else: # It's already BytesIO (from PDF)
            sftp.putfo(file_obj, remote_full_path)

        logging.info(f"File uploaded via SFTP to: {remote_full_path}")
        return True

    except paramiko.AuthenticationException:
        logging.error("SFTP authentication failed. Check credentials.")
        return False
    except Exception as e:
        logging.error(f"SFTP upload failed: {e}")
        return False
    finally:
        if sftp: sftp.close()
        if transport: transport.close()
        logging.info("SFTP connection closed.")

# --- Main Job Function ---

def export_job():
    """The main job function to be scheduled."""
    logging.info("Starting export job...")

    # --- Get Config ---
    sql_query = os.getenv("SQL_QUERY")
    output_format = os.getenv("OUTPUT_FORMAT", "csv").lower()
    include_header = os.getenv("INCLUDE_HEADER", "true").lower() == "true"
    filename_pattern = os.getenv("OUTPUT_FILENAME_PATTERN", "query_result_{timestamp:%Y%m%d_%H%M%S}.{ext}")
    delivery_method = os.getenv("DELIVERY_METHOD", "local").lower()
    local_output_path = os.getenv("LOCAL_OUTPUT_PATH", "./output")
    sftp_remote_path = os.getenv("SFTP_REMOTE_PATH")

    if not sql_query:
        logging.error("SQL_QUERY not found in .env file. Skipping job.")
        return

    # --- Database Operations ---
    conn = get_connection()
    if not conn:
        logging.error("Failed to get database connection. Skipping job.")
        return

    df = fetch_data(conn, sql_query)
    conn.close()
    logging.info("Database connection closed.")

    if df is None:
        logging.error("Failed to fetch data. Skipping file generation and delivery.")
        return
    if df.empty:
        logging.warning("Query returned no data. Proceeding to generate empty file.")

    # --- Format Data ---
    output_data = None
    file_extension = "txt" # Default

    if output_format == "csv":
        file_extension = "csv"
        output_data = df.to_csv(index=False, header=include_header)
    elif output_format == "pipe":
        file_extension = "txt"
        output_data = df.to_csv(index=False, header=include_header, sep='|')
    elif output_format == "pdf":
        file_extension = "pdf"
        output_data = create_pdf(df, include_header)
        if output_data is None:
             logging.error("Failed to generate PDF.")
             return # Stop if PDF generation fails
    else:
        logging.error(f"Invalid OUTPUT_FORMAT: {output_format}. Use 'csv', 'pdf', or 'pipe'.")
        return

    # --- Generate Filename ---
    filename_base = format_filename(filename_pattern)
    filename = filename_base.replace("{ext}", file_extension)

    # --- Deliver File ---
    success = False
    if delivery_method == "local":
        success = save_local(output_data, filename, local_output_path)
    elif delivery_method == "sftp":
        success = upload_sftp(output_data, filename, sftp_remote_path)
    else:
        logging.error(f"Invalid DELIVERY_METHOD: {delivery_method}. Use 'local' or 'sftp'.")

    if success:
        logging.info(f"Export job completed successfully. File: {filename}")
    else:
        logging.error("Export job failed during file delivery.")

# --- Scheduling ---

if __name__ == "__main__":
    interval_str = os.getenv("SCHEDULE_INTERVAL_MINUTES")
    try:
        interval_minutes = int(interval_str)
        if interval_minutes <= 0:
            raise ValueError("Interval must be positive")
        logging.info(f"Scheduling job to run every {interval_minutes} minutes.")
        # Run once immediately for testing/startup
        export_job()
        # Schedule future runs
        schedule.every(interval_minutes).minutes.do(export_job)
    except (TypeError, ValueError):
        logging.error(f"Invalid or missing SCHEDULE_INTERVAL_MINUTES: '{interval_str}'. Please provide a positive integer. Running job once now, but not scheduling.")
        export_job() # Run once even if scheduling fails
        interval_minutes = None # Ensure the loop below doesn't run if scheduling failed

    # Keep the script running if scheduled
    if interval_minutes:
        while True:
            schedule.run_pending()
            time.sleep(60) # Check every minute
