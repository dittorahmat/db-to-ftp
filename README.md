# DB to FTP/Local Exporter

This script connects to a database, executes a specified SQL query, formats the results, and exports them to either a local directory or an SFTP server on a schedule.

## Features

*   Connects to various databases supported by SQLAlchemy.
*   Executes custom SQL queries.
*   Exports data in CSV, PDF, or Pipe-delimited format.
*   Configurable output filename patterns with timestamps.
*   Option to include or exclude headers in the output file.
*   Delivers files to a local folder or via SFTP.
*   Runs automatically on a configurable schedule.
*   Uses a `.env` file for secure configuration.

## Setup

1.  **Clone the repository (if applicable):**
    ```bash
    # git clone <repository_url>
    # cd <repository_directory>
    ```

2.  **Install dependencies:**
    Make sure you have Python 3 installed. Then install the required libraries:
    ```bash
    pip install -r requirements.txt
    ```
    *Note:* You might need to install a specific database driver depending on your database (e.g., `psycopg2-binary` for PostgreSQL). See comments in `requirements.txt`.

3.  **Configure Environment Variables:**
    Copy the example environment file:
    ```bash
    cp .env.example .env
    ```
    Edit the `.env` file with your specific configuration:
    *   `DB_URL`: Your database connection string (SQLAlchemy format).
    *   `SQL_QUERY`: The SQL query you want to execute.
    *   `SCHEDULE_INTERVAL_MINUTES`: How often (in minutes) the script should run.
    *   `OUTPUT_FORMAT`: Choose `csv`, `pdf`, or `pipe`.
    *   `INCLUDE_HEADER`: Set to `true` to include column headers, `false` otherwise.
    *   `OUTPUT_FILENAME_PATTERN`: Define the output filename. Use `{timestamp:<format>}` for timestamps (e.g., `{timestamp:%Y%m%d_%H%M%S}`) and `{ext}` for the file extension.
    *   `DELIVERY_METHOD`: Choose `local` or `sftp`.
    *   **If `DELIVERY_METHOD=local`:**
        *   `LOCAL_OUTPUT_PATH`: The path to the local directory for saving files.
    *   **If `DELIVERY_METHOD=sftp`:**
        *   `SFTP_HOST`, `SFTP_PORT`, `SFTP_USER`, `SFTP_PASSWORD`: Your SFTP server credentials.
        *   `SFTP_REMOTE_PATH`: The directory path on the SFTP server.

## Usage

Run the script from your terminal:

```bash
python main.py
```

The script will perform an initial run and then continue running in the background, executing the export job according to the schedule defined in your `.env` file. Logs will be printed to the console.

To stop the script, press `Ctrl+C`.
