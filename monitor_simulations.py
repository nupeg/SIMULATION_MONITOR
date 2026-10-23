
from pathlib import Path
from datetime import datetime
import logging
import os
import re

import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

ROOT_DIR = Path(
    r"C:\Users\labsi\Documents\Resultados_densidade_ternaria\Tabela_resultados_ternaria\Pastas modelos"
)

# Store monitoring outputs outside the simulation directories.
OUTPUT_DIR = Path.home() / "Documents" / "Simulation_monitor"

OUTPUT_FILE = OUTPUT_DIR / "simulation_parameters.xlsx"

LOG_DIR = OUTPUT_DIR / "logs"
LOG_FILE = LOG_DIR / "monitor_simulations.log"

SIMULATION_FILE = "simulation.RUN"
SHEET_NAME = "Simulations"


# Only folder names matching this complete pattern are processed.
NUMBER_PATTERN = (
    r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?"
)

FOLDER_PATTERN = re.compile(
    rf"^S1_(?P<Salt1>.+?)"
    rf"_S2_(?P<Salt2>.+?)"
    rf"_M_(?P<Total_molality>{NUMBER_PATTERN})"
    rf"_X1_(?P<Salt1_molality>{NUMBER_PATTERN})"
    rf"_X2_(?P<Salt2_molality>{NUMBER_PATTERN})"
    rf"_T_(?P<Temperature_K>{NUMBER_PATTERN})"
    rf"_P_(?P<Pressure_atm>{NUMBER_PATTERN})$"
)

SUCCESS_DATETIME_PATTERN = re.compile(
    r"(?P<day>\d{2})_"
    r"(?P<month>\d{2})_"
    r"(?P<year>\d{4})_"
    r"(?P<hour>\d{1,2})h_"
    r"(?P<minute>\d{1,2})min_"
    r"(?P<second>\d{1,2})s"
)

NUMERIC_COLUMNS = [
    "Total_molality",
    "Salt1_molality",
    "Salt2_molality",
    "Temperature_K",
    "Pressure_atm",
]

OUTPUT_COLUMNS = [
    "Salt1",
    "Salt2",
    "Total_molality",
    "Salt1_molality",
    "Salt2_molality",
    "Temperature_K",
    "Pressure_atm",
    "Status",
    "Success_datetime",
    "Mean_density",
]


# ============================================================
# LOGGING
# ============================================================

def configure_logging():
    """Configure logging to both a file and the console."""

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )


# ============================================================
# FOLDER NAME PARSING
# ============================================================

def extract_parameters(folder_name):
    """
    Extract simulation parameters only when the complete
    folder name matches the predefined naming convention.
    """

    match = FOLDER_PATTERN.fullmatch(folder_name)

    if match is None:
        return None

    parameters = match.groupdict()

    for column in NUMERIC_COLUMNS:
        parameters[column] = float(parameters[column])

    return parameters


# ============================================================
# SUCCESS DATE AND TIME
# ============================================================

def extract_success_datetime(success_file):
    """
    Extract the completion date and time from the filename.

    Expected example:
    status_success_sim14_13_10_2026_22h_26min_13s.txt
    """

    match = SUCCESS_DATETIME_PATTERN.search(success_file.name)

    if match is None:
        logging.warning(
            "Could not extract completion date/time from: %s",
            success_file.name,
        )
        return None

    values = {
        key: int(value)
        for key, value in match.groupdict().items()
    }

    try:
        return datetime(
            values["year"],
            values["month"],
            values["day"],
            values["hour"],
            values["minute"],
            values["second"],
        )

    except ValueError:
        logging.exception(
            "Invalid completion date/time in: %s",
            success_file.name,
        )
        return None


# ============================================================
# TEMPORARY DENSITY CALCULATION METHOD
# ============================================================

def calculate_mean_density(simulation_folder):
    """
    TEMPORARY CALCULATION METHOD.

    Calculate the arithmetic mean of every value in the
    'density' column of simulation.RUN.

    All density values are included, from the first data row
    to the last data row.

    Replace this function if the density calculation method
    changes in the future.
    """

    density_file = Path(simulation_folder) / SIMULATION_FILE

    if not density_file.is_file():
        raise FileNotFoundError(
            f"Density file not found: {density_file}"
        )

    density_sum = 0.0
    density_count = 0

    # Read line by line to avoid loading the entire file into memory.
    with density_file.open("r", encoding="utf-8") as file:

        header = file.readline().split()

        if "density" not in header:
            raise ValueError(
                f"'density' column not found in {density_file}"
            )

        density_index = header.index("density")

        # TEMPORARY METHOD: include every density value.
        for line_number, line in enumerate(file, start=2):

            values = line.split()

            if not values:
                continue

            try:
                density = float(values[density_index])

            except (ValueError, IndexError) as error:
                raise ValueError(
                    f"Invalid density value at line "
                    f"{line_number} in {density_file}"
                ) from error

            density_sum += density
            density_count += 1

    if density_count == 0:
        raise ValueError(
            f"No density values found in {density_file}"
        )

    return density_sum / density_count


# ============================================================
# SIMULATION STATUS
# ============================================================

def inspect_simulation(simulation_folder, parameters):
    """
    Determine simulation status and calculate density when
    a success file exists.

    Priority:
    1. status_success -> Success
    2. status_error   -> Incomplete
    3. Neither        -> Not simulated
    """

    success_files = sorted(
        simulation_folder.glob("status_success*")
    )

    error_files = list(
        simulation_folder.glob("status_error*")
    )

    result = parameters.copy()
    result["Status"] = None
    result["Success_datetime"] = None
    result["Mean_density"] = None

    # Success takes priority even when an error file also exists.
    if success_files:

        result["Status"] = "Success"

        # If multiple success files exist, use the latest
        # date/time encoded in their filenames when possible.
        dated_files = []

        for success_file in success_files:
            success_datetime = extract_success_datetime(
                success_file
            )

            if success_datetime is not None:
                dated_files.append(
                    (success_datetime, success_file)
                )

        if dated_files:
            success_datetime, selected_file = max(
                dated_files,
                key=lambda item: item[0],
            )
            result["Success_datetime"] = success_datetime

        else:
            selected_file = max(
                success_files,
                key=lambda file: file.stat().st_mtime,
            )

        # TEMPORARY DENSITY CALCULATION:
        # Only calculate density for successful simulations.
        try:
            result["Mean_density"] = calculate_mean_density(
                simulation_folder
            )

        except Exception:
            logging.exception(
                "Density calculation failed for: %s",
                simulation_folder,
            )

        return result

    if error_files:
        result["Status"] = "Incomplete"
        return result

    result["Status"] = "Not simulated"
    return result


# ============================================================
# FIND SIMULATION FOLDERS
# ============================================================

def find_simulation_folders(root_dir):
    """
    Find folders whose names exactly match FOLDER_PATTERN.

    The logs directory is explicitly excluded.
    Other folders are ignored unless their names match
    the predefined simulation naming convention.
    """

    for current_path, directories, _ in os.walk(root_dir):

        current_folder = Path(current_path)

        # Never scan the logs directory.
        directories[:] = [
            directory
            for directory in directories
            if directory.lower() != "logs"
        ]

        # Do not process the root directory itself.
        if current_folder == root_dir:
            continue

        parameters = extract_parameters(current_folder.name)

        if parameters is not None:
            yield current_folder, parameters

            # Simulation folders are treated as individual units.
            # Their internal subdirectories are not scanned.
            directories[:] = []


# ============================================================
# EXCEL OUTPUT
# ============================================================

def save_results(results, invalid_folders):
    """Overwrite the existing workbook with the latest results."""

    dataframe = pd.DataFrame(
        results,
        columns=OUTPUT_COLUMNS,
    )

    summary = pd.DataFrame(
        [
            {
                "Metric": "Total simulations",
                "Value": len(dataframe),
            },
            {
                "Metric": "Successful simulations",
                "Value": (
                    dataframe["Status"].eq("Success").sum()
                    if not dataframe.empty else 0
                ),
            },
            {
                "Metric": "Incomplete simulations",
                "Value": (
                    dataframe["Status"].eq("Incomplete").sum()
                    if not dataframe.empty else 0
                ),
            },
            {
                "Metric": "Not simulated",
                "Value": (
                    dataframe["Status"].eq("Not simulated").sum()
                    if not dataframe.empty else 0
                ),
            },
            {
                "Metric": "Folders with invalid simulation names",
                "Value": len(invalid_folders),
            },
        ]
    )

    with pd.ExcelWriter(
        OUTPUT_FILE,
        engine="openpyxl",
    ) as writer:

        dataframe.to_excel(
            writer,
            sheet_name=SHEET_NAME,
            index=False,
        )

        summary.to_excel(
            writer,
            sheet_name="Summary",
            index=False,
        )

        if invalid_folders:
            pd.DataFrame(
                {"Invalid_folder": invalid_folders}
            ).to_excel(
                writer,
                sheet_name="Invalid_folders",
                index=False,
            )

        # Format worksheet columns for easier reading.
        for worksheet in writer.sheets.values():
            worksheet.freeze_panes = "A2"
            worksheet.auto_filter.ref = worksheet.dimensions

            for column_cells in worksheet.columns:
                max_length = max(
                    len(str(cell.value))
                    if cell.value is not None else 0
                    for cell in column_cells
                )

                column_letter = column_cells[0].column_letter

                worksheet.column_dimensions[
                    column_letter
                ].width = min(max_length + 2, 35)


# ============================================================
# MAIN PROGRAM
# ============================================================

def main():

    if not ROOT_DIR.is_dir():
        raise FileNotFoundError(
            f"Simulation root directory not found: {ROOT_DIR}"
        )

    configure_logging()

    logging.info("Starting simulation monitoring.")
    logging.info("Root directory: %s", ROOT_DIR)

    results = []
    invalid_folders = []
    processed_count = 0

    for simulation_folder, parameters in find_simulation_folders(
        ROOT_DIR
    ):

        processed_count += 1

        try:
            result = inspect_simulation(
                simulation_folder,
                parameters,
            )

            results.append(result)

            logging.info(
                "Processed: %s | Status: %s",
                simulation_folder.name,
                result["Status"],
            )

        except Exception:
            logging.exception(
                "Failed to process simulation folder: %s",
                simulation_folder,
            )

    # Record folders that look like simulation folders but do
    # not match the complete naming convention.
    for current_path, directories, _ in os.walk(ROOT_DIR):

        directories[:] = [
            directory
            for directory in directories
            if directory.lower() != "logs"
        ]

        for directory in directories:
            if directory.startswith("S1_"):
                if extract_parameters(directory) is None:
                    invalid_folders.append(directory)

    save_results(results, invalid_folders)

    logging.info("Monitoring completed.")
    logging.info("Simulations processed: %s", processed_count)
    logging.info("Excel file updated: %s", OUTPUT_FILE)
    logging.info("Log file: %s", LOG_FILE)


if __name__ == "__main__":
    main()
