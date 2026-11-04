
from pathlib import Path
from datetime import datetime
from collections import deque
import json
import logging
import os
import re
import time

import numpy as np
import pandas as pd
import gspread
from prefect import flow, task


# ============================================================
# CONFIGURATION
# ============================================================

# Simulations live here. This script only ever READS inside ROOT_DIR
# (simulation.RUN, status_* files, log_*.lammps) — it never creates,
# writes to, or deletes anything in this tree. Keep it that way: any
# future change that opens a file under ROOT_DIR must use mode "r".
ROOT_DIR = Path(r"G:\Modelos simulações")

# All of this script's own output (Excel, Google credentials, logs)
# lives here instead — well outside ROOT_DIR, so nothing it writes
# can ever collide with simulation files.
OUTPUT_DIR = Path.home() / "Documents" / "Simulation_monitor"

OUTPUT_FILE = OUTPUT_DIR / "simulation_parameters.xlsx"

# Google Sheets output: mirrors the same results so they can be read
# from anywhere (e.g. by Power BI) without opening the Excel file.
# The Excel file above is still written every run, untouched, as a
# local backup. One-time setup needed (see update_google_sheet below):
# 1. Create a Google Cloud service account, enable the Google Sheets
#    API for it, and download its JSON key to the path below.
# 2. Create a Google Sheet, share it ("Editor") with that service
#    account's "client_email" (inside the JSON key), and paste the
#    sheet's ID (the long string in its URL, between /d/ and /edit)
#    into GOOGLE_SHEET_ID below.
GOOGLE_SERVICE_ACCOUNT_FILE = OUTPUT_DIR / "google_service_account.json"
GOOGLE_SHEET_ID = "COLOQUE_AQUI_O_ID_DA_PLANILHA_GOOGLE"

# Cache of results already computed for completed ("Success")
# simulations, so a finished simulation's density/log data is only
# read and recomputed once, ever - not re-read from scratch every
# single day for the rest of the months-long project. See
# load_results_cache / inspect_simulation.
RESULTS_CACHE_FILE = OUTPUT_DIR / "results_cache.json"

# Folder count from the last trusted scan, used by the guard below.
MONITOR_STATE_FILE = OUTPUT_DIR / "monitor_state.json"

# Safety guard: if a scan finds fewer than this fraction of the
# folder count from the last trusted run, treat it as a likely
# incomplete scan (e.g. the network drive had a problem) rather than
# simulations having genuinely disappeared, and abort without
# touching the Excel file or Google Sheet. Lower this if legitimate
# folder deletions are expected to be common; 0 disables the guard.
MIN_FOLDER_COUNT_RATIO = 0.9

# Lock file preventing two runs from overlapping (and both writing
# to the same Excel file / Google Sheet / cache at once). If a lock
# file is found older than this, it's assumed to be left over from a
# run that crashed without cleaning up, and is overridden rather
# than blocking every future run forever.
LOCK_FILE = OUTPUT_DIR / "monitor.lock"
LOCK_STALE_AFTER_HOURS = 6

LOG_DIR = OUTPUT_DIR / "logs"
LOG_FILE = LOG_DIR / "monitor_simulations.log"

SIMULATION_FILE = "simulation.RUN"
SHEET_NAME = "Simulations"

# LAMMPS log file created when a simulation finishes, e.g.
# "log_gpu_GPU-2_sim14.lammps" or "log_omp_CPU-2_sim12.lammps".
LOG_FILE_GLOB = "log_*.lammps"

# Matches the compute device encoded in the log file name, e.g.
# "GPU-2" or "CPU-2" (case-insensitive: also matches "gpu-2", "Cpu-2").
COMPUTE_DEVICE_PATTERN = re.compile(r"(GPU|CPU)-\d+", re.IGNORECASE)

# Prefect task retries for save_results: the most common failure is
# OUTPUT_FILE being open in Excel. Retrying a few times, spaced out,
# gives time to close it without having to rerun the script by hand.
SAVE_RESULTS_RETRIES = 5
SAVE_RESULTS_RETRY_DELAY_S = 600  # 10 minutes between attempts

# Matches the last line of a LAMMPS log file, e.g.
# "Total wall time: 1:28:18" (hours can have any number of digits).
WALL_TIME_PATTERN = re.compile(
    r"^Total wall time:\s*(?P<hours>\d+):"
    r"(?P<minutes>\d{2}):(?P<seconds>\d{2})\s*$"
)

# Bootstrap density calculation.
# Only the last BOOTSTRAP_WINDOW_SIZE lines of simulation.RUN are used
# (500,000 lines = 1 ns of simulation at a 2 fs output interval).
BOOTSTRAP_WINDOW_SIZE = 500_000
BOOTSTRAP_SAMPLES = 1_000
DENSITY_DECIMALS = 2
DENSITY_UNCERTAINTY_DECIMALS = 3
# simulation.RUN reports density in g/cm3; multiply by this factor to
# report Mean_density / Density_uncertainty in kg/m3 instead.
DENSITY_UNIT_CONVERSION_FACTOR = 1000.0
# Set to an integer (e.g. 42) to make the bootstrap reproducible run to
# run; leave as None for a fresh random draw every time the script runs.
BOOTSTRAP_SEED = None


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
    "Compute_device",
    "Simulation_time_s",
    "Mean_density",
    "Density_uncertainty",
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
# DENSITY CALCULATION (BOOTSTRAP)
# ============================================================

def _read_last_density_values(density_file, window_size):
    """
    Stream simulation.RUN line by line and keep only the last
    `window_size` values of the 'density' column (a fixed-size
    deque discards older values as new ones come in, so the
    full file is never held in memory at once).
    """

    window = deque(maxlen=window_size)

    # Read-only: this must never become "w"/"a"/"r+". A simulation
    # folder reaches this point only after status_success exists, so
    # the run has already finished writing simulation.RUN.
    with density_file.open("r", encoding="utf-8") as file:

        header = file.readline().split()

        if "density" not in header:
            raise ValueError(
                f"'density' column not found in {density_file}"
            )

        density_index = header.index("density")

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

            window.append(density)

    return window


def calculate_density_bootstrap(
    simulation_folder,
    window_size=BOOTSTRAP_WINDOW_SIZE,
    n_bootstrap=BOOTSTRAP_SAMPLES,
    seed=BOOTSTRAP_SEED,
):
    """
    Calculate density and its uncertainty via bootstrap resampling.

    Method:
    1. Keep only the last `window_size` values of the 'density'
       column in simulation.RUN (the last 1 ns of simulation).
    2. Draw `n_bootstrap` resamples of size `window_size` from that
       window, each WITH replacement, and compute the mean of each
       resample (equivalent to building `n_bootstrap` new columns of
       `window_size` random rows drawn with replacement from the
       original vector).
    3. Density    = mean of the `n_bootstrap` resample means.
       Uncertainty = standard deviation of the `n_bootstrap`
       resample means.

    Replace this function if the density calculation method changes
    in the future.
    """

    density_file = Path(simulation_folder) / SIMULATION_FILE

    if not density_file.is_file():
        raise FileNotFoundError(
            f"Density file not found: {density_file}"
        )

    window = _read_last_density_values(density_file, window_size)

    if len(window) < window_size:
        raise ValueError(
            f"Only {len(window)} density values found in "
            f"{density_file}; at least {window_size} are needed "
            f"for the bootstrap window."
        )

    data = np.array(window, dtype=np.float64)
    data *= DENSITY_UNIT_CONVERSION_FACTOR

    rng = np.random.default_rng(seed)

    bootstrap_means = np.empty(n_bootstrap, dtype=np.float64)

    for i in range(n_bootstrap):
        resample = rng.choice(data, size=window_size, replace=True)
        bootstrap_means[i] = resample.mean()

    mean_density = round(
        float(bootstrap_means.mean()), DENSITY_DECIMALS
    )
    uncertainty = round(
        float(bootstrap_means.std(ddof=1)),
        DENSITY_UNCERTAINTY_DECIMALS,
    )

    return mean_density, uncertainty


# ============================================================
# SIMULATION LOG (COMPUTE DEVICE AND WALL TIME)
# ============================================================

def _extract_compute_device(log_file_name):
    """
    Extract the compute device encoded in the log file name, e.g.
    "GPU-2" from "log_gpu_GPU-2_sim14.lammps" or "CPU-2" from
    "log_omp_CPU-2_sim12.lammps". Matching ignores case, and the
    result is always returned upper case (e.g. "gpu-1" -> "GPU-1")
    so the column stays consistent regardless of how the file was
    named. Returns None if no device is found in the name.
    """

    match = COMPUTE_DEVICE_PATTERN.search(log_file_name)

    if match is None:
        return None

    return match.group(0).upper()


def _extract_simulation_wall_time(log_file):
    """
    Extract the total wall-clock simulation time from the last
    non-empty line of a LAMMPS log file (e.g. "Total wall time:
    1:28:18") and return it in seconds.
    """

    last_line = None

    # Read-only (see note on _read_last_density_values above).
    with log_file.open("r", encoding="utf-8", errors="replace") as file:
        for line in file:
            stripped = line.strip()

            if stripped:
                last_line = stripped

    if last_line is None:
        raise ValueError(f"Log file is empty: {log_file}")

    match = WALL_TIME_PATTERN.match(last_line)

    if match is None:
        raise ValueError(
            f"'Total wall time' not found on the last line of "
            f"{log_file}: {last_line!r}"
        )

    hours = int(match.group("hours"))
    minutes = int(match.group("minutes"))
    seconds = int(match.group("seconds"))

    return hours * 3600 + minutes * 60 + seconds


def extract_log_info(simulation_folder):
    """
    Find the simulation log file in `simulation_folder` and extract:
    - Compute_device: the GPU/CPU device encoded in its file name
      (e.g. "GPU-2", "CPU-2"), or None if no log file is found or
      its name doesn't match the expected pattern.
    - Simulation_time_s: total wall-clock simulation time, in
      seconds, parsed from the last line of the file.

    Both are stored for later use (GPU vs CPU comparison, average
    and remaining simulation time); no such calculation is done here.
    """

    log_files = sorted(
        Path(simulation_folder).glob(LOG_FILE_GLOB),
        key=lambda file: file.stat().st_mtime,
    )

    if not log_files:
        raise FileNotFoundError(
            f"No log file ({LOG_FILE_GLOB}) found in {simulation_folder}"
        )

    # If more than one log file exists, use the most recently
    # modified one.
    log_file = log_files[-1]

    compute_device = _extract_compute_device(log_file.name)
    simulation_time_s = _extract_simulation_wall_time(log_file)

    return compute_device, simulation_time_s


# ============================================================
# RESULTS CACHE (completed simulations are only computed once)
# ============================================================

def load_results_cache():
    """
    Load the per-folder cache of already-computed results for
    completed simulations. Returns an empty dict if the file doesn't
    exist yet or is unreadable - that just means every folder is
    (re)computed fresh this run, exactly like before caching existed.
    """

    try:
        with RESULTS_CACHE_FILE.open("r", encoding="utf-8") as file:
            return json.load(file)

    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_results_cache(cache):
    """Persist the (possibly updated) cache for the next run."""

    with RESULTS_CACHE_FILE.open("w", encoding="utf-8") as file:
        json.dump(cache, file, indent=2, sort_keys=True)


def _cache_key(simulation_folder):
    """
    A stable identifier for a simulation folder, used as its key in
    the results cache. This is the path relative to ROOT_DIR, not
    just the bare folder name - the tree can be organized into
    subfolders (by batch, by month, etc.) at any depth, and two
    folders in different places that happen to share the same name
    must never be confused with each other's cached results.
    """

    try:
        return str(simulation_folder.relative_to(ROOT_DIR))

    except ValueError:
        # Should never happen (every folder here comes from walking
        # ROOT_DIR), but fall back to something still unique rather
        # than crash.
        return str(simulation_folder)


# ============================================================
# SIMULATION STATUS
# ============================================================

@task(retries=2, retry_delay_seconds=30)
def inspect_simulation(simulation_folder, parameters, cache):
    """
    Determine simulation status based only on whether a
    status_success file is present.

    - status_success exists -> "Success": record the completion
      date/time, and get the log info / density either from `cache`
      (a finished simulation's files never change, so once they are
      read once there is nothing to gain from reading them again) or
      by computing them now, in which case `cache` is updated
      in place so the next run can reuse them.
    - status_success absent -> "Not completed": the folder is
      skipped, nothing is read or cached.
    """

    success_files = sorted(
        simulation_folder.glob("status_success*")
    )

    result = parameters.copy()
    result["Status"] = None
    result["Success_datetime"] = None
    result["Compute_device"] = None
    result["Simulation_time_s"] = None
    result["Mean_density"] = None
    result["Density_uncertainty"] = None

    if not success_files:
        result["Status"] = "Not completed"
        return result

    result["Status"] = "Success"

    # If multiple success files exist (e.g. a simulation was rerun),
    # use the latest date/time encoded in their filenames.
    success_datetimes = [
        dt
        for dt in (
            extract_success_datetime(success_file)
            for success_file in success_files
        )
        if dt is not None
    ]

    if success_datetimes:
        result["Success_datetime"] = max(success_datetimes)

    success_iso = (
        result["Success_datetime"].isoformat()
        if result["Success_datetime"] is not None
        else None
    )

    cache_key = _cache_key(simulation_folder)
    cached_entry = cache.get(cache_key)

    # A cached entry whose Success_datetime doesn't match the current
    # one is stale (the simulation was rerun since it was cached, so
    # its files are not the ones that were actually computed) -
    # discard it and compute everything fresh below.
    if cached_entry is not None and cached_entry.get("success_datetime") != success_iso:
        cached_entry = None

    if cached_entry is None:
        cached_entry = {"success_datetime": success_iso}
        cache[cache_key] = cached_entry

    # Compute device and simulation wall time, from the log file.
    if "compute_device" in cached_entry and "simulation_time_s" in cached_entry:
        result["Compute_device"] = cached_entry["compute_device"]
        result["Simulation_time_s"] = cached_entry["simulation_time_s"]

    else:
        try:
            compute_device, simulation_time_s = extract_log_info(
                simulation_folder
            )
            result["Compute_device"] = compute_device
            result["Simulation_time_s"] = simulation_time_s
            cached_entry["compute_device"] = compute_device
            cached_entry["simulation_time_s"] = simulation_time_s

        except Exception:
            # Left out of the cache on purpose, so this is retried
            # on the next run instead of being stuck forever.
            logging.exception(
                "Log file processing failed for: %s",
                simulation_folder,
            )

    # Density: same idea, reuse the cached value when there is one.
    if "mean_density" in cached_entry and "density_uncertainty" in cached_entry:
        result["Mean_density"] = cached_entry["mean_density"]
        result["Density_uncertainty"] = cached_entry["density_uncertainty"]

    else:
        try:
            mean_density, uncertainty = calculate_density_bootstrap(
                simulation_folder
            )
            result["Mean_density"] = mean_density
            result["Density_uncertainty"] = uncertainty
            cached_entry["mean_density"] = mean_density
            cached_entry["density_uncertainty"] = uncertainty

        except Exception:
            # Also left out of the cache, for the same reason.
            logging.exception(
                "Density calculation failed for: %s",
                simulation_folder,
            )

    return result


# ============================================================
# FIND SIMULATION FOLDERS
# ============================================================

def _log_walk_error(error):
    """
    os.walk() silently skips a directory it fails to list unless it
    is given this callback — without it, a transient network-drive
    hiccup would quietly produce an incomplete scan, with nothing in
    the log to show it happened, and that incomplete result would
    then overwrite yesterday's good data in save_results.
    """

    logging.warning("Error while scanning %s: %s", ROOT_DIR, error)


def scan_simulation_folders(root_dir):
    """
    Walk root_dir once, sorting every subfolder into one of two
    groups: folders whose name matches FOLDER_PATTERN (valid
    simulations) and folders named "S1_..." that don't fully match
    it (recorded as invalid, likely a typo in the folder name).

    One pass instead of two separate walks, since ROOT_DIR can hold
    thousands of folders on a network drive and listing it twice
    doubles that I/O for no benefit. The "logs" directory is never
    scanned, and once a folder is recognized as a valid simulation,
    nothing inside it is scanned either (each simulation folder is
    one flat unit, not a tree to recurse into).

    Returns (valid_folders, invalid_folder_names): valid_folders is
    a list of (folder_path, parameters) tuples.
    """

    valid_folders = []
    invalid_folder_names = []

    for current_path, directories, _ in os.walk(
        root_dir, onerror=_log_walk_error
    ):

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
            valid_folders.append((current_folder, parameters))

            # Simulation folders are treated as individual units.
            # Their internal subdirectories are not scanned.
            directories[:] = []

        elif current_folder.name.startswith("S1_"):
            invalid_folder_names.append(current_folder.name)

    return valid_folders, invalid_folder_names


# ============================================================
# EXCEL OUTPUT
# ============================================================

def build_output_dataframes(results, invalid_folders):
    """
    Build the two core result tables shared by both outputs (the
    Excel workbook and the Google Sheet): the per-simulation rows
    and the summary counts.
    """

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
                "Metric": "Not completed simulations",
                "Value": (
                    dataframe["Status"].eq("Not completed").sum()
                    if not dataframe.empty else 0
                ),
            },
            {
                "Metric": "Folders with invalid simulation names",
                "Value": len(invalid_folders),
            },
        ]
    )

    return dataframe, summary


@task(
    retries=SAVE_RESULTS_RETRIES,
    retry_delay_seconds=SAVE_RESULTS_RETRY_DELAY_S,
)
def save_results(dataframe, summary, invalid_folders):
    """Overwrite the existing Excel workbook with the latest results."""

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

        # Number formats so density/uncertainty keep a fixed amount
        # of decimal places when viewed in Excel.
        number_formats = {
            "Mean_density": f"0.{'0' * DENSITY_DECIMALS}",
            "Density_uncertainty": f"0.{'0' * DENSITY_UNCERTAINTY_DECIMALS}",
        }

        # Format worksheet columns for easier reading.
        for worksheet in writer.sheets.values():
            worksheet.freeze_panes = "A2"
            worksheet.auto_filter.ref = worksheet.dimensions

            header_cells = next(worksheet.iter_rows(max_row=1))

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

            for header_cell in header_cells:
                number_format = number_formats.get(header_cell.value)

                if number_format is None:
                    continue

                column_letter = header_cell.column_letter

                for row in range(
                    2, worksheet.max_row + 1
                ):
                    worksheet[
                        f"{column_letter}{row}"
                    ].number_format = number_format


# ============================================================
# GOOGLE SHEETS OUTPUT
# ============================================================

# Keep Mean_density / Density_uncertainty at a fixed number of
# decimal places in the sheet too, matching the Excel output.
_SHEET_NUMBER_FORMATS = {
    "Mean_density": f"0.{'0' * DENSITY_DECIMALS}",
    "Density_uncertainty": f"0.{'0' * DENSITY_UNCERTAINTY_DECIMALS}",
}


def _value_for_sheets(value):
    """
    Convert a single DataFrame cell into something the Sheets API
    accepts (plain str / int / float / bool): missing values become
    an empty string, dates become readable strings, and numpy
    numeric types become plain Python numbers.
    """

    if pd.isna(value):
        return ""

    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, (np.integer, np.floating)):
        return value.item()

    return value


def _dataframe_to_sheet_values(dataframe):
    """Convert a DataFrame into a header-row + data-rows list of lists."""

    header = list(dataframe.columns)
    rows = [
        [_value_for_sheets(value) for value in row]
        for row in dataframe.itertuples(index=False)
    ]

    return [header] + rows


def _write_worksheet(spreadsheet, title, values):
    """
    Get or create the `title` tab, clear whatever it currently
    holds, and write `values` (header + rows) starting at A1.
    """

    try:
        worksheet = spreadsheet.worksheet(title)

    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=title,
            rows=len(values),
            cols=len(values[0]),
        )

    worksheet.clear()
    worksheet.update(range_name="A1", values=values)

    header = values[0]

    for column_index, column_name in enumerate(header, start=1):
        pattern = _SHEET_NUMBER_FORMATS.get(column_name)

        if pattern is None:
            continue

        column_letter = gspread.utils.rowcol_to_a1(1, column_index)[:-1]
        worksheet.format(
            f"{column_letter}2:{column_letter}",
            {"numberFormat": {"type": "NUMBER", "pattern": pattern}},
        )


@task(retries=3, retry_delay_seconds=60)
def update_google_sheet(dataframe, summary, invalid_folders):
    """
    Mirror the same results into a Google Sheet, alongside the Excel
    file, so the data is reachable from any computer (e.g. via Power
    BI or a browser) without opening the workbook itself.

    Requires the one-time setup described next to GOOGLE_SHEET_ID
    above: a Google Cloud service account with the Sheets API
    enabled, its JSON key at GOOGLE_SERVICE_ACCOUNT_FILE, and a
    Google Sheet shared with that service account.
    """

    gc = gspread.service_account(
        filename=str(GOOGLE_SERVICE_ACCOUNT_FILE)
    )
    spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)

    _write_worksheet(
        spreadsheet, SHEET_NAME, _dataframe_to_sheet_values(dataframe)
    )
    _write_worksheet(
        spreadsheet, "Summary", _dataframe_to_sheet_values(summary)
    )
    _write_worksheet(
        spreadsheet,
        "Invalid_folders",
        _dataframe_to_sheet_values(
            pd.DataFrame({"Invalid_folder": invalid_folders})
        ),
    )


# ============================================================
# RUN SAFETY (overlap lock, incomplete-scan guard)
# ============================================================

def _assert_output_dir_is_safe():
    """
    Defensive check, run once at startup: OUTPUT_DIR (where this
    script writes its own Excel/cache/lock/log files) must never be
    ROOT_DIR itself or a folder inside it. This should already be
    impossible given how both are defined above, but if either one
    is ever edited in the future in a way that makes them overlap,
    this stops the script before it writes anything, instead of its
    own output files silently landing inside the simulation tree.
    """

    root_resolved = ROOT_DIR.resolve()
    output_resolved = OUTPUT_DIR.resolve()

    if output_resolved == root_resolved or root_resolved in output_resolved.parents:
        raise RuntimeError(
            f"OUTPUT_DIR ({OUTPUT_DIR}) is the same as, or inside, "
            f"ROOT_DIR ({ROOT_DIR}). Refusing to run - fix one of "
            "these two paths at the top of the script before "
            "rerunning."
        )


def _acquire_lock():
    """
    Refuse to start a second run while another one is still in
    progress (both would read/write the same Excel file, Google
    Sheet, and cache at once). Returns True if the lock was
    acquired by this run (the caller must call _release_lock() when
    done), False if another run appears to still be active.
    """

    if LOCK_FILE.is_file():
        age_hours = (time.time() - LOCK_FILE.stat().st_mtime) / 3600

        if age_hours < LOCK_STALE_AFTER_HOURS:
            return False

        logging.warning(
            "Found a lock file older than %s hours (%.1fh) - "
            "treating it as left over from a run that crashed "
            "without cleaning up, and continuing.",
            LOCK_STALE_AFTER_HOURS,
            age_hours,
        )

    LOCK_FILE.write_text(datetime.now().isoformat(), encoding="utf-8")
    return True


def _release_lock():
    try:
        LOCK_FILE.unlink(missing_ok=True)

    except OSError:
        logging.exception("Could not remove the lock file %s.", LOCK_FILE)


def load_last_known_folder_count():
    """Folder count from the last scan trusted enough to be saved."""

    try:
        with MONITOR_STATE_FILE.open("r", encoding="utf-8") as file:
            return json.load(file).get("valid_folder_count")

    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_last_known_folder_count(count):
    with MONITOR_STATE_FILE.open("w", encoding="utf-8") as file:
        json.dump({"valid_folder_count": count}, file)


# ============================================================
# MAIN PROGRAM
# ============================================================

@flow(name="monitor-simulations", log_prints=True)
def main():

    if not ROOT_DIR.is_dir():
        raise FileNotFoundError(
            f"Simulation root directory not found: {ROOT_DIR}"
        )

    _assert_output_dir_is_safe()

    configure_logging()

    # Refuse to run if a previous run seems to still be in progress,
    # so two instances never write to the same Excel file / Google
    # Sheet / cache at once (see LOCK_STALE_AFTER_HOURS).
    if not _acquire_lock():
        logging.warning(
            "Another run appears to still be in progress (%s is "
            "recent) - skipping this run.",
            LOCK_FILE,
        )
        return

    try:
        logging.info("Starting simulation monitoring.")
        logging.info("Root directory: %s", ROOT_DIR)

        valid_folders, invalid_folders = scan_simulation_folders(ROOT_DIR)

        # Guard against a partial/incomplete scan (e.g. the network
        # drive glitched partway through) silently overwriting good
        # historical data with a worse snapshot. See
        # MIN_FOLDER_COUNT_RATIO.
        last_known_count = load_last_known_folder_count()

        if (
            MIN_FOLDER_COUNT_RATIO > 0
            and last_known_count is not None
            and last_known_count > 0
            and len(valid_folders) < last_known_count * MIN_FOLDER_COUNT_RATIO
        ):
            logging.error(
                "Found only %s simulation folders, down from %s on "
                "the last trusted run (more than a %.0f%% drop). "
                "This looks like an incomplete scan rather than "
                "simulations actually disappearing, so the Excel "
                "file and Google Sheet were NOT touched this run - "
                "check that %s is reachable and rerun.",
                len(valid_folders),
                last_known_count,
                (1 - MIN_FOLDER_COUNT_RATIO) * 100,
                ROOT_DIR,
            )
            return

        save_last_known_folder_count(len(valid_folders))

        cache = load_results_cache()
        results = []
        processed_count = 0

        for simulation_folder, parameters in valid_folders:

            processed_count += 1

            try:
                result = inspect_simulation(
                    simulation_folder,
                    parameters,
                    cache,
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

        try:
            save_results_cache(cache)

        except Exception:
            logging.exception(
                "Could not save the results cache (%s); already"
                "-computed simulations will be recomputed next run.",
                RESULTS_CACHE_FILE,
            )

        dataframe, summary = build_output_dataframes(
            results, invalid_folders
        )

        # Excel and Google Sheets are two independent outputs built
        # from the same data: each is attempted regardless of what
        # happens to the other, so a long Excel-file-lock retry (up
        # to ~50 min, see SAVE_RESULTS_RETRIES) never delays the
        # Google Sheet, and a Google Sheets problem never discards
        # the Excel file.
        excel_saved = False

        try:
            save_results(dataframe, summary, invalid_folders)
            excel_saved = True

        except PermissionError:
            # Most common cause: OUTPUT_FILE is currently open in
            # Excel (or another program), which locks it for writing.
            logging.error(
                "Could not write %s: permission denied. Close the "
                "file if it is open in Excel (or any other "
                "program); the next scheduled run will try again. "
                "Simulations processed this run: %s.",
                OUTPUT_FILE,
                processed_count,
            )

        try:
            update_google_sheet(dataframe, summary, invalid_folders)
            logging.info("Google Sheet updated.")

        except Exception:
            logging.exception("Could not update the Google Sheet.")

        logging.info("Monitoring completed.")
        logging.info("Simulations processed: %s", processed_count)
        logging.info(
            "Excel file updated: %s",
            OUTPUT_FILE if excel_saved else "NO - see error above",
        )
        logging.info("Log file: %s", LOG_FILE)

    finally:
        _release_lock()


if __name__ == "__main__":
    main()
