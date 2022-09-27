import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from string import ascii_lowercase


def generate_ordered_index(input: str, width=3) -> int:
    # Map int (1 to 26) for [a-z] and 0 for everything else
    chars = "_" + ascii_lowercase
    chars_map = {chars[i]: i for i in range(27)}

    # Pad input to minimum width, replace upper case and non alphabetic chars
    input_ljust = input.ljust(width, "_")
    input_transformed = re.sub(r"[^a-z]", "_", input_ljust.lower())

    # "abc" -> (1 * 27**2) + (2 * 27**1) + (3 * 27**0) = 786
    output = 0
    for i in range(width):
        input_char = input_transformed[i]
        output_int = chars_map[input_char]
        output += output_int * 27 ** (width - i - 1)
    return output


def generate_schedule_offset(
    daily_schedule_start: time,
    daily_schedule_end: time,
    dataset_folder: str,
):
    assert (
        daily_schedule_start <= daily_schedule_end
    ), "`daily_schedule_end` cannot be before `daily_schedule_start`"

    start_dt = datetime.combine(date.min, daily_schedule_start)
    end_dt = datetime.combine(date.min, daily_schedule_end)
    total_minutes = (end_dt - start_dt) / timedelta(minutes=1)

    dataset_name = Path(dataset_folder).name
    ordered_index = generate_ordered_index(dataset_name, width=3)
    max_ordered_index = 27**3 - 1
    offset_minutes = total_minutes * ordered_index // max_ordered_index

    return timedelta(minutes=offset_minutes)


def calculate_schedule_interval(
    daily_schedule_start: time,
    schedule_offset: timedelta
):
    start_dt = datetime.combine(date.min, daily_schedule_start)
    schedule_time = (start_dt + schedule_offset).time()
    
    return f"{schedule_time.minute} {schedule_time.hour} * * *"
