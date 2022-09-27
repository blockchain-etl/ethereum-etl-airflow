import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from string import ascii_lowercase


def generate_ordered_index(input: str, width=3) -> int:
    # Map int (0 to 25) for [a-z]
    chars_map = {ascii_lowercase[i]: i for i in range(26)}

    # Force lower case and remove non-alpha chars, then pad to minimum width
    input_chars = re.sub(r"[^a-z]", "", input.lower())
    input_chars_padded = input_chars.ljust(width, "a")

    # "a__bc" -> (0 * 26**2) + (1 * 26**1) + (2 * 26**0) = 28
    output = 0
    for i in range(width):
        input_char = input_chars_padded[i]
        output_int = chars_map[input_char]
        output += output_int * 26 ** (width - i - 1)
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
    max_ordered_index = 26**3 - 1
    offset_minutes = total_minutes * ordered_index // max_ordered_index

    return timedelta(minutes=offset_minutes)


def calculate_schedule_interval(
    daily_schedule_start: time,
    schedule_offset: timedelta
):
    start_dt = datetime.combine(date.min, daily_schedule_start)
    schedule_time = (start_dt + schedule_offset).time()
    
    return f"{schedule_time.minute} {schedule_time.hour} * * *"
