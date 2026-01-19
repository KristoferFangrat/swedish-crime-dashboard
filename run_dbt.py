#!/usr/bin/env python
"""
Helper script to run dbt commands with environment variables loaded from .env file.
Usage: python run_dbt.py test
       python run_dbt.py run
       python run_dbt.py build
       python run_dbt.py run --select model_name
"""

import os
import subprocess
import sys
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Run dbt with all passed arguments
if __name__ == "__main__":
    # Pass all command-line arguments to dbt
    dbt_args = ["dbt"] + sys.argv[1:]
    result = subprocess.run(dbt_args)
    sys.exit(result.returncode)
