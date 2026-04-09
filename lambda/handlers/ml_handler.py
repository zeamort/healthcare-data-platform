"""
Generic ML Lambda handler.

The ML_MODULE environment variable selects which ML script to run:
  ml_clustering, ml_risk_scoring, or ml_comorbidity.
"""

import os
import importlib
import json
import logging

log = logging.getLogger()
log.setLevel(logging.INFO)

ML_MODULE = os.environ.get("ML_MODULE", "ml_clustering")


def lambda_handler(event, context):
    """Import and run the configured ML module's main()."""
    log.info("Running ML module: %s (event: %s)", ML_MODULE,
             json.dumps(event, default=str))

    mod = importlib.import_module(ML_MODULE)
    mod.main()

    log.info("%s complete.", ML_MODULE)
    return {"status": "complete", "module": ML_MODULE}
