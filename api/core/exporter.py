"""
Result Exporter — saves search results to CSV/XLSX files.
"""

import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ResultExporter:
    """Exports search results (DataFrames) to CSV and/or XLSX."""

    def __init__(self, output_dir="results"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def export(self, df, prefix="unified", formats=None):
        """Export DataFrame to file(s). Returns list of saved paths."""
        if df is None or df.empty:
            return []

        formats = formats or ["csv"]
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        saved = []

        for fmt in formats:
            fname = f"{prefix}_{ts}.{fmt}"
            fpath = os.path.join(self.output_dir, fname)
            try:
                if fmt == "csv":
                    df.to_csv(fpath, index=False)
                elif fmt == "xlsx":
                    df.to_excel(fpath, index=False)
                saved.append(fpath)
                logger.info("Exported %d rows → %s", len(df), fpath)
            except Exception as e:
                logger.error("Export to %s failed: %s", fmt, e)

        return saved
