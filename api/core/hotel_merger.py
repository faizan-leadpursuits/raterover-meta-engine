"""
Hotel result merger and deduplication engine.
Merges results from multiple hotel providers and detects cross-listed properties.
"""

import re
import pandas as pd
from .hotel_schemas import HOTEL_COMMON_COLUMNS


class HotelMerger:
    """
    Merge and deduplicate hotel results from multiple providers.

    Deduplication works by creating a fingerprint from:
    - Normalized hotel name + city + check_in + check_out
    When duplicates are found, the cheapest price is kept and
    all sources are tracked in the 'all_sources' column.
    """

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalize hotel name for fuzzy matching."""
        if not name:
            return ""
        # Lowercase, strip common suffixes, remove non-alphanumeric
        name = name.lower().strip()
        # Remove common words that vary across providers
        for word in ["hotel", "resort", "suites", "the", "&", "and", "-"]:
            name = name.replace(word, " ")
        # Collapse whitespace and strip
        name = re.sub(r"\s+", " ", name).strip()
        return name

    @staticmethod
    def _make_fingerprint(row):
        """Create a dedup fingerprint for a hotel result."""
        name = HotelMerger._normalize_name(str(row.get("hotel_name", "")))
        city = str(row.get("city", "")).lower().strip()
        cin = str(row.get("check_in", ""))
        cout = str(row.get("check_out", ""))
        return f"{name}|{city}|{cin}|{cout}"

    def merge(self, dataframes: list) -> pd.DataFrame:
        """
        Merge multiple DataFrames, deduplicate, and track cross-listings.

        Args:
            dataframes: List of DataFrames, each with HOTEL_COMMON_COLUMNS

        Returns:
            Merged DataFrame with added 'all_sources' and 'source_count' columns
        """
        if not dataframes:
            return pd.DataFrame(columns=HOTEL_COMMON_COLUMNS)

        # Ensure all DataFrames have the expected columns
        cleaned = []
        for df in dataframes:
            if df.empty:
                continue
            for col in HOTEL_COMMON_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
            cleaned.append(df[HOTEL_COMMON_COLUMNS])

        if not cleaned:
            return pd.DataFrame(columns=HOTEL_COMMON_COLUMNS)

        merged = pd.concat(cleaned, ignore_index=True)

        if merged.empty:
            return merged

        # ── Deduplication ──
        merged["_fingerprint"] = merged.apply(self._make_fingerprint, axis=1)

        # Group by fingerprint, keep cheapest, track all sources
        deduped_rows = []
        for fp, group in merged.groupby("_fingerprint"):
            if fp == "|||":  # Skip invalid fingerprints
                for _, row in group.iterrows():
                    row_dict = row.to_dict()
                    row_dict["all_sources"] = row_dict.get("source", "")
                    row_dict["source_count"] = 1
                    deduped_rows.append(row_dict)
                continue

            # Sort by price ascending, keep the cheapest
            group_sorted = group.sort_values("price", ascending=True, na_position="last")
            best = group_sorted.iloc[0].to_dict()

            # Track all sources
            sources = sorted(group["source"].unique())
            best["all_sources"] = ", ".join(sources)
            best["source_count"] = len(sources)

            # Track all booking providers and their prices
            providers_prices = []
            for _, row in group_sorted.iterrows():
                bp = row.get("booking_provider", "") or row.get("source", "")
                p = row.get("price", 0)
                if bp and p > 0:
                    providers_prices.append(f"{bp}: {p:.0f}")
            best["all_prices"] = " | ".join(providers_prices) if providers_prices else ""

            deduped_rows.append(best)

        result = pd.DataFrame(deduped_rows)

        # Clean up
        if "_fingerprint" in result.columns:
            result = result.drop(columns=["_fingerprint"])

        # Ensure numeric types
        for col in ["price", "price_per_night", "star_rating", "guest_rating",
                     "review_count", "nights"]:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0)

        # Sort by price by default
        if "price" in result.columns:
            result = result.sort_values("price", ascending=True, na_position="last")

        # Print stats
        total_raw = sum(len(df) for df in cleaned)
        dupes_removed = total_raw - len(result)
        multi_source = (
            len(result[result.get("source_count", 1) > 1])
            if "source_count" in result.columns
            else 0
        )

        print(f"\n  {'─' * 60}")
        print(f"  DEDUPLICATION")
        print(f"  {'─' * 60}")
        print(f"  Total raw results: {total_raw}")
        print(f"  After deduplication: {len(result)} unique hotels")
        print(f"  Duplicates removed: {dupes_removed}")
        print(f"  Multi-source hotels: {multi_source}")

        return result.reset_index(drop=True)
