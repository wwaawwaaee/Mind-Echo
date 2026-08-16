"""Shared Matplotlib style and export helpers for Mind-Echo academic figures."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


COLORS: Dict[str, str] = {
    "doctor": "#3B6EA8",
    "caregiver": "#D97706",
    "patient": "#4B5563",
    "gad": "#2563EB",
    "phq": "#DC2626",
    "accent": "#059669",
    "muted": "#6B7280",
    "grid": "#D1D5DB",
    "text": "#111827",
    "background": "#FFFFFF",
}


def apply_academic_style() -> None:
    """Apply vector-friendly English academic plotting settings."""
    plt.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["DejaVu Sans", "Arial", "Liberation Sans"],
            "axes.unicode_minus": False,
            "svg.fonttype": "none",
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "figure.dpi": 120,
            "savefig.dpi": 300,
            "savefig.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": "#374151",
            "axes.labelcolor": COLORS["text"],
            "xtick.color": COLORS["text"],
            "ytick.color": COLORS["text"],
            "text.color": COLORS["text"],
            "axes.titleweight": "bold",
            "axes.titlesize": 10.5,
            "axes.labelsize": 9.5,
            "xtick.labelsize": 8.5,
            "ytick.labelsize": 8.5,
            "legend.fontsize": 8.5,
            "figure.titlesize": 14,
            "figure.titleweight": "bold",
            "lines.linewidth": 1.6,
            "patch.linewidth": 0.8,
        }
    )


def style_axis(ax, grid_axis: str = "y") -> None:
    """Use a consistent light academic grid and remove visual clutter."""
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, axis=grid_axis, color=COLORS["grid"], linewidth=0.6, alpha=0.55)
    ax.set_axisbelow(True)


def panel_label(ax, label: str) -> None:
    """Place a bold lower-case panel label at the upper-left of an axis."""
    ax.text(
        -0.12,
        1.08,
        label,
        transform=ax.transAxes,
        fontsize=12,
        fontweight="bold",
        va="top",
        ha="left",
    )


def export_figure(fig, output_dir: Path, stem: str) -> List[Path]:
    """Export a figure as SVG, PDF, and a 300-dpi PNG preview."""
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = [output_dir / f"{stem}.svg", output_dir / f"{stem}.pdf", output_dir / f"{stem}.png"]
    fig.savefig(paths[0], bbox_inches="tight", facecolor="white")
    fig.savefig(paths[1], bbox_inches="tight", facecolor="white")
    fig.savefig(paths[2], bbox_inches="tight", facecolor="white", dpi=300)
    return paths
