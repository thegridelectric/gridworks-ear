"""Sphinx configuration."""
project = "Gridworks Ear"
author = "GridWorks"
copyright = "2022, GridWorks"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_click",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "furo"
