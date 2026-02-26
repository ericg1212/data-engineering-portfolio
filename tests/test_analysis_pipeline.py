"""Tests for analysis_pipeline DAG structure.

analysis_pipeline uses BashOperator with mocked Airflow (no real Airflow installed),
so tests validate module-level structure: import, task IDs, and bash command content.
Bash command strings are verified via inspect.getsource() — the cleanest approach
when the operator itself is a MagicMock and doesn't expose real attributes.
"""

import inspect
import analysis_pipeline.analysis_pipeline as ap


class TestAnalysisPipelineDag:
    """Structural tests for the analysis_pipeline DAG."""

    def test_module_imports_without_error(self):
        """Importing the module must not raise — validates all top-level code."""
        assert ap is not None

    def test_run_backtest_task_exists(self):
        assert hasattr(ap, 'run_backtest')

    def test_run_portfolio_analysis_task_exists(self):
        assert hasattr(ap, 'run_portfolio_analysis')

    def test_backtest_bash_command_points_to_correct_script(self):
        """Source must reference historical_backtest.py in the backtest bash_command."""
        source = inspect.getsource(ap)
        assert 'historical_backtest.py' in source

    def test_portfolio_analysis_bash_command_points_to_correct_script(self):
        """Source must reference portfolio_analysis.py in the portfolio bash_command."""
        source = inspect.getsource(ap)
        assert 'portfolio_analysis.py' in source

    def test_bash_commands_include_pythonpath(self):
        """PYTHONPATH must be set so config.py is importable inside the container."""
        source = inspect.getsource(ap)
        assert 'PYTHONPATH' in source
