# Contributing to Multi-Cloud Data Pipeline Framework

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing to this project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Pull Request Process](#pull-request-process)

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for all contributors.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/your-username/multi-cloud-data-pipeline.git`
3. Add upstream remote: `git remote add upstream https://github.com/original-owner/multi-cloud-data-pipeline.git`

## Development Setup

### Prerequisites

- Python 3.8+
- pip
- virtualenv or conda
- Git

### Setup Instructions

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

## How to Contribute

### Reporting Bugs

1. Check if the bug has already been reported in [Issues](https://github.com/your-username/multi-cloud-data-pipeline/issues)
2. If not, create a new issue with:
   - Clear title and description
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (OS, Python version, etc.)
   - Code samples if applicable

### Suggesting Enhancements

1. Open an issue with the "enhancement" label
2. Provide:
   - Clear description of the proposed feature
   - Use cases and benefits
   - Potential implementation approach

### Code Contributions

1. **Create a branch**: `git checkout -b feature/your-feature-name`
2. **Make your changes**: Follow our coding standards
3. **Add tests**: Ensure your code is well-tested
4. **Update documentation**: Update README.md and docstrings
5. **Commit your changes**: Use clear, descriptive commit messages
6. **Push to your fork**: `git push origin feature/your-feature-name`
7. **Submit a Pull Request**

## Coding Standards

### Python Style Guide

We follow PEP 8 with some modifications:

- **Line length**: 100 characters maximum
- **Formatting**: Use Black for code formatting
- **Import sorting**: Use isort
- **Linting**: Code must pass flake8 and pylint checks

### Code Quality Tools

Run these before submitting:

```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Lint code
flake8 src/ tests/
pylint src/

# Type checking
mypy src/
```

### Docstring Format

Use Google-style docstrings:

```python
def function_name(param1: str, param2: int) -> bool:
    """
    Brief description of function.
    
    More detailed description if needed.
    
    Args:
        param1: Description of param1
        param2: Description of param2
    
    Returns:
        Description of return value
    
    Raises:
        ValueError: When invalid input is provided
    
    Example:
        >>> function_name("test", 42)
        True
    """
    pass
```

## Testing Guidelines

### Running Tests

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=src/multicloud_pipeline tests/

# Run specific test file
pytest tests/test_pipeline.py

# Run specific test
pytest tests/test_pipeline.py::TestPipeline::test_pipeline_initialization
```

### Writing Tests

- Place tests in the `tests/` directory
- Mirror the source structure (e.g., `src/module.py` → `tests/test_module.py`)
- Use descriptive test names: `test_should_do_something_when_condition`
- Aim for >80% code coverage
- Test both success and failure cases
- Use fixtures for common setup
- Mock external dependencies

Example test:

```python
def test_pipeline_validates_with_source():
    """Test that pipeline validation passes when source is provided"""
    pipeline = Pipeline(name="test", cloud_provider="azure")
    pipeline.add_source(Mock())
    
    assert pipeline.validate() is True
```

## Pull Request Process

### Before Submitting

- [ ] All tests pass
- [ ] Code is formatted (Black, isort)
- [ ] Linting passes (flake8, pylint)
- [ ] Documentation is updated
- [ ] CHANGELOG.md is updated (if applicable)
- [ ] Commit messages are clear and descriptive

### PR Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
Describe testing performed

## Checklist
- [ ] Tests pass
- [ ] Code formatted
- [ ] Documentation updated
- [ ] No breaking changes (or documented)
```

### Review Process

1. Maintainers will review your PR
2. Address any requested changes
3. Once approved, your PR will be merged
4. Your contribution will be acknowledged in the release notes

## Development Workflow

### Branch Naming

- Features: `feature/description`
- Bug fixes: `fix/description`
- Documentation: `docs/description`
- Performance: `perf/description`

### Commit Messages

Follow conventional commits:

```
type(scope): brief description

Longer description if needed

Fixes #issue-number
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Formatting
- `refactor`: Code restructuring
- `test`: Adding tests
- `chore`: Maintenance

Examples:
```
feat(connectors): add support for Azure Synapse
fix(pipeline): resolve validation error for empty sources
docs(readme): update installation instructions
```

## Project Structure

```
multi-cloud-data-pipeline/
├── src/multicloud_pipeline/     # Source code
│   ├── connectors/              # Data connectors
│   ├── transformers/            # Transformations
│   ├── orchestration/           # Orchestration
│   └── quality/                 # Data quality
├── tests/                       # Test files
├── examples/                    # Example pipelines
├── terraform/                   # Infrastructure as Code
├── docs/                        # Documentation
└── .github/                     # GitHub workflows
```

## Getting Help

- **Questions**: Open a GitHub Discussion
- **Bugs**: Open a GitHub Issue
- **Chat**: Join our Slack channel (link in README)

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Recognition

Contributors will be acknowledged in:
- CONTRIBUTORS.md file
- Release notes
- Project documentation

Thank you for contributing to Multi-Cloud Data Pipeline Framework! 🚀
