# Unit Tests

Automated unit tests using pytest framework.

## Running Tests

```bash
# Run all unit tests
pytest tests/unit/

# Run with coverage
pytest tests/unit/ --cov=../

# Run specific test file
pytest tests/unit/test_storage.py
```

## Test Structure

- `test_storage.py` - Storage abstraction tests
- `test_auth.py` - Authentication tests  
- `test_client.py` - GRIN client tests

## Guidelines

- Use async test functions with `@pytest.mark.asyncio`
- Mock external dependencies (HTTP calls, file system)
- Focus on testing business logic, not integration
- Keep tests fast and deterministic