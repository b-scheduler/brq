# Making a new release of brq

## Manual release

### Python package

This project can be distributed as Python
packages. Before generating a package, we first need to install `build`.

```bash
pip install twine hatch
```

Bump the version using `hatch`.

```bash
hatch version <new-version>
```

To create a Python source package (`.tar.gz`) and the binary package (`.whl`) in the `dist/` directory, do:

```bash
rm -rf dist/*
hatch build
```

> `python setup.py sdist bdist_wheel` is deprecated and will not work for this package.

Then to upload the package to PyPI, do:

```bash
twine upload dist/*
```

## Automatic release

### GitHub Actions

Configure the following secrets in the [GitHub repository](https://github.com/wh1isper/brq/settings/secrets/actions/new):

- `PYPI_API_TOKEN`: PyPI API token

### Docker iamges

Configure the following secrets in the [GitHub repository](https://github.com/wh1isper/brq/settings/secrets/actions/new):

- `DOCKERHUB_USERNAME`: DockerHub username
- `DOCKERHUB_TOKEN`: DockerHub token

### Release

Create a new release in GitHub. Everything will be automatically published to PyPI and DockerHub.
