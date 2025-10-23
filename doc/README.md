# Apache Avro website

This website is base on [Hugo](https://gohugo.io) and uses the [Docsy](https://www.docsy.dev/) theme.
Before building the website, you need to initialize submodules.

```sh
hugo mod get -u
```

## Previewing the website locally

```sh
# Serve the website dynamically using extended hugo:
hugo server --buildDrafts --buildFuture --bind 0.0.0.0 --navigateToChanged

# You can do the same thing without installing hugo via docker.
# From the Avro root directory:
docker run --rm -v $(pwd):/src -p 1313:1313 jakejarvis/hugo-extended:latest --source doc/ server \
    --buildDrafts --buildFuture --bind 0.0.0.0 --navigateToChanged
```

## New release

When a new version of Apache Avro is released:

1. Change the value of `params.avroversion` in `config.toml`
2. Add a new entry to the `Releases` pages in the `Blog` section, for example:

```sh
cp content/en/blog/releases/avro-1.12.0-released.md content/en/blog/releases/avro-1.13.0-released.md
```

### Upload the docs

Copy the Markdown content from the release tar to the `doc/content/en/docs/1.12.0`:

```sh
tar xvfz avro-src-1.12.0.tar.gz
```

Here we need to copy everything, except the `api/` directory to this repository. The markdown will be rendered using Hugo, and the API docs are already html, and will be served from the ASF SVN. The `api/` directory needs to be uploaded to SVN:

```sh
svn co https://svn.apache.org/repos/asf/avro/site
cd site/publish/docs/
mkdir 1.12.0
cd 1.12.0
mkdir api
cp -r ~/Desktop/avro-release-dist/avro-1.12.0/avro-doc-1.12.0/api/ api/
svn commit -m "Avro 1.12.0 release"
```
