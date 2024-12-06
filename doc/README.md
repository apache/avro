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

## Building the website in a distribution

When you build an Avro distribution with the script, there is currently a manual step required.

After all the binary artifacts and source have been created and copied to the `dist/` directory, the process will 
stop with **Build build/staging-web/ manually now. Press a key to continue...**

At this point, from another terminal and in the Avro root directory, you can build the website:

```sh
# Install the necessary npm packages
docker run --entrypoint=sh --rm -v $(pwd):/src -p 1313:1313 jakejarvis/hugo-extended:latest \
    -c "cd build/staging-web && npm install"
# Generate the website and the release documentation
docker run --rm -v $(pwd):/src -p 1313:1313 jakejarvis/hugo-extended:latest \
    --source build/staging-web/  --gc --minify
# Optional: docker leaves some files with unmanageable permissions 
sudo chown -R $USER:$USER build/staging-web
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
