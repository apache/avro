# Apache Avro website

This website is base on [Hugo](https://gohugo.io) and uses the [Docsy](https://www.docsy.dev/) theme.

## Previewing the website locally

```
# From the doc directory, you will need to do this at least once for our SCSS modifications
(cd doc && npm install)

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

```
# Install the necessary npm packages
docker run --entrypoint=sh --rm -v $(pwd):/src -p 1313:1313 jakejarvis/hugo-extended:latest \
    -c "cd build/staging-web && npm install"
# Generate the website and the release documentation
docker run --rm -v $(pwd):/src -p 1313:1313 jakejarvis/hugo-extended:latest \
    --source build/staging-web/  --gc --minify
# Optional: docker leaves some files with unmanageable permissions 
sudo chown -R $USER:$USER build/staging-web
```

## Avro version

(TODO)

When a new version of Apache Avro is released:

1. Change the value of `params.avroversion` in `config.toml`
2. Add a new entry to the `Releases` pages in the `Blog` section, for example:

```
cp content/en/blog/releases/avro-1.10.2-released.md content/en/blog/releases/avro-1.11.0-released.md
```

## Updating the https://avro.apache.org website from a distribution

(TODO)

