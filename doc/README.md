# Apache Avro website

This is a repository of Apache Avro website. The repository of Apache Avro can be found [here](https://github.com/apache/avro).

This website is base on [Hugo](https://gohugo.io) and uses [Docsy](https://www.docsy.dev/) theme.

## Getting started

Clone this repository:

```bash
git clone --recurse-submodules https://github.com/apache/avro-website.git
```

You can now edit your own versions of the site’s source files.

If you want to do SCSS edits and want to publish these, you need to install `PostCSS`

```bash
npm install
```

## Work flow

1. Building and running the site locally requires a recent `extended` version of [Hugo](https://gohugo.io).
You can find out more about how to install Hugo for your environment in our
[Getting started](https://www.docsy.dev/docs/getting-started/#prerequisites-and-installation) guide.
Once you've made your working copy of the site repo, from the repo root folder, run:

    
   ```
   hugo server --navigateToChanged
   ```
   
1. Edit .md and .html files in `content/` folder
1. Once satisfied with the changes, commit them: 
   
   ```
   git commit -a
   ```

1. Generate the HTML filse
stop `hugo server --navigateToChanged` (with Ctrl+C) and run 

   ```
   hugo
   ```
   
    This will generate the HTMLs in `public/` folder and this is actually what is being deployed

1. Add the modified HTML files to Git
    
   ```
   git add .
   git rm offline-search-index.<<OLD-HASH>>.json
   git commit -a
   git push
   ```


This way even when the PR modifies a lot of files we can review only the first commit, the meaningful one, with the modified files in `content/` folder


## Running a container locally

You can also run avro-website inside a [Docker](https://docs.docker.com/)
container, the container runs with a volume bound to the `avro-website`
folder. This approach doesn't require you to install any dependencies other
than [Docker Desktop](https://www.docker.com/products/docker-desktop) on
Windows and Mac, and [Docker Compose](https://docs.docker.com/compose/install/)
on Linux.

1. Build the docker image 

   ```bash
   docker-compose build
   ```

1. Run the built image

   ```bash
   docker-compose up
   ```

   > NOTE: You can run both commands at once with `docker-compose up --build`.

1. Verify that the service is working. 

   Open your web browser and type `http://localhost:1313` in your navigation bar,
   This opens a local instance of the docsy-example homepage. You can now make
   changes to the docsy example and those changes will immediately show up in your
   browser after you save.

### Cleanup

To stop Docker Compose, on your terminal window, press **Ctrl + C**. 

To remove the produced images run:

```console
docker-compose rm
```
For more information see the [Docker Compose
documentation](https://docs.docker.com/compose/gettingstarted/).

### Troubleshooting

As you run the website locally, you may run into the following error:

```
➜ hugo server

INFO 2021/01/21 21:07:55 Using config file: 
Building sites … INFO 2021/01/21 21:07:55 syncing static files to /
Built in 288 ms
Error: Error building site: TOCSS: failed to transform "scss/main.scss" (text/x-scss): resource "scss/scss/main.scss_9fadf33d895a46083cdd64396b57ef68" not found in file cache
```

This error occurs if you have not installed the extended version of Hugo.
See our [user guide](https://www.docsy.dev/docs/getting-started/) for instructions on how to install Hugo.

## Edit content

The website content is in `content/en` folder. It contains `.md` (Markdown) and `.html` (HTML) files.

### Layouts

To change the layout of any page edit `layouts/<page>/**.html`. If there is no layout for a given page at that location then copy the one provided by the theme and edit it:

     cp themes/docsy/layouts/<xyz> layouts/<xyz>

### Avro version

When a new version of Apache Avro is released:

1. Change the value of `params.avroversion` in `config.toml`
2. Add a new entry to the `Releases` pages in the `Blog` section, for example:
```
cp content/en/blog/releases/avro-1.10.2-released.md content/en/blog/releases/avro-1.11.0-released.md
```

### API documentation for C/C++/C# modules

The API documentations for C/C++/C# are built by their respective `build.sh dist` implementations. The final HTML should be copied to the `external` folder, for example:

    cp ../avro/build/avro-doc-1.12.0-SNAPSHOT/api/c/* content/en/docs/external/c/