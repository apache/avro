---
title: "Previews and Deployment"
linkTitle: "Previews and Deployment"
weight: 7
description: >
  Deploying your Docsy site.
---

There are multiple possible options for deploying a Hugo site, including Netlify, Firebase Hosting, Bitbucket with Aerobatic, and more; you can read about them all in [Hosting and Deployment](https://gohugo.io/hosting-and-deployment/). Hugo also makes it easy to deploy your site locally for quick previews of your content.

## Serving your site locally

Depending on your deployment choice you may want to serve your site locally during development to preview content changes. To serve your site locally:

1.  Ensure you have an up to date local copy of your site files cloned from your repo. Don't forget to use `--recurse-submodules` or you won't pull down some of the code you need to generate a working site.

    ```
    git clone --recurse-submodules --depth 1 https://github.com/my/example.git
    ```
   
    {{% alert title="Note" color="primary" %}}
If you've just added the theme as a submodule in a local version of your site and haven't committed it to a repo yet,  you must get local copies of the theme's own submodules before serving your site.
    
    git submodule update --init --recursive
    {{% /alert %}}

1.  Ensure you have the tools described in [Prerequisites and installation](/docs/getting-started/#prerequisites-and-installation) installed on your local machine, including `postcss-cli` (you'll need it to generate the site resources the first time you run the server).
1.  Run the `hugo server` command in your site root. By default your site will be available at http://localhost:1313/.

Now that you're serving your site locally, Hugo will watch for changes to the content and automatically refresh your site. If you have more than one local git branch, when you switch between git branches the local website reflects the files in the current branch.

## Build environments and indexing

By default, Hugo sites built with `hugo` (rather than served locally with `hugo server`) have the Hugo build environment `production`. Deployed Docsy sites with `production` builds can be indexed by search engines, including [Google Custom Search Engines](/docs/adding-content/navigation/#configure-search-with-a-google-custom-search-engine). Production builds also have optimized JavaScript and CSS for live deployment (for example, minified JS rather than the more legible original source).

If you do not want your deployed site to be indexed by search engines (for example if you are still developing your live site), or if you want to build a development version of your site for offline analysis, you can set your Hugo build environment to something else such as `development` (the default for local deploys with `hugo server`), `test`, or another environment name of your choice.

The simplest way to set this is by using the `-e` flag when specifying or running your `hugo` command, as in the following example:

```
hugo -e development
```


## Deployment with Netlify

We recommend using [Netlify](https://www.netlify.com/) as a particularly simple way to serve your site from your Git provider (GitHub, GitLab, or BitBucket), with [continuous deployment](https://www.netlify.com/docs/continuous-deployment/), previews of the generated site when you or your users create pull requests against the doc repo, and more. Netlify is free to use for Open Source projects, with premium tiers if you require greater support.

Before deploying with Netlify, make sure that you've pushed your site source to your chosen GitHub (or other provider) repo, following any setup instructions in [Using the theme](/docs/getting-started/#using-the-theme).

Then follow the instructions in [Host on Netlify](https://gohugo.io/hosting-and-deployment/hosting-on-netlify/) to set up a Netlify account (if you don't have one already) and authorize access to your GitHub or other Git provider account. Once you're logged in:

1. Click **New site from Git**.
1. Click your chosen Git provider, then choose your site repo from your list of repos.
1. In the **Deploy settings** page:
   1. For your **Build command**, specify `cd themes/docsy && git submodule update -f --init && cd ../.. && hugo`. You need to specify this rather than just `hugo` so that Netlify can use the theme's submodules. If you don't want your site to be indexed by search engines, you can add an environment flag to specify a non-`production` environment, as described in [Build environments and indexing](/#build-environments-and-indexing).
   1. Click **Show advanced**. 
   1. In the **Advanced build settings** section, click **New variable**. 
   1. Specify `HUGO_VERSION` as the **Key** for the new variable, and `0.53` or later as its **Value**. 
1. Click **Deploy site**.

{{% alert title="Note" color="primary" %}}
Netlify uses your site repo's `package.json` file to install any JavaScript dependencies (like `postcss`) before building your site. If you haven't just copied our example site's version of this file, make sure that you've specified all our [prerequisites](/docs/getting-started/#install-postcss).

For example, if you want to use a version of `postcss-cli` later than version 8.0.0, you need to ensure that your `package.json` also specifies `postcss` separately:

```
  "devDependencies": {
    "autoprefixer": "^9.8.6",
    "postcss-cli": "^8.0.0",
    "postcss": "^8.0.0"
  }
```
{{% /alert %}}

Alternatively, you can follow the same instructions but specify your **Deploy settings** in a [`netlify.toml` file](https://docs.netlify.com/configure-builds/file-based-configuration/) in your repo rather than in the **Deploy settings** page. You can see an example of this in the [Docsy theme repo](https://github.com/google/docsy/blob/master/netlify.toml) (though note that the build command here is a little unusual because the Docsy user guide is *inside* the theme repo).

If you have an existing deployment you can view and update the relevant information by selecting the site from your list of sites in Netlify, then clicking **Site settings** - **Build and deploy**. Ensure that **Ubuntu Xenial 16.04** is selected in the **Build image selection** section - if you're creating a new deployment this is used by default. You need to use this image to run the extended version of Hugo.

