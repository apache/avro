---
title: "Repository Links"
linkTitle: "Repository Links"
weight: 9
description: >
  Help your users interact with your source repository.
---

The Docsy [docs and blog layouts](/docs/adding-content/content/#adding-docs-and-blog-posts) include links for readers to edit the page or create issues for your docs or project via your site's source repository. The current generated links for each docs or blog page are:

* **Edit this page**: Brings the user to an editable version of the page content in their fork (if available) of your docs repo. If the user doesn't have a current fork of your docs repo, they are invited to create one before making their edit. The user can then create a pull request for your docs.
* **Create child page**: Brings the user to a create new file form in their fork of your docs repo.  The new file will be located as a child of the page they clicked the link on.  The form will be pre-populated with a template the user can edit to create their page.  You can change this by adding `assets/stubs/new-page-template.md` to your own project.
* **Create documentation issue**: Brings the user to a new issue form in your docs repo with the name of the current page as the issue's title.
* **Create project issue** (optional): Brings the user to a new issue form in your project repo. This can be useful if you have separate project and docs repos and your users want to file issues against the project feature being discussed rather than your docs.

This page shows you how to configure these links using your `config.toml` file.

Currently Docsy supports only GitHub repository links "out of the box". If you are using another repository such as Bitbucket and would like generated repository links, feel free to [add a feature request or update our theme](/docs/contribution-guidelines/).

## Link configuration

There are four variables you can configure in `config.toml` to set up links, as well as one in your page metadata.

### `github_repo`

The URL for your site's source repository. This is used to generate the **Edit this page**, **Create child page**, and **Create documentation issue** links.

```toml
github_repo = "https://github.com/google/docsy"
```

### `github_subdir` (optional)

Specify a value here if your content directory is not in your repo's root directory. For example, this site is in the `userguide` subdirectory of its repo. Setting this value means that your edit links will go to the right page.

```toml
github_subdir = "userguide"
```

### `github_project_repo` (optional)

Specify a value here if you have a separate project repo and you'd like your users to be able to create issues against your project from the relevant docs. The **Create project issue** link appears only if this is set.

```toml
github_project_repo = "https://github.com/google/docsy"
```

### `github_branch` (optional)

Specify a value here if you have would like to reference a different branch for the other github settings like **Edit this page** or **Create project issue**.

```toml
github_branch = "release"
```

### `github_url` (optional)

Specify a value for this **in your page metadata** to set a specific edit URL for this page, as in the following example:

```yaml
---
title: "Example Page"
linkTitle: "Example Page"
date: 2017-01-05
github_url: "https://github.com/MyUsername/myrepo/edit/main/README.md"
description: >
  An example page.
---
```

This can be useful if you have page source files in multiple Git repositories, or require a non-GitHub URL.  Pages using this value have **Edit this page** links only.

