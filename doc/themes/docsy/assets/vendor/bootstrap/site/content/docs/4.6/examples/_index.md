---
layout: single
title: Examples
description: Quickly get a project started with any of our examples ranging from using parts of the framework to custom components and layouts.
aliases: "/examples/"
---

{{< list-examples.inline >}}
{{ range $entry := $.Site.Data.examples -}}
  <h2>{{ $entry.category }}</h2>
  <p>{{ $entry.description }}</p>

  {{ range $i, $example := $entry.examples -}}
    {{- $len := len $entry.examples -}}
    {{ if (eq $i 0) }}<div class="row">{{ end }}
      <div class="col-sm-6 col-md-4 col-xl-3 mb-3">
        <a href="/docs/{{ $.Site.Params.docs_version }}/examples/{{ $example.name | urlize }}/">
          <img class="img-thumbnail mb-3" srcset="/docs/{{ $.Site.Params.docs_version }}/assets/img/examples/{{ $example.name | urlize }}.png,
                                                  /docs/{{ $.Site.Params.docs_version }}/assets/img/examples/{{ $example.name | urlize }}@2x.png 2x"
                                          src="/docs/{{ $.Site.Params.docs_version }}/assets/img/examples/{{ $example.name | urlize }}.png"
                                          alt=""
                                          width="480" height="300"
                                          loading="lazy">
          <h3 class="h5 mb-1">{{ $example.name }}</h3>
        </a>
        <p class="text-muted">{{ $example.description }}</p>
      </div>
    {{ if (eq (add $i 1) $len) }}</div>{{ end }}
  {{ end -}}
{{ end -}}
{{< /list-examples.inline >}}
