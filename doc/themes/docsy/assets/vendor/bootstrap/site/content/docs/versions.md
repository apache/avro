---
layout: single
title: Versions
description: An appendix of hosted documentation for nearly every release of Bootstrap, from v1 through v4.
---

{{< list-versions.inline >}}
<div class="row">
  {{- range $release := sort (index $.Site.Data "docs-versions") "group" "desc" }}
  <div class="col-md-6 col-lg-4 col-xl mb-4">
    <h2>{{ $release.group }}</h2>
    <p>{{ $release.description }}</p>
    {{- $versions := sort $release.versions "v" "desc" -}}
    {{- range $i, $version := $versions }}
      {{- $len := len $versions -}}
      {{ if (eq $i 0) }}<div class="list-group">{{ end }}
        <a class="list-group-item list-group-item-action py-2 text-primary{{ if (eq $version.v $.Site.Params.docs_version) }} d-flex justify-content-between align-items-center{{ end }}" href="{{ $release.baseurl }}/{{ $version.v }}/">
          {{ $version.v }}
          {{ if (eq $version.v $.Site.Params.docs_version) -}}
          <span class="badge badge-primary">Latest</span>
          {{- end }}
        </a>
      {{ if (eq (add $i 1) $len) }}</div>{{ end }}
    {{ end -}}
  </div>
  {{ end -}}
</div>
{{< /list-versions.inline >}}
