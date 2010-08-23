/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
/* Graph functions for Avro tracing */

function max(one, two) {
  if (one > two) {
    return one;
  }
  else {
    return two;
  }
}

function makeLatencyChart(data) {
  var types = ["prelink", "cpu", "postlink"];
  var totals = data.map(function(d) {return (d.prelink + d.postlink + d.cpu) });
  
  /* Sizing and scales. */
  var w = 400,
      h = 200,
      xmax = data.length - 1,
      ymax = pv.max(totals) / 1000000,
      x = pv.Scale.linear(0, xmax).range(0, w),
      y = pv.Scale.linear(0, ymax).range(0, h);
  /* The root panel. */
  var vis = new pv.Panel()
      .width(w)
      .height(h)
      .bottom(20)
      .left(40)
      .right(10)
      .top(5);
  
  /* X-axis and ticks. */
  vis.add(pv.Rule)
      .data(x.ticks())
      .visible(function(d) d)
      .left(x)
      .bottom(-5)
      .height(5)
    .anchor("bottom").add(pv.Label)
      .text(x.tickFormat);

  /* The stack layout. */
  vis.add(pv.Layout.Stack)
      .layers(types)
      .values(data)
      .x(function(d) x(d.index))
      .y(function(d, p) y(max(d[p] / 1000000, .001)))
    .layer.add(pv.Area);

  /* Y-axis and ticks. */
  vis.add(pv.Rule)
     .data(y.ticks(5))
     .bottom(y)
     .strokeStyle(function(d) d ? "rgba(128,128,128,.2)" : "#000")
     .anchor("left").add(pv.Label)
     .text(y.tickFormat);
     
  /* Use an invisible panel to capture pan & zoom events. */
  vis.add(pv.Panel)
      .events("all")
      .event("mousewheel", pv.Behavior.zoom())
      .event("zoom", transform);
  
  /** Update the y-scale domains per the new transform. */
  function transform() {
    var t = this.transform().invert();
    y.domain(0, ymax / (1 - t.y/50));
    vis.add(pv.Rule)
     .data(y.ticks(5))
     .bottom(y)
     .strokeStyle(function(d) d ? "rgba(128,128,128,.2)" : "#000")
     .anchor("left").add(pv.Label)
     .text(y.tickFormat);
    vis.render();
  }
     
  vis.render();
}

function makePayloadChart(data) {
  var yVals = data.map(function(d) {return (d.y) });
  
  /* Sizing and scales. */
  var w = 400,
      h = 200,
      ymax = pv.max(yVals),
      x = pv.Scale.linear(data, function(d) d.x).range(0, w),
      y = pv.Scale.linear(0, ymax).range(0, h);
  
  /* The root panel. */
  var vis = new pv.Panel()
      .width(w)
      .height(h)
      .bottom(20)
      .left(40)
      .right(10)
      .top(5);
  
  /* Y-axis and ticks. */
  vis.add(pv.Rule)
      .data(y.ticks(5))
      .bottom(y)
      .strokeStyle(function(d) d ? "#eee" : "#000")
    .anchor("left").add(pv.Label)
      .text(y.tickFormat);
  
  /* X-axis and ticks. */
  vis.add(pv.Rule)
      .data(x.ticks())
      .visible(function(d) d)
      .left(x)
      .bottom(-5)
      .height(5)
    .anchor("bottom").add(pv.Label)
      .text(x.tickFormat);
  
  /* The area with top line. */
  vis.add(pv.Area)
      .data(data)
      .bottom(1)
      .left(function(d) x(d.x))
      .height(function(d) y(d.y))
      .fillStyle("rgb(121,173,210)")
    .anchor("top").add(pv.Line)
      .lineWidth(3);

  /* Use an invisible panel to capture pan & zoom events. */
  vis.add(pv.Panel)
      .events("all")
      .event("mousewheel", pv.Behavior.zoom())
      .event("zoom", transform);
  
  /** Update the y-scale domains per the new transform. */
  function transform() {
    var t = this.transform().invert();
    //alert(t.y);
    y.domain(0, ymax / (1 - t.y/50));
    vis.add(pv.Rule)
     .data(y.ticks(5))
     .bottom(y)
     .strokeStyle(function(d) d ? "rgba(128,128,128,.2)" : "#000")
     .anchor("left").add(pv.Label)
     .text(y.tickFormat);
    vis.render();
  }
  
  vis.render();
}