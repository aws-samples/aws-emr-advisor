package com.amazonaws.emr.report

import com.amazonaws.emr.utils.Formatter.humanReadableBytes

trait HtmlBase {

  val CssTableStyle = "table-bordered table-striped table-sm"

  /**
   * Generate an HTML code block
   *
   * @param text    command or code snippet text
   * @param code    language used to highlight properly the text
   * @param classes additional css class attributes
   */
  def htmlCodeBlock(text: String, code: String, classes: String = ""): String = {
    s"""
       |<div class="alert alert-light mt-2 w-100">
       |  <div class="highlight border mb-0 $classes">
       |    <pre class="mb-0"><code class="language-$code" data-lang="$code">$text</code></pre>
       |  </div>
       |</div>
       |""".stripMargin
  }

  /**
   * Generate a navbar with multiple content tabs.
   *
   * @param id   the HTML id of the navbar
   * @param tabs a sequence of T3 (id, name, content)
   */
  def htmlNavTabs(
    id: String,
    tabs: Seq[(String, String, String)],
    active: String,
    classesTab: String = "",
    classesContent: String = ""): String = {
    val navBarItems = tabs.map(x => s"""<button class="nav-link link-dark ${if (active.equalsIgnoreCase(x._1)) "active" else ""}" id="nav-${x._1}-tab" data-bs-toggle="tab" data-bs-target="#nav-${x._1}" type="button" role="tab" aria-controls="nav-${x._1}" aria-selected="${if (active.equalsIgnoreCase(x._1)) "true" else "false"}">${x._2}</button>""").mkString
    val navBar = s"""<nav><div class="nav $classesTab" id="${id}Tab" role="tablist">$navBarItems</div></nav>"""
    val contentItems = tabs.map(x => s"""<div class="tab-pane fade ${if (active.equalsIgnoreCase(x._1)) "show active" else ""}" id="nav-${x._1}" role="tabpanel" aria-labelledby="nav-${x._1}-tab">${x._3}</div>""").mkString
    val content = s"""<div class="tab-content $classesContent" id="nav-${id}TabContent">$contentItems</div>"""
    s"""$navBar $content"""
  }

  /**
   * Generate a Group list with 2 floated text elements for each item
   *
   * @param items   list of T2 (String, String) where first element is left text and second is right text
   * @param classes additional css classes to apply the group list
   */
  def htmlGroupListWithFloat(items: Seq[(String, String)], groupClass: String = "", innerClass: String = ""): String = {
    val listItems = items.map(x => s"""<li class="list-group-item $innerClass"><div class="float-start">${x._1}</div><div class="float-end">${x._2}</div></li>""").mkString
    s"""<ul class="list-group $groupClass">$listItems</ul>"""
  }

  /**
   * Generate a Group list
   *
   * @param items   list of T2 (String, String) where first element is left text and second is right text
   * @param classes additional css classes to apply the group list
   */
  def htmlGroupList(items: Seq[String], groupClass: String = "", innerClass: String = ""): String = {
    val listItems = items.map(i => s"""<li class="list-group-item $innerClass">$i</li>""").mkString
    s"""<ul class="list-group $groupClass">$listItems</ul>"""
  }

  /**
   * Generate an HTML Table
   *
   * @param header  optional list of string to build header row
   * @param rows    rows to add in the table
   * @param classes additional css classes to apply
   */
  def htmlTable(header: List[String], rows: List[List[String]], classes: String = ""): String = {
    val htmlHeader = if (header.nonEmpty) header.map(x => s"<th>$x</th>").mkString("<tr>", "", "</tr>") else ""
    val htmlRows = rows.map { r =>
      val row = r.map(c => s"""<td class="align-middle">$c</td>""").mkString
      s"""<tr>$row</tr>"""
    }.mkString
    s"""<table class="table $classes">$htmlHeader $htmlRows</table>"""
  }

  def htmlTablePaginated(id: String, header: List[String], rows: List[List[String]], classes: String = "", durationCols: List[String] = Nil): String = {
    val htmlHeader = if (header.nonEmpty) header.map(x => s"<th>$x</th>").mkString("<tr>", "", "</tr>") else ""
    val htmlRows = rows.map { r =>
      val row = r.map(c => s"""<td class="align-middle">$c</td>""").mkString
      s"""<tr>$row</tr>"""
    }.mkString

    s"""<table id="$id" class="table display $classes">
       |  <thead>$htmlHeader</thead>
       |  <tbody>$htmlRows</tbody>
       |</table>
       |
       |<!-- DataTable plugin -->
       |<script src="https://code.jquery.com/jquery-3.7.0.min.js" integrity="sha256-2Pmvv0kuTBOenSvLm6bvfBSSHrUJ+3A7x6P5Ebd07/g=" crossorigin="anonymous"></script>
       |<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js" crossorigin="anonymous"></script>
       |<script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"crossorigin="anonymous"></script>
       |
       |<script>
       |
       |function msToDuration(millis) {
       |    millis = Number(millis);
       |    var d = Math.floor(millis / (3600*24*1000));
       |    var h = Math.floor(millis % (3600*24*1000) / (3600*1000));
       |    var m = Math.floor(millis % (3600*1000) / (60*1000));
       |    var s = Math.floor(millis % (60*1000)/1000);
       |    var ms = millis % 1000;
       |
       |    var dStr = d > 0 ? d + "d" : "";
       |    var hStr = h > 0 ? h + "h" : "";
       |    var mStr = m > 0 ? m + "m" : "";
       |    var sStr = s > 0 ? s + "s" : "";
       |    var msStr = ms > 0 ? ms + "ms" : "";
       |
       |    if(d > 0) {
       |        return dStr + " " + hStr;
       |    } else if (h > 0) {
       |        return hStr + " " + mStr;
       |    } else if (m > 0) {
       |        return mStr + " " + sStr;
       |    } else if (s > 0) {
       |        return sStr;
       |    } else {
       |        return msStr;
       |    }
       |}
       |
       |  new DataTable('#$id', {
       |    columns: [
       |      ${header.map(c => if (durationCols.contains(c)) s"""{ "data": "$c", "render": function(data, type, row) {return msToDuration(data);} }""" else s"""{ "data": "$c" }""").mkString(",")}
       |    ]
       |  });
       |
       |</script>
       |""".stripMargin
  }

  /** Create a warning / alert visual that can be hidden / closed */
  def htmlBox(text: String, role: String): String = {
    s"""<svg xmlns="http://www.w3.org/2000/svg" style="display: none;">
       |  <symbol id="check-circle-fill" viewBox="0 0 16 16">
       |    <path d="M16 8A8 8 0 1 1 0 8a8 8 0 0 1 16 0zm-3.97-3.03a.75.75 0 0 0-1.08.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-.01-1.05z"/>
       |  </symbol>
       |  <symbol id="info-fill" viewBox="0 0 16 16">
       |    <path d="M8 16A8 8 0 1 0 8 0a8 8 0 0 0 0 16zm.93-9.412-1 4.705c-.07.34.029.533.304.533.194 0 .487-.07.686-.246l-.088.416c-.287.346-.92.598-1.465.598-.703 0-1.002-.422-.808-1.319l.738-3.468c.064-.293.006-.399-.287-.47l-.451-.081.082-.381 2.29-.287zM8 5.5a1 1 0 1 1 0-2 1 1 0 0 1 0 2z"/>
       |  </symbol>
       |  <symbol id="exclamation-triangle-fill" viewBox="0 0 16 16">
       |    <path d="M8.982 1.566a1.13 1.13 0 0 0-1.96 0L.165 13.233c-.457.778.091 1.767.98 1.767h13.713c.889 0 1.438-.99.98-1.767L8.982 1.566zM8 5c.535 0 .954.462.9.995l-.35 3.507a.552.552 0 0 1-1.1 0L7.1 5.995A.905.905 0 0 1 8 5zm.002 6a1 1 0 1 1 0 2 1 1 0 0 1 0-2z"/>
       |  </symbol>
       |</svg>
       |
       |<div class="alert alert-$role d-flex align-items-center" role="alert">
       |  <svg class="bi flex-shrink-0 me-2" role="img" aria-label="Info:"><use xlink:href="#info-fill"/></svg>
       |  <div>
       |    $text
       |  </div>
       |</div>
       |""".stripMargin
  }

  /**
   * Generate a graph to display Executors runtime using different counts
   *
   * @param id          HTML id of the object
   * @param recommended recommended value for max running executors
   * @param x           List[Int] of x-axis data
   * @param y           List[Int] of y-axis data
   */
  def htmlSimulationGraph(id: String, recommended: Int, x: List[Int], y: List[Int], vh: Int): String = {
    val ctxName = id.replaceAll("-", "")
    s"""
       |<div class="chart-container" style="position: relative; height: ${vh}vh; width: 100%;">
       |  <canvas id="$id"></canvas>
       |</div>
       |
       |<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
       |<script src=" https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.0.1/dist/chartjs-plugin-annotation.min.js "></script>
       |<script>
       |  const $ctxName = document.getElementById('$id');
       |
       |  new Chart($ctxName, {
       |    type: 'line',
       |    data: {
       |      labels: [${x.map(l => s"'$l'").mkString(",")}],
       |      datasets: [
       |        {
       |          label: 'Runtime',
       |          data: [${y.mkString(",")}],
       |          borderColor: '#ef6c00',
       |          fill: true,
       |          cubicInterpolationMode: 'monotone',
       |          tension: 0.4
       |        }
       |      ]
       |    },
       |    options: {
       |        responsive: true,
       |        maintainAspectRatio: false,
       |        plugins: {
       |          title: {
       |            display: true,
       |            text: 'Estimated application runtime'
       |          },
       |          legend: {
       |            display: false,
       |              labels: {
       |                display: false
       |              }
       |          },
       |          annotation: {
       |            annotations: [
       |              {
       |                type: "line",
       |                mode: "vertical",
       |                scaleID: "x",
       |                value: '$recommended',
       |                borderColor: "red",
       |                borderWidth: 2,
       |                label: {
       |                  content: "Suggested",
       |                  display: true,
       |                  position: "start"
       |                }
       |              },
       |            ]
       |          },
       |          tooltip: {
       |            callbacks: {
       |              label: function(context) {
       |                let label = context.dataset.label || '';
       |                if (label) {
       |                    label += ': ';
       |                }
       |                if (context.parsed.y !== null) {
       |                    label += secondsToDuration(context.parsed.y);
       |                }
       |                return label;
       |              },
       |              title: function(context) {
       |                return "Executor Count: " + context[0].label;
       |              }
       |            }
       |          }
       |        },
       |        interaction: {
       |          intersect: false,
       |        },
       |        scales: {
       |          x: {
       |            display: true,
       |            title: {
       |              display: true,
       |              text: 'Executors Count'
       |            }
       |          },
       |          y: {
       |            display: true,
       |            ticks: {
       |              callback: function(v) { return secondsToDuration(v) }
       |            },
       |            title: {
       |              display: true,
       |              text: 'Execution Time'
       |            },
       |            suggestedMin: 0,
       |            suggestedMax: 200
       |          }
       |        }
       |      },
       |  });
       |
       |  function secondsToDuration(seconds) {
       |      seconds = Number(seconds);
       |      var d = Math.floor(seconds / (3600*24));
       |      var h = Math.floor(seconds % (3600*24) / 3600);
       |      var m = Math.floor(seconds % 3600 / 60);
       |      var s = Math.floor(seconds % 60);
       |
       |      var dStr = d > 0 ? d + "d " : "";
       |      var hStr = h > 0 ? h + "h " : "";
       |      var mStr = m > 0 ? m + "m " : "";
       |      var sStr = s > 0 ? s + "s" : "";
       |
       |      if(d > 0) {
       |        return dStr + " " + hStr;
       |      } else if (h > 0) {
       |        return hStr + " " + mStr;
       |      } else if (m > 0) {
       |        return mStr + " " + sStr;
       |      } else {
       |        return sStr;
       |      }
       |  }
       |
       |  function epoch_to_hh_mm_ss(epoch) {
       |    return new Date(epoch*1000).toISOString().substr(12, 7)
       |  }
       |</script>
       |""".stripMargin
  }

  def htmlHardwareResources(cores: Int, memory: Long, storage: Long): String = {
    htmlGroupListWithFloat(Seq(
      (
        s"""
           |<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-cpu" viewBox="0 0 16 16">
           |  <path d="M5 0a.5.5 0 0 1 .5.5V2h1V.5a.5.5 0 0 1 1 0V2h1V.5a.5.5 0 0 1 1 0V2h1V.5a.5.5 0 0 1 1 0V2A2.5 2.5 0 0 1 14 4.5h1.5a.5.5 0 0 1 0 1H14v1h1.5a.5.5 0 0 1 0 1H14v1h1.5a.5.5 0 0 1 0 1H14v1h1.5a.5.5 0 0 1 0 1H14a2.5 2.5 0 0 1-2.5 2.5v1.5a.5.5 0 0 1-1 0V14h-1v1.5a.5.5 0 0 1-1 0V14h-1v1.5a.5.5 0 0 1-1 0V14h-1v1.5a.5.5 0 0 1-1 0V14A2.5 2.5 0 0 1 2 11.5H.5a.5.5 0 0 1 0-1H2v-1H.5a.5.5 0 0 1 0-1H2v-1H.5a.5.5 0 0 1 0-1H2v-1H.5a.5.5 0 0 1 0-1H2A2.5 2.5 0 0 1 4.5 2V.5A.5.5 0 0 1 5 0zm-.5 3A1.5 1.5 0 0 0 3 4.5v7A1.5 1.5 0 0 0 4.5 13h7a1.5 1.5 0 0 0 1.5-1.5v-7A1.5 1.5 0 0 0 11.5 3h-7zM5 6.5A1.5 1.5 0 0 1 6.5 5h3A1.5 1.5 0 0 1 11 6.5v3A1.5 1.5 0 0 1 9.5 11h-3A1.5 1.5 0 0 1 5 9.5v-3zM6.5 6a.5.5 0 0 0-.5.5v3a.5.5 0 0 0 .5.5h3a.5.5 0 0 0 .5-.5v-3a.5.5 0 0 0-.5-.5h-3z"/>
           |</svg>
           |<b>cores</b>
           |""".stripMargin, s"$cores"),
      (
        s"""
           |<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-memory" viewBox="0 0 16 16">
           |  <path d="M1 3a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h4.586a1 1 0 0 0 .707-.293l.353-.353a.5.5 0 0 1 .708 0l.353.353a1 1 0 0 0 .707.293H15a1 1 0 0 0 1-1V4a1 1 0 0 0-1-1H1Zm.5 1h3a.5.5 0 0 1 .5.5v4a.5.5 0 0 1-.5.5h-3a.5.5 0 0 1-.5-.5v-4a.5.5 0 0 1 .5-.5Zm5 0h3a.5.5 0 0 1 .5.5v4a.5.5 0 0 1-.5.5h-3a.5.5 0 0 1-.5-.5v-4a.5.5 0 0 1 .5-.5Zm4.5.5a.5.5 0 0 1 .5-.5h3a.5.5 0 0 1 .5.5v4a.5.5 0 0 1-.5.5h-3a.5.5 0 0 1-.5-.5v-4ZM2 10v2H1v-2h1Zm2 0v2H3v-2h1Zm2 0v2H5v-2h1Zm3 0v2H8v-2h1Zm2 0v2h-1v-2h1Zm2 0v2h-1v-2h1Zm2 0v2h-1v-2h1Z"/>
           |</svg>
           |<b>memory</b>
           |""".stripMargin, s"${humanReadableBytes(memory)}"),
      (
        s"""
           |<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-hdd" viewBox="0 0 16 16">
           |  <path d="M4.5 11a.5.5 0 1 0 0-1 .5.5 0 0 0 0 1zM3 10.5a.5.5 0 1 1-1 0 .5.5 0 0 1 1 0z"/>
           |  <path d="M16 11a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V9.51c0-.418.105-.83.305-1.197l2.472-4.531A1.5 1.5 0 0 1 4.094 3h7.812a1.5 1.5 0 0 1 1.317.782l2.472 4.53c.2.368.305.78.305 1.198V11zM3.655 4.26 1.592 8.043C1.724 8.014 1.86 8 2 8h12c.14 0 .276.014.408.042L12.345 4.26a.5.5 0 0 0-.439-.26H4.094a.5.5 0 0 0-.44.26zM1 10v1a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-1a1 1 0 0 0-1-1H2a1 1 0 0 0-1 1z"/>
           |</svg>
           |<b>storage</b>
           |""".stripMargin, s"${humanReadableBytes(storage)}")
    ))
  }

  def htmlBoxAlert(text: String): String = htmlBox(text, "danger")

  def htmlBoxInfo(text: String): String = htmlBox(text, "secondary")

  def htmlBoxNote(text: String): String = htmlBox(text, "primary")

  def htmlBoxSuccess(text: String): String = htmlBox(text, "success")

  def htmlTextInfo(text: String): String =
    s"""<span class="badge alert alert-secondary py-1 px-3 m-0">Info</span> $text"""

  def htmlTextOk(text: String): String =
    s"""<span class="badge text-bg-success py-1 px-3 m-0">Ok</span> $text"""

  def htmlTextIssue(text: String): String =
    s"""<span class="badge text-bg-danger py-1 px-3 m-0">Issue</span> $text"""

  def htmlTextWarning(text: String): String =
    s"""<span class="badge text-bg-warning py-1 px-3 m-0">Warning</span> $text"""

  def htmlTextSmall(text: String): String =
    s"""<small class="text-muted">$text</small>"""

  def htmlLink(text: String, url: String): String =
    s"""<a href="$url" target="_blank">$text</a>"""

  def htmlTextRed(text: String) =
    s"""<span class="text-danger font-weight-bold">$text</span>"""

  def htmlBold(text: String) =
    s"""<b>$text</b>"""

}
