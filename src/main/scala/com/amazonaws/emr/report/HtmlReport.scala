package com.amazonaws.emr.report

import com.amazonaws.emr.Config.{AppName, AppNamePrefix, ResourceAppLogoPath}
import com.amazonaws.emr.api.AwsUtils
import com.amazonaws.emr.utils.Constants.{LinkEmrAdvisor, LinkEmrBestPractice, ParamBucket}

import java.io.{PrintWriter, StringWriter}
import java.nio.file.{FileAlreadyExistsException, Files, Paths}
import java.util.Base64

trait HtmlReport extends HtmlBase {

  def title: String

  def pages: List[HtmlPage]

  def reportName: String

  def reportPrefix: String

  private val reportFileName: String = s"$AppNamePrefix.$reportPrefix.${System.currentTimeMillis()}.html"

  def render: String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    pw.write(html())
    pw.close()
    sw.toString
  }

  def save(options: Map[String, String]): Unit = {

    val htmlReportPath = s"/tmp/$reportFileName"
    val directoryPath = Paths.get(htmlReportPath).getParent.toAbsolutePath

    try {
      Files.createDirectories(directoryPath)
    } catch {

      case _: FileAlreadyExistsException =>
        println(s"Directory already exists: $directoryPath")

      case e: Throwable => e.printStackTrace()
    }

    // store report locally
    println(s"Saving html report in $htmlReportPath")
    new PrintWriter(htmlReportPath) {
      write(render)
      close()
    }

    // store report on amazon s3
    val s3BucketPath = options.getOrElse(ParamBucket.name, "")
    val finalPath = if (s3BucketPath.nonEmpty) {

      def removeS3Prefix(s: String): String = {
        s.replaceAll("(?i)^s3a?n?://", "")
      }

      def parseS3Path(path: String): (String, String) = {
        val tmpS3Path = removeS3Prefix(path)
        if (tmpS3Path.contains("/")) {
          val parts = tmpS3Path.split("/", 2)
          val prefix = if (parts(1).isEmpty) AppNamePrefix else parts(1)
          (parts(0), prefix.stripSuffix("/"))
        } else {
          (tmpS3Path, AppNamePrefix)
        }
      }

      val (bucketName: String, bucketPrefix: String) = parseS3Path(s3BucketPath)

      println(s"Saving html report in s3://$bucketName/$bucketPrefix/$reportFileName")
      AwsUtils.putS3Object(bucketName, s"$bucketPrefix/$reportFileName", htmlReportPath)
      AwsUtils.getS3ObjectPreSigned(bucketName, s"$bucketPrefix/$reportFileName")
    } else s"file://$htmlReportPath"

    println(
      s"""
         |To access the report, open the following url in your browser:
         |
         | - $finalPath
         |
         |""".stripMargin
    )

  }

  private val CssBase: String = {
    s"""
       |  :root {
       |    --primary-color: #ff9100;
       |  }
       |
       |  .primary-color {
       |    color: #ff9100;
       |  }
       |
       |  .bi {
       |    display: inline-block;
       |    width: 1rem;
       |    height: 1rem;
       |  }
       |
       |  /* Sidebar */
       |  @media (min-width: 768px) {
       |    .sidebar .offcanvas-lg {
       |      position: -webkit-sticky;
       |      position: sticky;
       |      top: 48px;
       |    }
       |    .navbar-search {
       |      display: block;
       |    }
       |  }
       |
       |  .sidebar {
       |    height: 500px;
       |  }
       |
       |  .sidebar .nav-link {
       |    font-size: .875rem;
       |    font-weight: 500;
       |  }
       |
       |  .sidebar .nav-link.active {
       |    color: #2470dc;
       |  }
       |
       |  .sidebar-heading {
       |    font-size: .75rem;
       |  }
       |
       |  /* Navbar */
       |  .navbar-brand {
       |    padding-top: .2rem;
       |    padding-bottom: .2rem;
       |    background-color: rgba(0, 0, 0, .25);
       |    box-shadow: inset -1px 0 0 rgba(0, 0, 0, .25);
       |  }
       |  .navbar-brand:hover {
       |    color: var(--primary-color) ! important;
       |  }
       |
       |  .navbar .form-control {
       |    padding: .75rem 1rem;
       |  }
       |
       |  /* Sub Nav */
       |  .nav-pills .nav-link.active, .nav-pills .show > .nav-link {
       |    background-color: transparent !IMPORTANT;
       |    font-weight: bold;
       |  }
       |
       |  #sidebarMenu {
       |
       |    a:hover {
       |      color: var(--primary-color) ! important;
       |    }
       |
       |    a.active {
       |      font-weight: bold ! important;
       |      color: var(--primary-color) ! important;
       |    }
       |
       |  }
       |
       |  .nav-link:focus, .nav-link:hover {
       |    color: var(--primary-color) !important;
       |  }
       |
       |  tbody tr td:first-child {
       |    padding: 2px 6px;
       |    width: 15em;
       |    min-width: 15em;
       |    max-width: 15em;
       |    word-break: break-all;
       |  }
       |
       |  #pills-tabContent .nav.nav-pills {
       |    .nav-link {
       |      font-size: .875rem;
       |    }
       |
       |    .nav-link.active {
       |      font-weight: bold ! important;
       |      color: var(--primary-color) ! important;
       |    }
       |  }
       |
       |
       |""".stripMargin
  }

  protected val CssCustom = ""

  def html(): String = {
    s"""
       |<!doctype html>
       |<html lang="en" data-bs-theme="light" class=" emnkehl idc0_350">
       |  ${head()}
       |  ${body()}
       |</html>
       |""".stripMargin
  }

  def head(): String = {
    s"""
       |<!-- Head Section -->
       |<head>
       |  <meta charset="utf-8">
       |  <meta name="description" content="">
       |  <meta name="author" content="ripani">
       |  <meta name="viewport" content="width=device-width, initial-scale=1">
       |  <title>$title</title>
       |
       |  <!-- Bootstrap -->
       |  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
       |        integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH"
       |        rel="stylesheet" crossorigin="anonymous">
       |  <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"
       |        rel="stylesheet" crossorigin="anonymous">
       |
       |  <!-- TODO - Verify -->
       |  <link href="https://getbootstrap.com/docs/5.2/assets/css/docs.css" rel="stylesheet">
       |  <link href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css"
       |        rel="stylesheet" crossorigin="anonymous">
       |  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css"
       |        integrity="sha384-rbsA2VBKQhggwzxH7pPCaAqO46MgnOM80zW1RWuH61DGLwZJEdK2Kadq2F9CUG65"
       |        rel="stylesheet" crossorigin="anonymous">
       |  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/js/bootstrap.bundle.min.js"
       |          integrity="sha384-kenU1KFdBIe4zVF0s0G1M5b4hcpxyD9F7jL+jjXkk+Q2h455rYXK/7HAuoJl+0I4"
       |          crossorigin="anonymous"></script>
       |
       |  <style>
       |    $CssBase
       |    $CssCustom
       |  </style>
       |
       |</head>
       |""".stripMargin
  }

  def body(): String = {
    s"""
       |<!-- Body -->
       |<body>
       |  ${header()}
       |  ${content()}
       |  ${scripts()}
       |</body>
       |""".stripMargin
  }

  protected def scripts(): String = {
    s"""
       |<!-- Bootstrap -->
       |<script src="https://cdn.jsdelivr.net/npm/@popperjs/core@2.11.8/dist/umd/popper.min.js"
       |        integrity="sha384-I7E8VVD/ismYTF4hNIPjVp/Zjvgyol6VFvRkX/vR+Vc4jQkC+hVqc2pM8ODewa9r"
       |        crossorigin="anonymous"></script>
       |<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.min.js"
       |        integrity="sha384-0pUGZvbkm6XF6gxjEnlmuGrJXVbNuzT9qBBavbLwCsOGabYfZo0T0to5eqruptLy"
       |        crossorigin="anonymous"></script>
       |
       |<!-- JQuery -->
       |<script src="https://code.jquery.com/jquery-3.3.1.slim.min.js"
       |        integrity="sha384-q8i/X+965DzO0rT7abK41JStQIAqVgRVzpbzo5smXKp4YfRvH+8abtTE1Pi6jizo"
       |        crossorigin="anonymous"></script>
       |
       |<!-- Optional -->
       |<script src="https://cdn.jsdelivr.net/npm/chart.js@4.3.2/dist/chart.umd.js"
       |        integrity="sha384-eI7PSr3L1XLISH8JdDII5YN/njoSsxfbrkCTnJrzXt+ENP5MOVBxD+l6sEG4zoLp"
       |        crossorigin="anonymous"></script>
       |""".stripMargin
  }

  def header(): String = {
    val imgBinary = getClass.getResourceAsStream(ResourceAppLogoPath).readAllBytes()
    val imgBase64 = Base64.getEncoder.encodeToString(imgBinary)

    s"""
       |<!-- Header -->
       |<header class="navbar sticky-top bg-dark flex-md-nowrap p-0 shadow" data-bs-theme="dark">
       |  <a class="navbar-brand col-md-3 col-lg-1 me-0 p-1 fs-6" href="#">
       |    <img src="data:image/png;base64,$imgBase64" width="40" height="40" class="float-start">
       |    <div class="pt-2">$AppName</div>
       |  </a>
       |  <div class="w-100">
       |    <div class="d-flex bd-highlight">
       |      <div class="mr-auto px-4 my-2 fs-5 bd-highlight">
       |        <span class="primary-color">$reportName</span>
       |        <span class="text-white">/</span>
       |        <span class="primary-color">$title</span>
       |      </div>
       |      <div class="p-2 bd-highlight" style="margin-left: auto;">
       |        <a href="$LinkEmrBestPractice" class="nav-link text-secondary" target="_blank"
       |           title="EMR Best Practice - Github">
       |          <i class="bi bi-file-text-fill"></i>
       |        </a>
       |      </div>
       |      <div class="p-2 bd-highlight" style="padding-right: 45px !important;">
       |        <a href="$LinkEmrAdvisor" class="nav-link text-secondary" target="_blank"
       |           title="EMR Advisor - Github">
       |          <i class="bi bi-github"></i>
       |        </a>
       |      </div>
       |    </div>
       |  </div>
       |</header>
       |""".stripMargin
  }

  def content(): String = {

    val baseNavLinks = pages
      .filter(_.subSection.isEmpty)
      .map(p => NavLink(p.pageName, p.pageId, p.pageIcon, p.isActive))
    val groupedLinks = pages
      .filter(_.subSection.nonEmpty)
      .groupBy(page => page.subSection)

    val groupedNavLinks = groupedLinks.map { e =>
      val subLinks = e._2.map(p => NavLink(p.pageName, p.pageId, p.pageIcon, p.isActive))
      s"""
         |  <h6 class="sidebar-heading d-flex justify-content-between align-items-center px-2 mt-4 mb-1 text-body-secondary text-uppercase">
         |    <span>${e._1}</span>
         |  </h6>
         |  ${subLinks.mkString}
         |""".stripMargin
    }

    s"""
       |  <!-- Content -->
       |  <div class="container-fluid">
       |    <div class="row">
       |      <div class="sidebar border border-right col-md-3 col-lg-1 p-0 bg-body-tertiary">
       |        <div class="offcanvas-md offcanvas-end bg-body-tertiary" tabindex="-1"
       |             id="sidebarMenu" aria-labelledby="sidebarMenuLabel">
       |          <div class="offcanvas-header">
       |            <h5 class="offcanvas-title" id="sidebarMenuLabel">Company name</h5>
       |            <button type="button" class="btn-close" data-bs-dismiss="offcanvas"
       |                    data-bs-target="#sidebarMenu" aria-label="Close"></button>
       |          </div>
       |          <div class="offcanvas-body d-md-flex flex-column p-0 pt-lg-3 overflow-y-auto">
       |            <ul class="nav flex-column">
       |              ${baseNavLinks.mkString}
       |              ${groupedNavLinks.mkString}
       |            </ul>
       |          </div>
       |        </div>
       |      </div>
       |
       |      <main class="col-md-9 ms-sm-auto col-lg-11 px-md-4">
       |        <div class="d-flex justify-content-between flex-wrap flex-md-nowrap align-items-center pt-3 pb-2 mb-3">
       |          <div class="tab-content w-100" id="pills-tabContent">
       |            ${pages.map(htmlPage).mkString}
       |          </div>
       |        </div>
       |      </main>
       |    </div>
       |  </div>
       |""".stripMargin
  }

  private def htmlPage(page: HtmlPage): String = {
    s"""
       |<div class="tab-pane fade show ${if (page.isActive) "active" else ""}"
       |  id="pills-${page.pageId}" role="tabpanel" aria-labelledby="pills-${page.pageId}-tab">
       |  <div class="container-fluid app-${page.pageId} mt-2">
       |    ${page.content}
       |  </div>
       |</div>
       |""".stripMargin
  }

  private case class NavLink(name: String, resourceId: String, icon: String, isActive: Boolean) {
    override def toString: String = {
      val cssId = resourceId.toLowerCase
      s"""
         |  <li class="nav-item">
         |    <a class="nav-link link-dark px-2 ${if (isActive) "active" else ""} d-flex align-middle gap-2"
         |       href="#" id="pills-$cssId-tab" data-bs-toggle="pill" data-bs-target="#pills-$cssId" type="button"
         |       role="tab" aria-controls="pills-$cssId" aria-selected="${if (isActive) "true" else "false"}">
         |      <i class="bi bi-$icon"></i>
         |      ${name.capitalize}
         |    </a>
         |  </li>
         |""".stripMargin
    }
  }

}
